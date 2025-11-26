// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`VarianceSample`]: variance sample aggregations.
//! [`VariancePopulation`]: variance population aggregations.

use arrow::datatypes::FieldRef;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray,
        FixedSizeBinaryBuilder, Float64Array, Float64Builder, PrimitiveArray,
        UInt64Array, UInt64Builder,
    },
    buffer::NullBuffer,
    compute::kernels::cast,
    datatypes::i256,
    datatypes::{
        ArrowNumericType, DataType, Decimal128Type, Decimal256Type, Decimal32Type,
        Decimal64Type, DecimalType, Field, DECIMAL256_MAX_SCALE,
    },
};
use datafusion_common::{
    downcast_value, exec_err, not_impl_err, plan_err, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Coercion, Documentation, GroupsAccumulator, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions_aggregate_common::{
    aggregate::groups_accumulator::accumulate::accumulate, stats::StatsType,
};
use datafusion_macros::user_doc;
use std::convert::TryInto;
use std::mem::{size_of, size_of_val};
use std::{fmt::Debug, marker::PhantomData, ops::Neg, sync::Arc};

make_udaf_expr_and_func!(
    VarianceSample,
    var_sample,
    expression,
    "Computes the sample variance.",
    var_samp_udaf
);

make_udaf_expr_and_func!(
    VariancePopulation,
    var_pop,
    expression,
    "Computes the population variance.",
    var_pop_udaf
);

fn variance_signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Numeric(1),
            TypeSignature::Coercible(vec![Coercion::new_exact(
                TypeSignatureClass::Decimal,
            )]),
        ],
        Volatility::Immutable,
    )
}

const DECIMAL_VARIANCE_BINARY_SIZE: i32 = 32;

fn decimal_overflow_err() -> DataFusionError {
    DataFusionError::Execution("Decimal variance overflow".to_string())
}

fn i256_to_f64_lossy(value: i256) -> f64 {
    const SCALE: f64 = 18446744073709551616.0; // 2^64
    let mut abs = value;
    let negative = abs < i256::ZERO;
    if negative {
        abs = abs.neg();
    }
    let bytes = abs.to_le_bytes();
    let mut result = 0f64;
    for chunk in bytes.chunks_exact(8).rev() {
        let chunk_val = u64::from_le_bytes(chunk.try_into().unwrap());
        result = result * SCALE + chunk_val as f64;
    }
    if negative {
        -result
    } else {
        result
    }
}

fn decimal_scale(dt: &DataType) -> Option<i8> {
    match dt {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => Some(*scale),
        _ => None,
    }
}

fn decimal_variance_state_fields(name: &str) -> Vec<FieldRef> {
    vec![
        Field::new(format_state_name(name, "count"), DataType::UInt64, true),
        Field::new(
            format_state_name(name, "sum"),
            DataType::FixedSizeBinary(DECIMAL_VARIANCE_BINARY_SIZE),
            true,
        ),
        Field::new(
            format_state_name(name, "sum_squares"),
            DataType::FixedSizeBinary(DECIMAL_VARIANCE_BINARY_SIZE),
            true,
        ),
    ]
    .into_iter()
    .map(Arc::new)
    .collect()
}

fn is_numeric_or_decimal(data_type: &DataType) -> bool {
    data_type.is_numeric()
        || matches!(
            data_type,
            DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

fn i256_from_bytes(bytes: &[u8]) -> Result<i256> {
    if bytes.len() != DECIMAL_VARIANCE_BINARY_LEN {
        return exec_err!(
            "Decimal variance state expected {} bytes got {}",
            DECIMAL_VARIANCE_BINARY_LEN,
            bytes.len()
        );
    }
    let mut buffer = [0u8; DECIMAL_VARIANCE_BINARY_LEN];
    buffer.copy_from_slice(bytes);
    Ok(i256::from_le_bytes(buffer))
}

const DECIMAL_VARIANCE_BINARY_LEN: usize = DECIMAL_VARIANCE_BINARY_SIZE as usize;

fn i256_to_scalar(value: i256) -> ScalarValue {
    ScalarValue::FixedSizeBinary(
        DECIMAL_VARIANCE_BINARY_SIZE,
        Some(value.to_le_bytes().to_vec()),
    )
}

fn create_decimal_variance_accumulator(
    data_type: &DataType,
    stats_type: StatsType,
) -> Result<Option<Box<dyn Accumulator>>> {
    let accumulator = match data_type {
        DataType::Decimal32(_, scale) => Some(Box::new(DecimalVarianceAccumulator::<
            Decimal32Type,
        >::try_new(
            *scale, stats_type
        )?) as Box<dyn Accumulator>),
        DataType::Decimal64(_, scale) => Some(Box::new(DecimalVarianceAccumulator::<
            Decimal64Type,
        >::try_new(
            *scale, stats_type
        )?) as Box<dyn Accumulator>),
        DataType::Decimal128(_, scale) => Some(Box::new(DecimalVarianceAccumulator::<
            Decimal128Type,
        >::try_new(
            *scale, stats_type
        )?) as Box<dyn Accumulator>),
        DataType::Decimal256(_, scale) => Some(Box::new(DecimalVarianceAccumulator::<
            Decimal256Type,
        >::try_new(
            *scale, stats_type
        )?) as Box<dyn Accumulator>),
        _ => None,
    };
    Ok(accumulator)
}

fn create_decimal_variance_groups_accumulator(
    data_type: &DataType,
    stats_type: StatsType,
) -> Result<Option<Box<dyn GroupsAccumulator>>> {
    let accumulator = match data_type {
        DataType::Decimal32(_, scale) => Some(Box::new(
            DecimalVarianceGroupsAccumulator::<Decimal32Type>::new(*scale, stats_type),
        ) as Box<dyn GroupsAccumulator>),
        DataType::Decimal64(_, scale) => Some(Box::new(
            DecimalVarianceGroupsAccumulator::<Decimal64Type>::new(*scale, stats_type),
        ) as Box<dyn GroupsAccumulator>),
        DataType::Decimal128(_, scale) => Some(Box::new(
            DecimalVarianceGroupsAccumulator::<Decimal128Type>::new(*scale, stats_type),
        ) as Box<dyn GroupsAccumulator>),
        DataType::Decimal256(_, scale) => Some(Box::new(
            DecimalVarianceGroupsAccumulator::<Decimal256Type>::new(*scale, stats_type),
        ) as Box<dyn GroupsAccumulator>),
        _ => None,
    };
    Ok(accumulator)
}

trait DecimalNative: Copy {
    fn to_i256(self) -> i256;
}

impl DecimalNative for i32 {
    fn to_i256(self) -> i256 {
        i256::from(self)
    }
}

impl DecimalNative for i64 {
    fn to_i256(self) -> i256 {
        i256::from(self)
    }
}

impl DecimalNative for i128 {
    fn to_i256(self) -> i256 {
        i256::from_i128(self)
    }
}

impl DecimalNative for i256 {
    fn to_i256(self) -> i256 {
        self
    }
}

#[derive(Clone, Debug, Default)]
struct DecimalVarianceState {
    count: u64,
    sum: i256,
    sum_squares: i256,
}

impl DecimalVarianceState {
    fn update(&mut self, value: i256) -> Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(decimal_overflow_err)?;
        self.sum = self
            .sum
            .checked_add(value)
            .ok_or_else(decimal_overflow_err)?;
        let square = value.checked_mul(value).ok_or_else(decimal_overflow_err)?;
        self.sum_squares = self
            .sum_squares
            .checked_add(square)
            .ok_or_else(decimal_overflow_err)?;
        Ok(())
    }

    fn retract(&mut self, value: i256) -> Result<()> {
        if self.count == 0 {
            return exec_err!("Decimal variance retract underflow");
        }
        self.count -= 1;
        self.sum = self
            .sum
            .checked_sub(value)
            .ok_or_else(decimal_overflow_err)?;
        let square = value.checked_mul(value).ok_or_else(decimal_overflow_err)?;
        self.sum_squares = self
            .sum_squares
            .checked_sub(square)
            .ok_or_else(decimal_overflow_err)?;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.count = self
            .count
            .checked_add(other.count)
            .ok_or_else(decimal_overflow_err)?;
        self.sum = self
            .sum
            .checked_add(other.sum)
            .ok_or_else(decimal_overflow_err)?;
        self.sum_squares = self
            .sum_squares
            .checked_add(other.sum_squares)
            .ok_or_else(decimal_overflow_err)?;
        Ok(())
    }

    fn variance(&self, stats_type: StatsType, scale: i8) -> Result<Option<f64>> {
        if self.count == 0 {
            return Ok(None);
        }
        if matches!(stats_type, StatsType::Sample) && self.count <= 1 {
            return Ok(None);
        }

        let count_i256 = i256::from_i128(self.count as i128);
        let scaled_sum_squares = self
            .sum_squares
            .checked_mul(count_i256)
            .ok_or_else(decimal_overflow_err)?;
        let sum_squared = self
            .sum
            .checked_mul(self.sum)
            .ok_or_else(decimal_overflow_err)?;
        let numerator = scaled_sum_squares
            .checked_sub(sum_squared)
            .ok_or_else(decimal_overflow_err)?;

        let numerator = if numerator < i256::ZERO {
            i256::ZERO
        } else {
            numerator
        };

        let denominator_counts = match stats_type {
            StatsType::Population => {
                let count = self.count as f64;
                count * count
            }
            StatsType::Sample => {
                let count = self.count as f64;
                count * ((self.count - 1) as f64)
            }
        };

        if denominator_counts == 0.0 {
            return Ok(None);
        }

        let numerator_f64 = i256_to_f64_lossy(numerator);
        let scale_factor = 10f64.powi(2 * scale as i32);
        Ok(Some(numerator_f64 / (denominator_counts * scale_factor)))
    }

    fn to_scalar_state(&self) -> Vec<ScalarValue> {
        vec![
            ScalarValue::from(self.count),
            i256_to_scalar(self.sum),
            i256_to_scalar(self.sum_squares),
        ]
    }
}

#[derive(Debug)]
struct DecimalVarianceAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    state: DecimalVarianceState,
    scale: i8,
    stats_type: StatsType,
    _marker: PhantomData<T>,
}

impl<T> DecimalVarianceAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    fn try_new(scale: i8, stats_type: StatsType) -> Result<Self> {
        if scale > DECIMAL256_MAX_SCALE {
            return exec_err!(
                "Decimal variance does not support scale {} greater than {}",
                scale,
                DECIMAL256_MAX_SCALE
            );
        }
        Ok(Self {
            state: DecimalVarianceState::default(),
            scale,
            stats_type,
            _marker: PhantomData,
        })
    }

    fn convert_array(values: &ArrayRef) -> &PrimitiveArray<T> {
        values.as_primitive::<T>()
    }
}

impl<T> Accumulator for DecimalVarianceAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(self.state.to_scalar_state())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = Self::convert_array(&values[0]);
        for value in array.iter().flatten() {
            self.state.update(value.to_i256())?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = Self::convert_array(&values[0]);
        for value in array.iter().flatten() {
            self.state.retract(value.to_i256())?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let sums = downcast_value!(states[1], FixedSizeBinaryArray);
        let sum_squares = downcast_value!(states[2], FixedSizeBinaryArray);

        for i in 0..counts.len() {
            if counts.is_null(i) {
                continue;
            }
            let count = counts.value(i);
            if count == 0 {
                continue;
            }
            let sum = i256_from_bytes(sums.value(i))?;
            let sum_sq = i256_from_bytes(sum_squares.value(i))?;
            let other = DecimalVarianceState {
                count,
                sum,
                sum_squares: sum_sq,
            };
            self.state.merge(&other)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.state.variance(self.stats_type, self.scale)? {
            Some(v) => Ok(ScalarValue::Float64(Some(v))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct DecimalVarianceGroupsAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    states: Vec<DecimalVarianceState>,
    scale: i8,
    stats_type: StatsType,
    _marker: PhantomData<T>,
}

impl<T> DecimalVarianceGroupsAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    fn new(scale: i8, stats_type: StatsType) -> Self {
        Self {
            states: Vec::new(),
            scale,
            stats_type,
            _marker: PhantomData,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        if self.states.len() < total_num_groups {
            self.states
                .resize(total_num_groups, DecimalVarianceState::default());
        }
    }
}

impl<T> GroupsAccumulator for DecimalVarianceGroupsAccumulator<T>
where
    T: DecimalType + ArrowNumericType + Debug,
    T::Native: DecimalNative,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let array = values[0].as_primitive::<T>();
        self.resize(total_num_groups);
        for (row, group_index) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if !filter.value(row) {
                    continue;
                }
            }
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row).to_i256();
            self.states[*group_index].update(value)?;
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let counts = downcast_value!(values[0], UInt64Array);
        let sums = downcast_value!(values[1], FixedSizeBinaryArray);
        let sum_squares = downcast_value!(values[2], FixedSizeBinaryArray);
        self.resize(total_num_groups);

        for (row, group_index) in group_indices.iter().enumerate() {
            if counts.is_null(row) {
                continue;
            }
            let count = counts.value(row);
            if count == 0 {
                continue;
            }
            let sum = i256_from_bytes(sums.value(row))?;
            let sum_sq = i256_from_bytes(sum_squares.value(row))?;
            let other = DecimalVarianceState {
                count,
                sum,
                sum_squares: sum_sq,
            };
            self.states[*group_index].merge(&other)?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let states = emit_to.take_needed(&mut self.states);
        let mut builder = Float64Builder::with_capacity(states.len());
        for state in &states {
            match state.variance(self.stats_type, self.scale)? {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        let states = emit_to.take_needed(&mut self.states);
        let mut counts = UInt64Builder::with_capacity(states.len());
        let mut sums = FixedSizeBinaryBuilder::with_capacity(
            states.len(),
            DECIMAL_VARIANCE_BINARY_SIZE,
        );
        let mut sum_squares = FixedSizeBinaryBuilder::with_capacity(
            states.len(),
            DECIMAL_VARIANCE_BINARY_SIZE,
        );

        for state in states {
            counts.append_value(state.count);
            sums.append_value(state.sum.to_le_bytes())?;
            sum_squares.append_value(state.sum_squares.to_le_bytes())?;
        }

        Ok(vec![
            Arc::new(counts.finish()),
            Arc::new(sums.finish()),
            Arc::new(sum_squares.finish()),
        ])
    }

    fn size(&self) -> usize {
        self.states.capacity() * size_of::<DecimalVarianceState>()
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the statistical sample variance of a set of numbers.",
    syntax_example = "var(expression)",
    standard_argument(name = "expression", prefix = "Numeric")
)]
#[derive(PartialEq, Eq, Hash)]
pub struct VarianceSample {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VarianceSample {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VarianceSample")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VarianceSample {
    fn default() -> Self {
        Self::new()
    }
}

impl VarianceSample {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_sample"), String::from("var_samp")],
            signature: variance_signature(),
        }
    }
}

impl AggregateUDFImpl for VarianceSample {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        if args
            .input_fields
            .first()
            .and_then(|field| decimal_scale(field.data_type()))
            .is_some()
        {
            return Ok(decimal_variance_state_fields(name));
        }
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR(DISTINCT) aggregations are not available");
        }

        if let Some(acc) = create_decimal_variance_accumulator(
            acc_args.expr_fields[0].data_type(),
            StatsType::Sample,
        )? {
            return Ok(acc);
        }

        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if let Some(acc) = create_decimal_variance_groups_accumulator(
            args.expr_fields[0].data_type(),
            StatsType::Sample,
        )? {
            return Ok(acc);
        }
        Ok(Box::new(VarianceGroupsAccumulator::new(StatsType::Sample)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the statistical population variance of a set of numbers.",
    syntax_example = "var_pop(expression)",
    standard_argument(name = "expression", prefix = "Numeric")
)]
#[derive(PartialEq, Eq, Hash)]
pub struct VariancePopulation {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VariancePopulation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VariancePopulation")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VariancePopulation {
    fn default() -> Self {
        Self::new()
    }
}

impl VariancePopulation {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_population")],
            signature: variance_signature(),
        }
    }
}

impl AggregateUDFImpl for VariancePopulation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !is_numeric_or_decimal(&arg_types[0]) {
            return plan_err!("Variance requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        if args
            .input_fields
            .first()
            .and_then(|field| decimal_scale(field.data_type()))
            .is_some()
        {
            return Ok(decimal_variance_state_fields(name));
        }
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR_POP(DISTINCT) aggregations are not available");
        }

        if let Some(acc) = create_decimal_variance_accumulator(
            acc_args.expr_fields[0].data_type(),
            StatsType::Population,
        )? {
            return Ok(acc);
        }

        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
        )?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if let Some(acc) = create_decimal_variance_groups_accumulator(
            args.expr_fields[0].data_type(),
            StatsType::Population,
        )? {
            return Ok(acc);
        }
        Ok(Box::new(VarianceGroupsAccumulator::new(
            StatsType::Population,
        )))
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// An accumulator to compute variance
/// The algorithm used is an online implementation and numerically stable. It is based on this paper:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: f64,
    mean: f64,
    count: u64,
    stats_type: StatsType,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            m2: 0_f64,
            mean: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean(&self) -> f64 {
        self.mean
    }

    pub fn get_m2(&self) -> f64 {
        self.m2
    }
}

#[inline]
fn merge(
    count: u64,
    mean: f64,
    m2: f64,
    count2: u64,
    mean2: f64,
    m22: f64,
) -> (u64, f64, f64) {
    debug_assert!(count != 0 || count2 != 0, "Cannot merge two empty states");
    let new_count = count + count2;
    let new_mean =
        mean * count as f64 / new_count as f64 + mean2 * count2 as f64 / new_count as f64;
    let delta = mean - mean2;
    let new_m2 =
        m2 + m22 + delta * delta * count as f64 * count2 as f64 / new_count as f64;

    (new_count, new_mean, new_m2)
}

#[inline]
fn update(count: u64, mean: f64, m2: f64, value: f64) -> (u64, f64, f64) {
    let new_count = count + 1;
    let delta1 = value - mean;
    let new_mean = delta1 / new_count as f64 + mean;
    let delta2 = value - new_mean;
    let new_m2 = m2 + delta1 * delta2;

    (new_count, new_mean, new_m2)
}

impl Accumulator for VarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean),
            ScalarValue::from(self.m2),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            (self.count, self.mean, self.m2) =
                update(self.count, self.mean, self.m2, value)
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count - 1;
            let delta1 = self.mean - value;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = new_mean - value;
            let new_m2 = self.m2 - delta1 * delta2;

            self.count -= 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let means = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            (self.count, self.mean, self.m2) = merge(
                self.count,
                self.mean,
                self.m2,
                c,
                means.value(i),
                m2s.value(i),
            )
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0 {
                    self.count - 1
                } else {
                    self.count
                }
            }
        };

        Ok(ScalarValue::Float64(match self.count {
            0 => None,
            1 => {
                if let StatsType::Population = self.stats_type {
                    Some(0.0)
                } else {
                    None
                }
            }
            _ => Some(self.m2 / count as f64),
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct VarianceGroupsAccumulator {
    m2s: Vec<f64>,
    means: Vec<f64>,
    counts: Vec<u64>,
    stats_type: StatsType,
}

impl VarianceGroupsAccumulator {
    pub fn new(s_type: StatsType) -> Self {
        Self {
            m2s: Vec::new(),
            means: Vec::new(),
            counts: Vec::new(),
            stats_type: s_type,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.m2s.resize(total_num_groups, 0.0);
        self.means.resize(total_num_groups, 0.0);
        self.counts.resize(total_num_groups, 0);
    }

    fn merge<F>(
        group_indices: &[usize],
        counts: &UInt64Array,
        means: &Float64Array,
        m2s: &Float64Array,
        _opt_filter: Option<&BooleanArray>,
        mut value_fn: F,
    ) where
        F: FnMut(usize, u64, f64, f64) + Send,
    {
        assert_eq!(counts.null_count(), 0);
        assert_eq!(means.null_count(), 0);
        assert_eq!(m2s.null_count(), 0);

        group_indices
            .iter()
            .zip(counts.values().iter())
            .zip(means.values().iter())
            .zip(m2s.values().iter())
            .for_each(|(((&group_index, &count), &mean), &m2)| {
                value_fn(group_index, count, mean, m2);
            });
    }

    pub fn variance(
        &mut self,
        emit_to: datafusion_expr::EmitTo,
    ) -> (Vec<f64>, NullBuffer) {
        let mut counts = emit_to.take_needed(&mut self.counts);
        // means are only needed for updating m2s and are not needed for the final result.
        // But we still need to take them to ensure the internal state is consistent.
        let _ = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);

        if let StatsType::Sample = self.stats_type {
            counts.iter_mut().for_each(|count| {
                *count = count.saturating_sub(1);
            });
        }
        let nulls = NullBuffer::from_iter(counts.iter().map(|&count| count != 0));
        let variance = m2s
            .iter()
            .zip(counts)
            .map(|(m2, count)| m2 / count as f64)
            .collect();
        (variance, nulls)
    }
}

impl GroupsAccumulator for VarianceGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = &cast(&values[0], &DataType::Float64)?;
        let values = downcast_value!(values, Float64Array);

        self.resize(total_num_groups);
        accumulate(group_indices, values, opt_filter, |group_index, value| {
            let (new_count, new_mean, new_m2) = update(
                self.counts[group_index],
                self.means[group_index],
                self.m2s[group_index],
                value,
            );
            self.counts[group_index] = new_count;
            self.means[group_index] = new_mean;
            self.m2s[group_index] = new_m2;
        });
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        // Since aggregate filter should be applied in partial stage, in final stage there should be no filter
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 3, "two arguments to merge_batch");
        // first batch is counts, second is partial means, third is partial m2s
        let partial_counts = downcast_value!(values[0], UInt64Array);
        let partial_means = downcast_value!(values[1], Float64Array);
        let partial_m2s = downcast_value!(values[2], Float64Array);

        self.resize(total_num_groups);
        Self::merge(
            group_indices,
            partial_counts,
            partial_means,
            partial_m2s,
            None,
            |group_index, partial_count, partial_mean, partial_m2| {
                if partial_count == 0 {
                    return;
                }
                let (new_count, new_mean, new_m2) = merge(
                    self.counts[group_index],
                    self.means[group_index],
                    self.m2s[group_index],
                    partial_count,
                    partial_mean,
                    partial_m2,
                );
                self.counts[group_index] = new_count;
                self.means[group_index] = new_mean;
                self.m2s[group_index] = new_m2;
            },
        );
        Ok(())
    }

    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let (variances, nulls) = self.variance(emit_to);
        Ok(Arc::new(Float64Array::new(variances.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let means = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);

        Ok(vec![
            Arc::new(UInt64Array::new(counts.into(), None)),
            Arc::new(Float64Array::new(means.into(), None)),
            Arc::new(Float64Array::new(m2s.into(), None)),
        ])
    }

    fn size(&self) -> usize {
        self.m2s.capacity() * size_of::<f64>()
            + self.means.capacity() * size_of::<f64>()
            + self.counts.capacity() * size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Decimal128Builder;
    use datafusion_expr::EmitTo;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn variance_population_accepts_decimal() -> Result<()> {
        let variance = VariancePopulation::new();
        variance.return_type(&[DataType::Decimal128(10, 3)])?;
        Ok(())
    }

    #[test]
    fn variance_decimal_input() -> Result<()> {
        let mut builder = Decimal128Builder::with_capacity(20);
        for i in 0..10 {
            builder.append_value(110000 + i);
        }
        for i in 0..10 {
            builder.append_value(-((100000 + i) as i128));
        }
        let decimal_array = builder.finish().with_precision_and_scale(10, 3).unwrap();
        let array: ArrayRef = Arc::new(decimal_array);

        let mut pop_acc = VarianceAccumulator::try_new(StatsType::Population)?;
        let pop_input = [Arc::clone(&array)];
        pop_acc.update_batch(&pop_input)?;
        assert_variance(pop_acc.evaluate()?, 11025.9450285);

        let mut sample_acc = VarianceAccumulator::try_new(StatsType::Sample)?;
        let sample_input = [array];
        sample_acc.update_batch(&sample_input)?;
        assert_variance(sample_acc.evaluate()?, 11606.257924736841);

        Ok(())
    }

    fn assert_variance(value: ScalarValue, expected: f64) {
        match value {
            ScalarValue::Float64(Some(actual)) => {
                assert!((actual - expected).abs() < 1e-9)
            }
            other => panic!("expected Float64 result, got {other:?}"),
        }
    }

    #[test]
    fn test_groups_accumulator_merge_empty_states() -> Result<()> {
        let state_1 = vec![
            Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0])),
            Arc::new(Float64Array::from(vec![0.0])),
        ];
        let state_2 = vec![
            Arc::new(UInt64Array::from(vec![2])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![1.0])),
        ];
        let mut acc = VarianceGroupsAccumulator::new(StatsType::Sample);
        acc.merge_batch(&state_1, &[0], None, 1)?;
        acc.merge_batch(&state_2, &[0], None, 1)?;
        let result = acc.evaluate(EmitTo::All)?;
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.value(0), 1.0);
        Ok(())
    }
}
