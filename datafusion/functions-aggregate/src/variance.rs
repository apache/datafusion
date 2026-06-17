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

use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, Decimal128Type, Decimal256Type,
    FieldRef, Float64Type, i256,
};
use arrow::{
    array::{
        Array, ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray, Float64Array,
        UInt64Array,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field},
};
use datafusion_common::cast::{as_boolean_array, as_float64_array, as_uint64_array};
use datafusion_common::types::NativeType;
use datafusion_common::{
    Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err, plan_err,
};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, GroupsAccumulator, Signature,
    Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
};
use datafusion_functions_aggregate_common::utils::GenericDistinctBuffer;
use datafusion_functions_aggregate_common::{
    aggregate::groups_accumulator::accumulate::accumulate, stats::StatsType,
};
use datafusion_macros::user_doc;
use num_traits::ToPrimitive;
use std::mem::{size_of, size_of_val};
use std::{fmt::Debug, sync::Arc};

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

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the statistical sample variance of a set of numbers.",
    syntax_example = "var(expression)",
    standard_argument(name = "expression", prefix = "Numeric")
)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct VarianceSample {
    signature: Signature,
    aliases: Vec<String>,
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
            signature: stats_signature(),
        }
    }
}

impl AggregateUDFImpl for VarianceSample {
    fn name(&self) -> &str {
        "var"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        stats_coerce_types(self.name(), arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(variance_state_fields(
            args.name,
            args.input_fields[0].data_type(),
            args.is_distinct,
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if let DataType::Decimal128(precision, scale) =
            acc_args.expr_fields[0].data_type()
        {
            if acc_args.is_distinct {
                return Ok(Box::new(Decimal128DistinctVarianceAccumulator::new(
                    StatsType::Sample,
                    *precision,
                    *scale,
                )));
            }

            return Ok(Box::new(Decimal128VarianceAccumulator::new(
                StatsType::Sample,
                *precision,
                *scale,
            )));
        }

        if acc_args.is_distinct {
            return Ok(Box::new(DistinctVarianceAccumulator::new(
                StatsType::Sample,
            )));
        }

        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        // This relies on stats_coerce_types mapping supported inputs to one of
        // these execution types before physical planning.
        !acc_args.is_distinct
            && matches!(
                acc_args.expr_fields[0].data_type(),
                DataType::Float64 | DataType::Decimal128(_, _)
            )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if let DataType::Decimal128(precision, scale) = args.expr_fields[0].data_type() {
            return Ok(Box::new(Decimal128VarianceGroupsAccumulator::new(
                StatsType::Sample,
                *precision,
                *scale,
            )));
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
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct VariancePopulation {
    signature: Signature,
    aliases: Vec<String>,
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
            signature: stats_signature(),
        }
    }
}

impl AggregateUDFImpl for VariancePopulation {
    fn name(&self) -> &str {
        "var_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        stats_coerce_types(self.name(), arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(variance_state_fields(
            args.name,
            args.input_fields[0].data_type(),
            args.is_distinct,
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if let DataType::Decimal128(precision, scale) =
            acc_args.expr_fields[0].data_type()
        {
            if acc_args.is_distinct {
                return Ok(Box::new(Decimal128DistinctVarianceAccumulator::new(
                    StatsType::Population,
                    *precision,
                    *scale,
                )));
            }

            return Ok(Box::new(Decimal128VarianceAccumulator::new(
                StatsType::Population,
                *precision,
                *scale,
            )));
        }

        if acc_args.is_distinct {
            return Ok(Box::new(DistinctVarianceAccumulator::new(
                StatsType::Population,
            )));
        }

        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
        )?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        // This relies on stats_coerce_types mapping supported inputs to one of
        // these execution types before physical planning.
        !acc_args.is_distinct
            && matches!(
                acc_args.expr_fields[0].data_type(),
                DataType::Float64 | DataType::Decimal128(_, _)
            )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if let DataType::Decimal128(precision, scale) = args.expr_fields[0].data_type() {
            return Ok(Box::new(Decimal128VarianceGroupsAccumulator::new(
                StatsType::Population,
                *precision,
                *scale,
            )));
        }

        Ok(Box::new(VarianceGroupsAccumulator::new(
            StatsType::Population,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

pub(crate) fn stats_signature() -> Signature {
    Signature::user_defined(Volatility::Immutable)
}

pub(crate) fn stats_coerce_types(
    func_name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return plan_err!(
            "The function {func_name} expects 1 argument, but {} were provided",
            arg_types.len()
        );
    }

    coerce_stats_arg(func_name, &arg_types[0]).map(|data_type| vec![data_type])
}

fn coerce_stats_arg(func_name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Dictionary(_, value_type) => coerce_stats_arg(func_name, value_type),
        DataType::Null => Ok(DataType::Float64),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale)
            if *precision <= DECIMAL128_MAX_PRECISION =>
        {
            Ok(DataType::Decimal128(*precision, *scale))
        }
        DataType::Decimal256(_, _) => Ok(DataType::Float64),
        _ => {
            let logical_type: NativeType = data_type.into();
            if logical_type.is_integer() || logical_type.is_float() {
                Ok(DataType::Float64)
            } else {
                plan_err!(
                    "The function {func_name} expects Numeric but received {logical_type}"
                )
            }
        }
    }
}

pub(crate) fn variance_state_fields(
    name: &str,
    input_type: &DataType,
    is_distinct: bool,
) -> Vec<FieldRef> {
    if is_distinct {
        let value_type = match input_type {
            DataType::Decimal128(precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            _ => DataType::Float64,
        };
        let field = Field::new_list_field(value_type, true);
        return vec![
            Field::new(
                format_state_name(name, "distinct_var"),
                DataType::List(Arc::new(field)),
                true,
            )
            .into(),
        ];
    }

    match input_type {
        DataType::Decimal128(precision, scale) => vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(
                format_state_name(name, "sum"),
                decimal_sum_type(*scale),
                true,
            ),
            Field::new(
                format_state_name(name, "sum_squares"),
                decimal_sum_squares_type(*scale),
                true,
            ),
            Field::new(
                format_state_name(name, "anchor"),
                DataType::Decimal128(*precision, *scale),
                true,
            ),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
            Field::new(format_state_name(name, "exact"), DataType::Boolean, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect(),
        _ => vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect(),
    }
}

fn decimal_sum_type(scale: i8) -> DataType {
    DataType::Decimal256(DECIMAL256_MAX_PRECISION, scale)
}

fn decimal_sum_squares_type(scale: i8) -> DataType {
    DataType::Decimal256(DECIMAL256_MAX_PRECISION, scale * 2)
}

pub(crate) fn checked_add_decimal(lhs: i256, rhs: i256) -> Result<i256> {
    lhs.add_checked(rhs).map_err(|e| arrow_datafusion_err!(e))
}

pub(crate) fn checked_sub_decimal(lhs: i256, rhs: i256) -> Result<i256> {
    lhs.sub_checked(rhs).map_err(|e| arrow_datafusion_err!(e))
}

pub(crate) fn checked_mul_decimal(lhs: i256, rhs: i256) -> Result<i256> {
    lhs.mul_checked(rhs).map_err(|e| arrow_datafusion_err!(e))
}

pub(crate) fn checked_add_count(lhs: u64, rhs: u64) -> Result<u64> {
    lhs.checked_add(rhs).ok_or_else(|| {
        exec_datafusion_err!("Arithmetic overflow in statistical aggregate count")
    })
}

pub(crate) fn i256_to_f64(value: i256) -> Result<f64> {
    value
        .to_f64()
        .ok_or_else(|| exec_datafusion_err!("Failed to convert Decimal256 to Float64"))
}

fn variance_divisor_count(count: u64, stats_type: StatsType) -> u64 {
    match stats_type {
        StatsType::Population => count,
        StatsType::Sample => count.saturating_sub(1),
    }
}

fn variance_from_m2(count: u64, m2: f64, stats_type: StatsType) -> Option<f64> {
    match (count, stats_type) {
        (0, _) => None,
        (1, StatsType::Sample) => None,
        (1, StatsType::Population) => Some(0.0),
        _ => Some(normalize_decimal_stat(
            m2 / variance_divisor_count(count, stats_type) as f64,
        )),
    }
}

fn shifted_decimal_to_f64(value: i128, anchor: i128, scale_mul: f64) -> Result<f64> {
    let value = i256::from_i128(value);
    let anchor = i256::from_i128(anchor);
    Ok(i256_to_f64(checked_sub_decimal(value, anchor)?)? / scale_mul)
}

fn shift_mean_to_anchor(
    mean: f64,
    current_anchor: i128,
    target_anchor: i128,
    scale_mul: f64,
) -> Result<f64> {
    Ok(mean + shifted_decimal_to_f64(current_anchor, target_anchor, scale_mul)?)
}

fn non_null_anchor(anchor: Option<i128>) -> Result<i128> {
    anchor.ok_or_else(|| exec_datafusion_err!("Missing Decimal128 variance anchor"))
}

fn checked_decimal_state(
    sum: i256,
    sum_squares: i256,
    value: i256,
) -> Result<(i256, i256)> {
    let square = checked_mul_decimal(value, value)?;
    Ok((
        checked_add_decimal(sum, value)?,
        checked_add_decimal(sum_squares, square)?,
    ))
}

fn checked_decimal_state_retract(
    sum: i256,
    sum_squares: i256,
    value: i256,
) -> Result<(i256, i256)> {
    let square = checked_mul_decimal(value, value)?;
    Ok((
        checked_sub_decimal(sum, value)?,
        checked_sub_decimal(sum_squares, square)?,
    ))
}

fn exact_decimal_variance_value(
    count: u64,
    divisor_count: u64,
    sum: i256,
    sum_squares: i256,
    scale: i8,
) -> Result<Option<f64>> {
    let count_i256 = i256::from_i128(i128::from(count));
    let numerator =
        match checked_mul_decimal(sum_squares, count_i256).and_then(|sum_squares| {
            checked_sub_decimal(sum_squares, checked_mul_decimal(sum, sum)?)
        }) {
            Ok(numerator) => numerator,
            Err(_) => return Ok(None),
        };

    let scale_mul = 10_f64.powi(scale as i32);
    let denominator = count as f64 * divisor_count as f64 * scale_mul * scale_mul;
    Ok(Some(i256_to_f64(numerator)? / denominator))
}

pub(crate) fn normalize_decimal_stat(value: f64) -> f64 {
    if value < 0.0 { 0.0 } else { value }
}

#[derive(Debug)]
pub(crate) struct Decimal128VarianceAccumulator {
    sum: i256,
    sum_squares: i256,
    anchor: Option<i128>,
    mean: f64,
    m2: f64,
    count: u64,
    precision: u8,
    scale: i8,
    scale_mul: f64,
    exact: bool,
    stats_type: StatsType,
}

impl Decimal128VarianceAccumulator {
    pub(crate) fn new(stats_type: StatsType, precision: u8, scale: i8) -> Self {
        Self {
            sum: i256::ZERO,
            sum_squares: i256::ZERO,
            anchor: None,
            mean: 0.0,
            m2: 0.0,
            count: 0,
            precision,
            scale,
            scale_mul: 10_f64.powi(scale as i32),
            exact: true,
            stats_type,
        }
    }

    fn update_value(&mut self, value: i128) -> Result<()> {
        checked_add_count(self.count, 1)?;
        let anchor = *self.anchor.get_or_insert(value);
        // Keep Welford state in lock-step so Decimal256 overflow can fall back
        // without retaining all input values. The shifted value keeps nearby
        // full-width Decimal128 values distinguishable after conversion to f64.
        let value_f64 = shifted_decimal_to_f64(value, anchor, self.scale_mul)?;
        (self.count, self.mean, self.m2) =
            update(self.count, self.mean, self.m2, value_f64);

        if self.exact {
            let value = i256::from_i128(value);
            match checked_decimal_state(self.sum, self.sum_squares, value) {
                Ok((sum, sum_squares)) => {
                    self.sum = sum;
                    self.sum_squares = sum_squares;
                }
                Err(_) => self.exact = false,
            }
        }

        Ok(())
    }

    fn retract_value(&mut self, value: i128) -> Result<()> {
        if self.count == 0 {
            return Err(exec_datafusion_err!(
                "Cannot retract from empty variance accumulator"
            ));
        }

        let anchor = non_null_anchor(self.anchor)?;
        let value_f64 = shifted_decimal_to_f64(value, anchor, self.scale_mul)?;
        if self.count == 1 {
            self.count = 0;
            self.anchor = None;
            self.mean = 0.0;
            self.m2 = 0.0;
            self.sum = i256::ZERO;
            self.sum_squares = i256::ZERO;
            self.exact = true;
            return Ok(());
        } else {
            let new_count = self.count - 1;
            let delta1 = self.mean - value_f64;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = new_mean - value_f64;
            let new_m2 = self.m2 - delta1 * delta2;

            self.count = new_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        if self.exact {
            let value = i256::from_i128(value);
            match checked_decimal_state_retract(self.sum, self.sum_squares, value) {
                Ok((sum, sum_squares)) => {
                    self.sum = sum;
                    self.sum_squares = sum_squares;
                }
                Err(_) => self.exact = false,
            }
        }

        Ok(())
    }

    fn evaluate_variance(&self) -> Result<Option<f64>> {
        let divisor_count = variance_divisor_count(self.count, self.stats_type);
        if divisor_count == 0 {
            return Ok(variance_from_m2(self.count, self.m2, self.stats_type));
        }

        if self.exact
            && let Some(variance) = exact_decimal_variance_value(
                self.count,
                divisor_count,
                self.sum,
                self.sum_squares,
                self.scale,
            )?
        {
            return Ok(Some(normalize_decimal_stat(variance)));
        }

        Ok(variance_from_m2(self.count, self.m2, self.stats_type))
    }

    fn merge_exact_state(
        &mut self,
        partial_sum: Option<i256>,
        partial_sum_squares: Option<i256>,
        partial_exact: bool,
    ) {
        if !self.exact || !partial_exact {
            self.exact = false;
            return;
        }

        let (Some(partial_sum), Some(partial_sum_squares)) =
            (partial_sum, partial_sum_squares)
        else {
            self.exact = false;
            return;
        };

        match checked_add_decimal(self.sum, partial_sum).and_then(|sum| {
            checked_add_decimal(self.sum_squares, partial_sum_squares)
                .map(|sum_squares| (sum, sum_squares))
        }) {
            Ok((sum, sum_squares)) => {
                self.sum = sum;
                self.sum_squares = sum_squares;
            }
            Err(_) => self.exact = false,
        }
    }

    fn merge_welford_state(
        &mut self,
        partial_count: u64,
        partial_anchor: i128,
        partial_mean: f64,
        partial_m2: f64,
    ) -> Result<()> {
        checked_add_count(self.count, partial_count)?;
        let anchor = match self.anchor {
            Some(anchor) => anchor,
            None => {
                self.anchor = Some(partial_anchor);
                partial_anchor
            }
        };
        let partial_mean =
            shift_mean_to_anchor(partial_mean, partial_anchor, anchor, self.scale_mul)?;

        (self.count, self.mean, self.m2) = merge(
            self.count,
            self.mean,
            self.m2,
            partial_count,
            partial_mean,
            partial_m2,
        );
        Ok(())
    }
}

impl Accumulator for Decimal128VarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::Decimal256(
                self.exact.then_some(self.sum),
                DECIMAL256_MAX_PRECISION,
                self.scale,
            ),
            ScalarValue::Decimal256(
                self.exact.then_some(self.sum_squares),
                DECIMAL256_MAX_PRECISION,
                self.scale * 2,
            ),
            ScalarValue::Decimal128(self.anchor, self.precision, self.scale),
            ScalarValue::from(self.mean),
            ScalarValue::from(self.m2),
            ScalarValue::Boolean(Some(self.exact)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_primitive::<Decimal128Type>();
        for value in arr.iter().flatten() {
            self.update_value(value)?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_primitive::<Decimal128Type>();
        for value in arr.iter().flatten() {
            self.retract_value(value)?;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = as_uint64_array(&states[0])?;
        let sums = states[1].as_primitive::<Decimal256Type>();
        let sum_squares = states[2].as_primitive::<Decimal256Type>();
        let anchors = states[3].as_primitive::<Decimal128Type>();
        let means = as_float64_array(&states[4])?;
        let m2s = as_float64_array(&states[5])?;
        let exacts = as_boolean_array(&states[6])?;

        for i in 0..counts.len() {
            let count = counts.value(i);
            if count == 0 {
                continue;
            }

            self.merge_welford_state(
                count,
                anchors
                    .is_valid(i)
                    .then(|| anchors.value(i))
                    .ok_or_else(|| {
                        exec_datafusion_err!("Missing Decimal128 variance anchor")
                    })?,
                means.value(i),
                m2s.value(i),
            )?;

            self.merge_exact_state(
                sums.is_valid(i).then(|| sums.value(i)),
                sum_squares.is_valid(i).then(|| sum_squares.value(i)),
                exacts.value(i),
            );
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(self.evaluate_variance()?))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn supports_retract_batch(&self) -> bool {
        true
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
        let arr = as_float64_array(&values[0])?;
        for value in arr.iter().flatten() {
            (self.count, self.mean, self.m2) =
                update(self.count, self.mean, self.m2, value)
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = as_float64_array(&values[0])?;
        for value in arr.iter().flatten() {
            if self.count <= 1 {
                self.count = 0;
                self.mean = 0.0;
                self.m2 = 0.0;
                continue;
            }

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
        let counts = as_uint64_array(&states[0])?;
        let means = as_float64_array(&states[1])?;
        let m2s = as_float64_array(&states[2])?;

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
        let values = as_float64_array(&values[0])?;

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
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 3, "two arguments to merge_batch");
        // first batch is counts, second is partial means, third is partial m2s
        let partial_counts = as_uint64_array(&values[0])?;
        let partial_means = as_float64_array(&values[1])?;
        let partial_m2s = as_float64_array(&values[2])?;

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

#[derive(Debug)]
pub(crate) struct Decimal128VarianceGroupsAccumulator {
    sums: Vec<i256>,
    sum_squares: Vec<i256>,
    anchors: Vec<Option<i128>>,
    means: Vec<f64>,
    m2s: Vec<f64>,
    counts: Vec<u64>,
    exact: Vec<bool>,
    precision: u8,
    scale: i8,
    scale_mul: f64,
    stats_type: StatsType,
}

impl Decimal128VarianceGroupsAccumulator {
    pub(crate) fn new(stats_type: StatsType, precision: u8, scale: i8) -> Self {
        Self {
            sums: Vec::new(),
            sum_squares: Vec::new(),
            anchors: Vec::new(),
            means: Vec::new(),
            m2s: Vec::new(),
            counts: Vec::new(),
            exact: Vec::new(),
            precision,
            scale,
            scale_mul: 10_f64.powi(scale as i32),
            stats_type,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, i256::ZERO);
        self.sum_squares.resize(total_num_groups, i256::ZERO);
        self.anchors.resize(total_num_groups, None);
        self.means.resize(total_num_groups, 0.0);
        self.m2s.resize(total_num_groups, 0.0);
        self.counts.resize(total_num_groups, 0);
        self.exact.resize(total_num_groups, true);
    }

    fn update_group(&mut self, group_index: usize, value: i128) -> Result<()> {
        checked_add_count(self.counts[group_index], 1)?;

        let anchor = *self.anchors[group_index].get_or_insert(value);
        // Keep Welford state in lock-step so Decimal256 overflow can fall back
        // without retaining all input values. The shifted value keeps nearby
        // full-width Decimal128 values distinguishable after conversion to f64.
        let value_f64 = shifted_decimal_to_f64(value, anchor, self.scale_mul)?;
        let (count, mean, m2) = update(
            self.counts[group_index],
            self.means[group_index],
            self.m2s[group_index],
            value_f64,
        );
        self.counts[group_index] = count;
        self.means[group_index] = mean;
        self.m2s[group_index] = m2;

        if self.exact[group_index] {
            let value = i256::from_i128(value);
            match checked_decimal_state(
                self.sums[group_index],
                self.sum_squares[group_index],
                value,
            ) {
                Ok((sum, sum_squares)) => {
                    self.sums[group_index] = sum;
                    self.sum_squares[group_index] = sum_squares;
                }
                Err(_) => self.exact[group_index] = false,
            }
        }

        Ok(())
    }

    fn exact_variance(
        &self,
        count: u64,
        sum: i256,
        sum_squares: i256,
        exact: bool,
    ) -> Result<Option<f64>> {
        let divisor_count = variance_divisor_count(count, self.stats_type);
        if !exact || divisor_count == 0 {
            return Ok(None);
        }

        exact_decimal_variance_value(count, divisor_count, sum, sum_squares, self.scale)
    }

    pub(crate) fn variance(
        &mut self,
        emit_to: datafusion_expr::EmitTo,
    ) -> Result<(Vec<f64>, NullBuffer)> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let sum_squares = emit_to.take_needed(&mut self.sum_squares);
        // Keep anchors in lock-step with emitted groups; variance itself is
        // shift-invariant, so anchors are not needed for final evaluation.
        let _ = emit_to.take_needed(&mut self.anchors);
        let means = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);
        let exact = emit_to.take_needed(&mut self.exact);

        let mut variances = Vec::with_capacity(counts.len());
        let mut valid = Vec::with_capacity(counts.len());

        for (((((count, sum), sum_square), _mean), m2), exact) in counts
            .into_iter()
            .zip(sums)
            .zip(sum_squares)
            .zip(means)
            .zip(m2s)
            .zip(exact)
        {
            let variance = self
                .exact_variance(count, sum, sum_square, exact)?
                .or_else(|| variance_from_m2(count, m2, self.stats_type));
            valid.push(variance.is_some());
            variances.push(variance.unwrap_or(0.0));
        }

        Ok((variances, NullBuffer::from_iter(valid)))
    }

    fn merge_exact_state(
        &mut self,
        group_index: usize,
        partial_sum: Option<i256>,
        partial_sum_squares: Option<i256>,
        partial_exact: bool,
    ) {
        if !self.exact[group_index] || !partial_exact {
            self.exact[group_index] = false;
            return;
        }

        let (Some(partial_sum), Some(partial_sum_squares)) =
            (partial_sum, partial_sum_squares)
        else {
            self.exact[group_index] = false;
            return;
        };

        match checked_add_decimal(self.sums[group_index], partial_sum).and_then(|sum| {
            checked_add_decimal(self.sum_squares[group_index], partial_sum_squares)
                .map(|sum_squares| (sum, sum_squares))
        }) {
            Ok((sum, sum_squares)) => {
                self.sums[group_index] = sum;
                self.sum_squares[group_index] = sum_squares;
            }
            Err(_) => self.exact[group_index] = false,
        }
    }

    fn merge_welford_state(
        &mut self,
        group_index: usize,
        partial_count: u64,
        partial_anchor: i128,
        partial_mean: f64,
        partial_m2: f64,
    ) -> Result<()> {
        checked_add_count(self.counts[group_index], partial_count)?;
        let anchor = match self.anchors[group_index] {
            Some(anchor) => anchor,
            None => {
                self.anchors[group_index] = Some(partial_anchor);
                partial_anchor
            }
        };
        let partial_mean =
            shift_mean_to_anchor(partial_mean, partial_anchor, anchor, self.scale_mul)?;
        let (count, mean, m2) = merge(
            self.counts[group_index],
            self.means[group_index],
            self.m2s[group_index],
            partial_count,
            partial_mean,
            partial_m2,
        );
        self.counts[group_index] = count;
        self.means[group_index] = mean;
        self.m2s[group_index] = m2;
        Ok(())
    }
}

impl GroupsAccumulator for Decimal128VarianceGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Decimal128Type>();

        self.resize(total_num_groups);
        let mut result = Ok(());
        accumulate(group_indices, values, opt_filter, |group_index, value| {
            if result.is_ok() {
                result = self.update_group(group_index, value);
            }
        });
        result
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 7, "seven arguments to merge_batch");
        let partial_counts = as_uint64_array(&values[0])?;
        let partial_sums = values[1].as_primitive::<Decimal256Type>();
        let partial_sum_squares = values[2].as_primitive::<Decimal256Type>();
        let partial_anchors = values[3].as_primitive::<Decimal128Type>();
        let partial_means = as_float64_array(&values[4])?;
        let partial_m2s = as_float64_array(&values[5])?;
        let partial_exact = as_boolean_array(&values[6])?;

        self.resize(total_num_groups);
        for (idx, &group_index) in group_indices.iter().enumerate() {
            let partial_count = partial_counts.value(idx);
            if partial_count == 0 {
                continue;
            }

            self.merge_welford_state(
                group_index,
                partial_count,
                partial_anchors
                    .is_valid(idx)
                    .then(|| partial_anchors.value(idx))
                    .ok_or_else(|| {
                        exec_datafusion_err!("Missing Decimal128 variance anchor")
                    })?,
                partial_means.value(idx),
                partial_m2s.value(idx),
            )?;

            self.merge_exact_state(
                group_index,
                partial_sums.is_valid(idx).then(|| partial_sums.value(idx)),
                partial_sum_squares
                    .is_valid(idx)
                    .then(|| partial_sum_squares.value(idx)),
                partial_exact.value(idx),
            );
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let (variances, nulls) = self.variance(emit_to)?;
        Ok(Arc::new(Float64Array::new(variances.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let sum_squares = emit_to.take_needed(&mut self.sum_squares);
        let anchors = emit_to.take_needed(&mut self.anchors);
        let means = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);
        let exact = emit_to.take_needed(&mut self.exact);
        let sum_nulls = NullBuffer::from_iter(exact.iter().copied());

        Ok(vec![
            Arc::new(UInt64Array::new(counts.into(), None)),
            Arc::new(
                arrow::array::PrimitiveArray::<Decimal256Type>::new(
                    sums.into(),
                    Some(sum_nulls.clone()),
                )
                .with_data_type(decimal_sum_type(self.scale)),
            ),
            Arc::new(
                arrow::array::PrimitiveArray::<Decimal256Type>::new(
                    sum_squares.into(),
                    Some(sum_nulls),
                )
                .with_data_type(decimal_sum_squares_type(self.scale)),
            ),
            Arc::new(
                arrow::array::PrimitiveArray::<Decimal128Type>::from(anchors)
                    .with_data_type(DataType::Decimal128(self.precision, self.scale)),
            ),
            Arc::new(Float64Array::new(means.into(), None)),
            Arc::new(Float64Array::new(m2s.into(), None)),
            Arc::new(BooleanArray::from(exact)),
        ])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<i256>()
            + self.sum_squares.capacity() * size_of::<i256>()
            + self.anchors.capacity() * size_of::<Option<i128>>()
            + self.means.capacity() * size_of::<f64>()
            + self.m2s.capacity() * size_of::<f64>()
            + self.counts.capacity() * size_of::<u64>()
            + self.exact.capacity() * size_of::<bool>()
    }
}

#[derive(Debug)]
pub struct DistinctVarianceAccumulator {
    distinct_values: GenericDistinctBuffer<Float64Type>,
    stat_type: StatsType,
}

impl DistinctVarianceAccumulator {
    pub fn new(stat_type: StatsType) -> Self {
        Self {
            distinct_values: GenericDistinctBuffer::<Float64Type>::new(DataType::Float64),
            stat_type,
        }
    }
}

impl Accumulator for DistinctVarianceAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.distinct_values.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values = self
            .distinct_values
            .values
            .iter()
            .map(|v| v.0)
            .collect::<Vec<_>>();

        let count = match self.stat_type {
            StatsType::Sample => {
                if !values.is_empty() {
                    values.len() - 1
                } else {
                    0
                }
            }
            StatsType::Population => values.len(),
        };

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let m2 = values.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>();

        Ok(ScalarValue::Float64(match values.len() {
            0 => None,
            1 => match self.stat_type {
                StatsType::Population => Some(0.0),
                StatsType::Sample => None,
            },
            _ => Some(m2 / count as f64),
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.distinct_values.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.distinct_values.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.distinct_values.merge_batch(states)
    }
}

#[derive(Debug)]
struct Decimal128DistinctVarianceAccumulator {
    distinct_values: GenericDistinctBuffer<Decimal128Type>,
    stat_type: StatsType,
    scale: i8,
}

impl Decimal128DistinctVarianceAccumulator {
    fn new(stat_type: StatsType, precision: u8, scale: i8) -> Self {
        Self {
            distinct_values: GenericDistinctBuffer::<Decimal128Type>::new(
                DataType::Decimal128(precision, scale),
            ),
            stat_type,
            scale,
        }
    }
}

impl Accumulator for Decimal128DistinctVarianceAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.distinct_values.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut sum = i256::ZERO;
        let mut sum_squares = i256::ZERO;
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut count = 0;
        let mut anchor = None;
        let mut exact = true;
        let scale_mul = 10_f64.powi(self.scale as i32);

        for value in self.distinct_values.values.iter().map(|v| v.0) {
            let anchor = *anchor.get_or_insert(value);
            let value_f64 = shifted_decimal_to_f64(value, anchor, scale_mul)?;
            (count, mean, m2) = update(count, mean, m2, value_f64);

            if exact {
                let value = i256::from_i128(value);
                match checked_decimal_state(sum, sum_squares, value) {
                    Ok((new_sum, new_sum_squares)) => {
                        sum = new_sum;
                        sum_squares = new_sum_squares;
                    }
                    Err(_) => exact = false,
                }
            }
        }

        let divisor_count = variance_divisor_count(count, self.stat_type);
        let variance = if exact && divisor_count != 0 {
            exact_decimal_variance_value(
                count,
                divisor_count,
                sum,
                sum_squares,
                self.scale,
            )?
            .map(normalize_decimal_stat)
        } else {
            None
        }
        .or_else(|| variance_from_m2(count, m2, self.stat_type));

        Ok(ScalarValue::Float64(variance))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.distinct_values.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.distinct_values.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.distinct_values.merge_batch(states)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::EmitTo;
    use datafusion_functions_aggregate_common::utils::get_accum_scalar_values_as_arrays;

    use super::*;

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
        acc.merge_batch(&state_1, &[0], 1)?;
        acc.merge_batch(&state_2, &[0], 1)?;
        let result = acc.evaluate(EmitTo::All)?;
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.value(0), 1.0);
        Ok(())
    }

    #[test]
    fn test_normalize_decimal_stat_clamps_negative() {
        assert_eq!(normalize_decimal_stat(-1e-9), 0.0);
        assert_eq!(normalize_decimal_stat(1.0), 1.0);
    }

    #[test]
    fn test_stats_coerce_decimal_types() -> Result<()> {
        assert_eq!(
            stats_coerce_types("var", &[DataType::Decimal32(9, 2)])?,
            vec![DataType::Decimal128(9, 2)]
        );
        assert_eq!(
            stats_coerce_types("var", &[DataType::Decimal64(18, 2)])?,
            vec![DataType::Decimal128(18, 2)]
        );

        let dictionary_decimal = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Decimal128(10, 2)),
        );
        assert_eq!(
            stats_coerce_types("var", &[dictionary_decimal])?,
            vec![DataType::Decimal128(10, 2)]
        );

        let dictionary_decimal256 = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Decimal256(76, 0)),
        );
        assert_eq!(
            stats_coerce_types("var", &[dictionary_decimal256])?,
            vec![DataType::Float64]
        );

        Ok(())
    }

    #[test]
    fn test_decimal_merge_mixed_exact_and_fallback_states() -> Result<()> {
        let max_decimal = "99999999999999999999999999999999999999"
            .parse::<i128>()
            .unwrap();

        let mut exact_acc =
            Decimal128VarianceAccumulator::new(StatsType::Population, 38, 0);
        let exact_values = Arc::new(
            arrow::array::Decimal128Array::from(vec![
                Some(max_decimal - 7),
                Some(max_decimal - 6),
            ])
            .with_precision_and_scale(38, 0)?,
        ) as ArrayRef;
        exact_acc.update_batch(&[exact_values])?;
        assert!(exact_acc.exact);

        let mut fallback_acc =
            Decimal128VarianceAccumulator::new(StatsType::Population, 38, 0);
        let fallback_values = Arc::new(
            arrow::array::Decimal128Array::from(vec![
                Some(max_decimal - 1),
                Some(max_decimal - 2),
                Some(max_decimal - 3),
                Some(max_decimal - 4),
                Some(max_decimal - 5),
                Some(max_decimal),
            ])
            .with_precision_and_scale(38, 0)?,
        ) as ArrayRef;
        fallback_acc.update_batch(&[fallback_values])?;
        assert!(!fallback_acc.exact);

        let fallback_state = get_accum_scalar_values_as_arrays(&mut fallback_acc)?;
        assert!(fallback_state[1].is_null(0));
        assert!(fallback_state[2].is_null(0));
        assert!(fallback_state[3].is_valid(0));

        exact_acc.merge_batch(&fallback_state)?;
        assert!(!exact_acc.exact);

        let ScalarValue::Float64(Some(value)) = exact_acc.evaluate()? else {
            panic!("expected a non-null Float64 variance")
        };
        assert!((value - 5.25).abs() < 1e-9, "{value}");

        Ok(())
    }
}
