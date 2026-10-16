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

//! Defines `BitAnd`, `BitOr`, `BitXor` and `BitXor DISTINCT` aggregate accumulators

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::mem::{size_of, size_of_val};

use arrow::array::{Array, ArrayRef, AsArray, downcast_integer};
use arrow::datatypes::{
    ArrowNativeType, ArrowNumericType, DataType, Field, FieldRef, Int8Type, Int16Type,
    Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::hash_utils::RandomState;

use datafusion_common::cast::as_list_array;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr::DistinctHandling;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, GroupsAccumulator,
    ReversedUDAF, Signature, TypeSignatureClass, Volatility,
};

use datafusion_doc::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_functions_aggregate_common::noop_accumulator::NoopAccumulator;
use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign};
use std::sync::LazyLock;

/// This macro helps create group accumulators based on bitwise operations typically used internally
/// and might not be necessary for users to call directly.
macro_rules! group_accumulator_helper {
    ($t:ty, $dt:expr, $opr:expr) => {
        match $opr {
            BitwiseOperationType::And => Ok(Box::new(
                PrimitiveGroupsAccumulator::<$t, _>::new($dt, |x, y| x.bitand_assign(y))
                    .with_starting_value(!0),
            )),
            BitwiseOperationType::Or => Ok(Box::new(
                PrimitiveGroupsAccumulator::<$t, _>::new($dt, |x, y| x.bitor_assign(y)),
            )),
            BitwiseOperationType::Xor => Ok(Box::new(
                PrimitiveGroupsAccumulator::<$t, _>::new($dt, |x, y| x.bitxor_assign(y)),
            )),
        }
    };
}

/// Create an accumulator for the integer type and bitwise operation.
macro_rules! accumulator_helper {
    ($t:ty, $opr:expr, $is_distinct:expr, $is_sliding:expr) => {
        match $opr {
            BitwiseOperationType::And => Ok(Box::<BitAndAccumulator<$t>>::default()),
            BitwiseOperationType::Or => Ok(Box::<BitOrAccumulator<$t>>::default()),
            BitwiseOperationType::Xor => {
                if $is_distinct {
                    Ok(Box::<DistinctBitXorAccumulator<$t>>::default())
                } else if $is_sliding {
                    Ok(Box::<SlidingBitXorAccumulator<$t>>::default())
                } else {
                    Ok(Box::<BitXorAccumulator<$t>>::default())
                }
            }
        }
    };
}

/// AND, OR and XOR only supports a subset of numeric types
///
/// `args` is [AccumulatorArgs]
/// `opr` is [BitwiseOperationType]
/// `is_distinct` is boolean value indicating whether the operation is distinct or not.
macro_rules! downcast_bitwise_accumulator {
    ($args:ident, $opr:expr, $is_distinct:expr, $is_sliding:expr) => {
        match $args.return_field.data_type() {
            DataType::Null => Ok(Box::new(NoopAccumulator::default())),
            DataType::Int8 => {
                accumulator_helper!(Int8Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::Int16 => {
                accumulator_helper!(Int16Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::Int32 => {
                accumulator_helper!(Int32Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::Int64 => {
                accumulator_helper!(Int64Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::UInt8 => {
                accumulator_helper!(UInt8Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::UInt16 => {
                accumulator_helper!(UInt16Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::UInt32 => {
                accumulator_helper!(UInt32Type, $opr, $is_distinct, $is_sliding)
            }
            DataType::UInt64 => {
                accumulator_helper!(UInt64Type, $opr, $is_distinct, $is_sliding)
            }
            _ => {
                not_impl_err!(
                    "{} not supported for {}: {}",
                    stringify!($opr),
                    $args.name,
                    $args.return_field.data_type()
                )
            }
        }
    };
}

/// Simplifies the creation of User-Defined Aggregate Functions (UDAFs) for performing bitwise operations in a declarative manner.
///
/// `EXPR_FN` identifier used to name the generated expression function.
/// `AGGREGATE_UDF_FN` is an identifier used to name the underlying UDAF function.
/// `OPR_TYPE` is an expression that evaluates to the type of bitwise operation to be performed.
/// `DOCUMENTATION` documentation for the UDAF
macro_rules! make_bitwise_udaf_expr_and_func {
    ($EXPR_FN:ident, $AGGREGATE_UDF_FN:ident, $OPR_TYPE:expr, $DOCUMENTATION:expr) => {
        make_udaf_expr!(
            $EXPR_FN,
            expr_x,
            concat!(
                "Returns the bitwise",
                stringify!($OPR_TYPE),
                "of a group of values"
            ),
            $AGGREGATE_UDF_FN
        );
        create_func!(
            $EXPR_FN,
            $AGGREGATE_UDF_FN,
            BitwiseOperation::new($OPR_TYPE, stringify!($EXPR_FN), $DOCUMENTATION)
        );
    };
}

static BIT_AND_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_GENERAL,
        "Computes the bitwise AND of all non-null input values.",
        "bit_and(expression)",
    )
    .with_standard_argument("expression", Some("Integer"))
    .build()
});

fn get_bit_and_doc() -> &'static Documentation {
    &BIT_AND_DOC
}

static BIT_OR_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_GENERAL,
        "Computes the bitwise OR of all non-null input values.",
        "bit_or(expression)",
    )
    .with_standard_argument("expression", Some("Integer"))
    .build()
});

fn get_bit_or_doc() -> &'static Documentation {
    &BIT_OR_DOC
}

static BIT_XOR_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_GENERAL,
        "Computes the bitwise exclusive OR of all non-null input values.",
        "bit_xor(expression)",
    )
    .with_standard_argument("expression", Some("Integer"))
    .build()
});

fn get_bit_xor_doc() -> &'static Documentation {
    &BIT_XOR_DOC
}

make_bitwise_udaf_expr_and_func!(
    bit_and,
    bit_and_udaf,
    BitwiseOperationType::And,
    get_bit_and_doc()
);
make_bitwise_udaf_expr_and_func!(
    bit_or,
    bit_or_udaf,
    BitwiseOperationType::Or,
    get_bit_or_doc()
);
make_bitwise_udaf_expr_and_func!(
    bit_xor,
    bit_xor_udaf,
    BitwiseOperationType::Xor,
    get_bit_xor_doc()
);

/// The different types of bitwise operations that can be performed.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum BitwiseOperationType {
    And,
    Or,
    Xor,
}

impl Display for BitwiseOperationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// [BitwiseOperation] struct encapsulates information about a bitwise operation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BitwiseOperation {
    signature: Signature,
    /// `operation` indicates the type of bitwise operation to be performed.
    operation: BitwiseOperationType,
    func_name: &'static str,
    documentation: &'static Documentation,
}

impl BitwiseOperation {
    pub fn new(
        operator: BitwiseOperationType,
        func_name: &'static str,
        documentation: &'static Documentation,
    ) -> Self {
        Self {
            operation: operator,
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Integer)],
                Volatility::Immutable,
            ),
            func_name,
            documentation,
        }
    }
}

impl AggregateUDFImpl for BitwiseOperation {
    fn name(&self) -> &str {
        self.func_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        downcast_bitwise_accumulator!(
            acc_args,
            self.operation,
            acc_args.is_distinct,
            false
        )
    }

    fn create_sliding_accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        downcast_bitwise_accumulator!(
            acc_args,
            self.operation,
            acc_args.is_distinct,
            true
        )
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields[0].data_type().is_null() {
            Ok(vec![
                Field::new(
                    format_state_name(args.name, self.name()),
                    DataType::Null,
                    true,
                )
                .into(),
            ])
        } else if self.operation == BitwiseOperationType::Xor && args.is_distinct {
            Ok(vec![
                Field::new_list(
                    format_state_name(
                        args.name,
                        format!("{} distinct", self.name()).as_str(),
                    ),
                    // See COMMENTS.md to understand why nullable is set to true
                    Field::new_list_field(args.return_type().clone(), true),
                    false,
                )
                .into(),
            ])
        } else {
            Ok(vec![
                Field::new(
                    format_state_name(args.name, self.name()),
                    args.return_field.data_type().clone(),
                    true,
                )
                .into(),
            ])
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if args.return_field.data_type().is_null() {
            // A Null input has no integer groups accumulator (see
            // create_groups_accumulator), so fall back to the scalar accumulator.
            return false;
        }
        // DISTINCT only changes the result of XOR; AND and OR are idempotent
        !(args.is_distinct && self.operation == BitwiseOperationType::Xor)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.return_field.data_type();
        let operation = &self.operation;
        downcast_integer! {
            data_type => (group_accumulator_helper, data_type, operation),
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                data_type
            ),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(self.documentation)
    }

    fn distinct_handling(&self) -> DistinctHandling {
        match self.operation {
            // Bitwise AND/OR are idempotent: duplicates cannot change the
            // result. Only XOR has a distinct accumulator.
            BitwiseOperationType::And | BitwiseOperationType::Or => {
                DistinctHandling::Insensitive
            }
            // XOR cancels duplicate pairs, so `DISTINCT` is meaningful.
            BitwiseOperationType::Xor => DistinctHandling::Sensitive,
        }
    }
}

struct BitAndAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitAndAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitAndAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitAndAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitAndAccumulator<T>
where
    T::Native: std::ops::BitAnd<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_and(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(x);
            *v = *v & x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

struct BitOrAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitOrAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitOrAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitOrAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitOrAccumulator<T>
where
    T::Native: std::ops::BitOr<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_or(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert_with(|| T::Native::usize_as(0));
            *v = *v | x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

struct BitXorAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitXorAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitXorAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitXorAccumulator<T>
where
    T::Native: std::ops::BitXor<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = arrow::compute::bit_xor(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert_with(|| T::Native::usize_as(0));
            *v = *v ^ x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

/// Tracks non-null cardinality so an empty window returns NULL, while a
/// non-empty window whose values cancel returns zero. Ordinary aggregation
/// retains its single-field state and primitive groups accumulator.
struct SlidingBitXorAccumulator<T: ArrowNumericType> {
    value: T::Native,
    count: u64,
}

impl<T: ArrowNumericType> std::fmt::Debug for SlidingBitXorAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlidingBitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for SlidingBitXorAccumulator<T> {
    fn default() -> Self {
        Self {
            value: T::Native::usize_as(0),
            count: 0,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SlidingBitXorAccumulator<T>
where
    T::Native: std::ops::BitXor<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(value) = arrow::compute::bit_xor(values) {
            self.value = self.value ^ value;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count -= (values.len() - values.null_count()) as u64;
        // XOR is its own inverse; nullness also depends on the remaining count.
        if let Some(value) = arrow::compute::bit_xor(values) {
            self.value = self.value ^ value;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(
            (self.count != 0).then_some(self.value),
            &T::DATA_TYPE,
        )
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.evaluate()?,
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(value) = arrow::compute::bit_xor(states[0].as_primitive::<T>()) {
            self.value = self.value ^ value;
        }
        if let Some(count) = arrow::compute::sum(states[1].as_primitive::<UInt64Type>()) {
            self.count += count;
        }
        Ok(())
    }
}

struct DistinctBitXorAccumulator<T: ArrowNumericType> {
    values: HashSet<T::Native, RandomState>,
}

impl<T: ArrowNumericType> std::fmt::Debug for DistinctBitXorAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctBitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for DistinctBitXorAccumulator<T> {
    fn default() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T: ArrowNumericType> Accumulator for DistinctBitXorAccumulator<T>
where
    T::Native: std::ops::BitXor<Output = T::Native> + Hash + Eq,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(array.value(idx));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(*x);
            }),
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc ^ *distinct_value;
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<T::Native>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let values = self
                .values
                .iter()
                .map(|x| ScalarValue::new_primitive::<T>(Some(*x), &T::DATA_TYPE))
                .collect::<Result<Vec<_>>>()?;

            let arr = ScalarValue::new_list_nullable(&values, &T::DATA_TYPE);
            vec![ScalarValue::List(arr)]
        };
        Ok(state_out)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(state) = states.first() {
            let list_arr = as_list_array(state)?;
            for arr in list_arr.iter().flatten() {
                self.update_batch(&[arr])?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, UInt64Type};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Accumulator;
    use datafusion_expr::function::AccumulatorArgs;

    use super::{SlidingBitXorAccumulator, bit_xor_udaf};

    fn array(values: &[Option<u64>]) -> ArrayRef {
        Arc::new(UInt64Array::from(values.to_vec()))
    }

    fn accumulator_args(schema: &Schema) -> AccumulatorArgs<'_> {
        AccumulatorArgs {
            return_field: schema.field(0).clone().into(),
            schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "bit_xor(v)",
            is_distinct: false,
            exprs: &[],
            expr_fields: &[],
        }
    }

    #[test]
    fn sliding_bit_xor_retract_last_non_null() -> Result<()> {
        let schema = Schema::new(vec![Field::new("v", DataType::UInt64, true)]);
        let mut accumulator =
            bit_xor_udaf().create_sliding_accumulator(accumulator_args(&schema))?;
        accumulator.update_batch(&[array(&[Some(7), None, Some(7), None])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(Some(0)));
        accumulator.retract_batch(&[array(&[Some(7), None])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(Some(7)));
        accumulator.retract_batch(&[array(&[Some(7)])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(None));

        // A real zero remains non-null when the empty accumulator is reused.
        accumulator.update_batch(&[array(&[Some(0)])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(Some(0)));
        accumulator.retract_batch(&[array(&[None, Some(0)])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(None));
        Ok(())
    }

    #[test]
    fn sliding_bit_xor_merge_preserves_count() -> Result<()> {
        let mut states = Vec::new();
        for values in [vec![Some(7); 4], vec![Some(9)], vec![None]] {
            let mut partial = SlidingBitXorAccumulator::<UInt64Type>::default();
            partial.update_batch(&[array(&values)])?;
            states.push(partial.state()?);
        }
        let arrays = (0..2)
            .map(|index| {
                ScalarValue::iter_to_array(
                    states.iter().map(|state| state[index].clone()),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut accumulator = SlidingBitXorAccumulator::<UInt64Type>::default();
        accumulator.merge_batch(&arrays)?;

        // The zero-valued partial contributes four input rows.
        accumulator.retract_batch(&[array(&[Some(7); 4])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(Some(9)));
        accumulator.retract_batch(&[array(&[Some(9)])])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::UInt64(None));
        Ok(())
    }

    #[test]
    fn bit_xor_factory_contracts() -> Result<()> {
        let schema = Schema::new(vec![Field::new("v", DataType::UInt64, true)]);
        let udf = bit_xor_udaf();
        let args = accumulator_args(&schema);
        let values = array(&[Some(7), Some(7), None]);

        let mut ordinary = udf.accumulator(args.clone())?;
        assert!(!ordinary.supports_retract_batch());
        ordinary.update_batch(&[Arc::clone(&values)])?;
        assert_eq!(ordinary.state()?, vec![ScalarValue::UInt64(Some(0))]);

        let mut distinct = udf.create_sliding_accumulator(AccumulatorArgs {
            is_distinct: true,
            ..args
        })?;
        assert!(!distinct.supports_retract_batch());
        distinct.update_batch(&[values])?;
        assert_eq!(distinct.evaluate()?, ScalarValue::UInt64(Some(7)));
        assert_eq!(distinct.state()?.len(), 1);
        Ok(())
    }
}
