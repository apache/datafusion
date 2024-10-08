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

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use ahash::RandomState;
use arrow::array::{downcast_integer, Array, ArrayRef, AsArray};
use arrow::datatypes::{
    ArrowNativeType, ArrowNumericType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_schema::Field;

use datafusion_common::cast::as_list_array;
use datafusion_common::{exec_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::INTEGERS;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    Signature, Volatility,
};

use datafusion_expr::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign};
use std::sync::OnceLock;

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

/// `accumulator_helper` is a macro accepting (ArrowPrimitiveType, BitwiseOperationType, bool)
macro_rules! accumulator_helper {
    ($t:ty, $opr:expr, $is_distinct: expr) => {
        match $opr {
            BitwiseOperationType::And => Ok(Box::<BitAndAccumulator<$t>>::default()),
            BitwiseOperationType::Or => Ok(Box::<BitOrAccumulator<$t>>::default()),
            BitwiseOperationType::Xor => {
                if $is_distinct {
                    Ok(Box::<DistinctBitXorAccumulator<$t>>::default())
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
    ($args:ident, $opr:expr, $is_distinct: expr) => {
        match $args.return_type {
            DataType::Int8 => accumulator_helper!(Int8Type, $opr, $is_distinct),
            DataType::Int16 => accumulator_helper!(Int16Type, $opr, $is_distinct),
            DataType::Int32 => accumulator_helper!(Int32Type, $opr, $is_distinct),
            DataType::Int64 => accumulator_helper!(Int64Type, $opr, $is_distinct),
            DataType::UInt8 => accumulator_helper!(UInt8Type, $opr, $is_distinct),
            DataType::UInt16 => accumulator_helper!(UInt16Type, $opr, $is_distinct),
            DataType::UInt32 => accumulator_helper!(UInt32Type, $opr, $is_distinct),
            DataType::UInt64 => accumulator_helper!(UInt64Type, $opr, $is_distinct),
            _ => {
                not_impl_err!(
                    "{} not supported for {}: {}",
                    stringify!($opr),
                    $args.name,
                    $args.return_type
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

static BIT_AND_DOC: OnceLock<Documentation> = OnceLock::new();

fn get_bit_and_doc() -> &'static Documentation {
    BIT_AND_DOC.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description("Computes the bitwise AND of all non-null input values.")
            .with_syntax_example("bit_and(expression)")
            .with_standard_argument("expression", "Integer")
            .build()
            .unwrap()
    })
}

static BIT_OR_DOC: OnceLock<Documentation> = OnceLock::new();

fn get_bit_or_doc() -> &'static Documentation {
    BIT_OR_DOC.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description("Computes the bitwise OR of all non-null input values.")
            .with_syntax_example("bit_or(expression)")
            .with_standard_argument("expression", "Integer")
            .build()
            .unwrap()
    })
}

static BIT_XOR_DOC: OnceLock<Documentation> = OnceLock::new();

fn get_bit_xor_doc() -> &'static Documentation {
    BIT_XOR_DOC.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description(
                "Computes the bitwise exclusive OR of all non-null input values.",
            )
            .with_syntax_example("bit_xor(expression)")
            .with_standard_argument("expression", "Integer")
            .build()
            .unwrap()
    })
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
#[derive(Debug, Clone, Eq, PartialEq)]
enum BitwiseOperationType {
    And,
    Or,
    Xor,
}

impl Display for BitwiseOperationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// [BitwiseOperation] struct encapsulates information about a bitwise operation.
#[derive(Debug)]
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
            signature: Signature::uniform(1, INTEGERS.to_vec(), Volatility::Immutable),
            func_name,
            documentation,
        }
    }
}

impl AggregateUDFImpl for BitwiseOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.func_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];
        if !arg_type.is_integer() {
            return exec_err!(
                "[return_type] {} not supported for {}",
                self.name(),
                arg_type
            );
        }
        Ok(arg_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        downcast_bitwise_accumulator!(acc_args, self.operation, acc_args.is_distinct)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if self.operation == BitwiseOperationType::Xor && args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(
                    args.name,
                    format!("{} distinct", self.name()).as_str(),
                ),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new("item", args.return_type.clone(), true),
                false,
            )])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, self.name()),
                args.return_type.clone(),
                true,
            )])
        }
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.return_type;
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

    fn documentation(&self) -> Option<&Documentation> {
        Some(self.documentation)
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
        std::mem::size_of_val(self)
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
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v | x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v ^ x;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // XOR is it's own inverse
        self.update_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
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
    T::Native: std::ops::BitXor<Output = T::Native> + std::hash::Hash + Eq,
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
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<T::Native>()
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
    use arrow::datatypes::UInt64Type;
    use datafusion_common::ScalarValue;

    use crate::bit_and_or_xor::BitXorAccumulator;
    use datafusion_expr::Accumulator;

    #[test]
    fn test_bit_xor_accumulator() {
        let mut accumulator = BitXorAccumulator::<UInt64Type> { value: None };
        let batches: Vec<_> = vec![vec![1, 2], vec![1]]
            .into_iter()
            .map(|b| Arc::new(b.into_iter().collect::<UInt64Array>()) as ArrayRef)
            .collect();

        let added = &[Arc::clone(&batches[0])];
        let retracted = &[Arc::clone(&batches[1])];

        // XOR of 1..3 is 3
        accumulator.update_batch(added).unwrap();
        assert_eq!(
            accumulator.evaluate().unwrap(),
            ScalarValue::UInt64(Some(3))
        );

        // Removing [1] ^ 3 = 2
        accumulator.retract_batch(retracted).unwrap();
        assert_eq!(
            accumulator.evaluate().unwrap(),
            ScalarValue::UInt64(Some(2))
        );
    }
}
