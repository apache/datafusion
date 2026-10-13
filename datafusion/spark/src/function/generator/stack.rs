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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, ListArray, NullBufferBuilder, PrimitiveArray, StructArray,
    UInt32Array, downcast_primitive, new_empty_array, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{concat, take};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, FieldRef, Fields};
use datafusion_common::types::NativeType;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, internal_err, plan_err,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Spark-compatible `stack` expression
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#stack>
///
/// `stack(n, expr1, ..., exprk)` separates `expr1`, ..., `exprk` into `n` rows of
/// `ceil(k / n)` columns named `col0`, `col1`, ... The values fill the rows in
/// order, and the last rows are padded with NULLs.
///
/// Note: In Spark, `stack` is a Generator that produces rows. In DataFusion, a
/// ScalarUDF can only return one value per row, so the `n` rows are returned as a
/// `List<Struct<col0, col1, ...>>`, which is the data type of Spark's Generator.
/// The caller (e.g. Comet, or `unnest` in SQL) is expected to unnest the list.
///
/// As in Spark, the values of a column must have the same type, which is the type
/// of its first non-NULL value. NULLs, Arrow types of the same Spark type (such as
/// `Utf8` and `Utf8View`), and integer literals that fit are cast to it. Unlike
/// Spark, which accepts any foldable `INT`, `n` must be an integer literal in
/// `[1, 2147483647]`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStack {
    signature: Signature,
}

impl Default for SparkStack {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStack {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkStack {
    fn name(&self) -> &str {
        "stack"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    /// Validates the arguments in the order Spark's `Stack.checkInputDataTypes` does.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (n, values) = match args.arg_fields {
            [n, values @ ..] if !values.is_empty() => (n, values),
            fields => {
                return plan_err!(
                    "The `stack` requires > 1 parameters but the actual number is {}.",
                    fields.len()
                );
            }
        };
        if !n.data_type().is_integer() {
            return plan_err!(
                "The first parameter requires the \"INT\" type, however \"{}\" has the type \"{}\".",
                n.name(),
                n.data_type()
            );
        }
        let Some(n_value) = args.scalar_arguments.first().copied().flatten() else {
            return plan_err!(
                "the input `n` should be a foldable \"INT\" expression; however, got \"{}\". Only an integer literal is supported.",
                n.name()
            );
        };
        let num_fields = values.len().div_ceil(parse_n(n_value)?);
        let literals = args.scalar_arguments.get(1..).unwrap_or_default();
        let fields = column_types(values, literals, num_fields)?
            .into_iter()
            .enumerate()
            .map(|(i, data_type)| Field::new(format!("col{i}"), data_type, true))
            .collect::<Fields>();
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::new_list(DataType::Struct(fields), false),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_field,
            ..
        } = args;
        let DataType::List(item) = return_field.data_type() else {
            return internal_err!("stack: unexpected return type {return_field}");
        };
        let [ColumnarValue::Scalar(n), values @ ..] = args.as_slice() else {
            return internal_err!("stack: `n` must be a scalar");
        };
        let n = parse_n(n)?;
        if values.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
            let list = stack(values, item, n, 1)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list))));
        }
        Ok(ColumnarValue::Array(Arc::new(stack(
            values,
            item,
            n,
            number_rows,
        )?)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Spark does no implicit casts. Values are cast to their column's type at
        // execution.
        Ok(arg_types.to_vec())
    }
}

/// Returns the number of rows `n` to generate per input row.
fn parse_n(n: &ScalarValue) -> Result<usize> {
    match n.cast_to(&DataType::Int32) {
        Ok(ScalarValue::Int32(Some(v))) if v > 0 => Ok(v as usize),
        _ => plan_err!("The `n` must be between (0, 2147483647] (current value = {n})."),
    }
}

/// Returns the type of each output column, which is the type of its first
/// non-NULL value as in Spark's `StackCoercion`. Errors on the first value, in
/// argument order, whose type differs from its column's.
fn column_types(
    values: &[FieldRef],
    literals: &[Option<&ScalarValue>],
    num_fields: usize,
) -> Result<Vec<DataType>> {
    let mut types = vec![DataType::Null; num_fields];
    for (i, value) in values.iter().enumerate() {
        let col = i % num_fields;
        let (expected, actual) = (&mut types[col], value.data_type());
        if expected.is_null() {
            *expected = actual.clone();
            continue;
        }
        if actual.is_null() || same_spark_type(actual, expected) {
            continue;
        }
        // Integer literals are BIGINT here but INT in Spark, so one that fits is
        // cast to an integer column. Being a literal never changes the column type,
        // which must not change when constant folding turns an expression into a
        // literal.
        let literal = literals.get(i).copied().flatten();
        if actual.is_integer()
            && expected.is_integer()
            && literal.is_some_and(|literal| literal.cast_to(expected).is_ok())
        {
            continue;
        }
        return plan_err!(
            "The data type of the column ({col}) do not have the same type: \"{expected}\" ({}) <> \"{actual}\" ({}).",
            col + 1,
            i + 1
        );
    }
    Ok(types)
}

/// Whether two Arrow types are the same Spark type. Types of one logical type, such
/// as `Utf8` and `Utf8View`, match, and list elements match regardless of their
/// field name, which Spark arrays do not have.
fn same_spark_type(left: &DataType, right: &DataType) -> bool {
    use DataType::*;
    left == right
        || match (left, right) {
            (
                List(left) | LargeList(left) | ListView(left) | LargeListView(left),
                List(right) | LargeList(right) | ListView(right) | LargeListView(right),
            ) => {
                left.is_nullable() == right.is_nullable()
                    && same_spark_type(left.data_type(), right.data_type())
            }
            (Struct(left), Struct(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right.iter()).all(|(left, right)| {
                        left.name() == right.name()
                            && left.is_nullable() == right.is_nullable()
                            && same_spark_type(left.data_type(), right.data_type())
                    })
            }
            _ => NativeType::from(left) == NativeType::from(right),
        }
}

/// Generates `n` rows of `item`'s struct type for each of `number_rows` input
/// rows. Field `j` of generated row `i` is `values[i * num_fields + j]`, or NULL
/// past the last value.
fn stack(
    values: &[ColumnarValue],
    item: &FieldRef,
    n: usize,
    number_rows: usize,
) -> Result<ListArray> {
    let DataType::Struct(fields) = item.data_type() else {
        return internal_err!("stack: unexpected element type {item}");
    };
    let offsets =
        OffsetBuffer::try_from_repeated_length(n, number_rows).map_err(|_| {
            exec_datafusion_err!(
                "stack: {number_rows} rows of {n} generated rows exceed the maximum list size"
            )
        })?;
    let columns = if number_rows == 0 {
        fields
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect()
    } else if n == 1 {
        // Every value is a column as is, without copying arrays
        ColumnarValue::values_to_arrays(values)?
    } else {
        stack_columns(values, fields, n, number_rows)?
    };
    let rows = StructArray::try_new(fields.clone(), columns, None)?;
    Ok(ListArray::try_new(
        Arc::clone(item),
        offsets,
        Arc::new(rows),
        None,
    )?)
}

/// Builds each output column from its values, one per generated row, with the
/// rows past the last value as NULL padding. An array value is read at the input
/// row. A scalar is a single value array read at index 0, so it is never
/// broadcast.
fn stack_columns(
    values: &[ColumnarValue],
    fields: &Fields,
    n: usize,
    number_rows: usize,
) -> Result<Vec<ArrayRef>> {
    macro_rules! primitive_helper {
        ($t:ty, $sources:ident, $is_array:ident, $n:ident, $number_rows:ident, $data_type:ident) => {
            Ok(stack_primitive::<$t>(
                &$sources,
                &$is_array,
                $n,
                $number_rows,
                $data_type,
            ))
        };
    }
    let num_fields = fields.len();
    let mut sources = Vec::with_capacity(values.len().div_ceil(num_fields));
    let mut is_array = Vec::with_capacity(sources.capacity());
    fields
        .iter()
        .enumerate()
        .map(|(col, field)| {
            let data_type = field.data_type();
            sources.clear();
            is_array.clear();
            for value in values.iter().skip(col).step_by(num_fields) {
                sources.push(value.cast_to(data_type, None)?.into_array(1)?);
                is_array.push(matches!(value, ColumnarValue::Array(_)));
            }
            downcast_primitive! {
                data_type => (primitive_helper, sources, is_array, n, number_rows, data_type),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
                    if !is_array.contains(&true) =>
                {
                    // The same `n` values for every input row, so `n` copies of
                    // them are cheaper than one copy per value
                    let padding = new_null_array(data_type, n - sources.len());
                    let mut pattern = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
                    pattern.push(padding.as_ref());
                    repeat(concat(&pattern)?, number_rows)
                }
                _ => stack_take(&sources, &is_array, n, number_rows, data_type),
            }
        })
        .collect()
}

/// Writes source `i` to every `n`-th slot starting at `i`, and leaves the slots of
/// padding rows NULL. This avoids the index lookups of `take`, and only visits the
/// NULL slots of the validity. It is about 2-3x faster for primitive types.
fn stack_primitive<T: ArrowPrimitiveType>(
    sources: &[ArrayRef],
    is_array: &[bool],
    n: usize,
    number_rows: usize,
    data_type: &DataType,
) -> ArrayRef {
    let len = number_rows * n;
    let mut buffer = vec![T::Native::default(); len];
    let mut nulls = NullBufferBuilder::new_with_len(len);
    for (i, (source, &is_array)) in sources.iter().zip(is_array).enumerate() {
        let source = source.as_primitive::<T>();
        let slots = buffer[i..].iter_mut().step_by(n);
        // `for` loops, as `for_each` on these iterators is 2x slower
        if is_array {
            for (slot, value) in slots.zip(source.values().iter()) {
                *slot = *value;
            }
        } else {
            let value = source.value(0);
            for slot in slots {
                *slot = value;
            }
        }
        if let Some(source_nulls) = source.nulls().filter(|nulls| nulls.null_count() > 0)
        {
            if is_array {
                for input_row in (!source_nulls.inner()).set_indices() {
                    nulls.set_bit(input_row * n + i, false);
                }
            } else {
                for input_row in 0..number_rows {
                    nulls.set_bit(input_row * n + i, false);
                }
            }
        }
    }
    for i in sources.len()..n {
        for input_row in 0..number_rows {
            nulls.set_bit(input_row * n + i, false);
        }
    }
    Arc::new(
        PrimitiveArray::<T>::new(buffer.into(), nulls.build())
            .with_data_type(data_type.clone()),
    )
}

/// Takes every output value from the concatenated sources, with one more NULL at
/// the end for the padding rows. `take` has dedicated kernels for every type,
/// such as `Boolean`, and keeps the data buffers of view types shared.
fn stack_take(
    sources: &[ArrayRef],
    is_array: &[bool],
    n: usize,
    number_rows: usize,
    data_type: &DataType,
) -> Result<ArrayRef> {
    let null = new_null_array(data_type, 1);
    let mut arrays = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    arrays.push(null.as_ref());
    let values = concat(&arrays)?;
    // Source `i` is read at `start + input_row * step`
    let mut start = 0;
    let steps = sources
        .iter()
        .zip(is_array)
        .map(|(source, &is_array)| {
            let step = (start, u32::from(is_array));
            start += source.len() as u32;
            step
        })
        .collect::<Vec<_>>();
    let padding = n - sources.len();
    let mut indices = Vec::with_capacity(number_rows * n);
    for input_row in 0..number_rows as u32 {
        indices.extend(steps.iter().map(|&(start, step)| start + input_row * step));
        // `start` is now the index of the NULL
        indices.extend(std::iter::repeat_n(start, padding));
    }
    Ok(take(&values, &UInt32Array::from(indices), None)?)
}

/// Concatenates `count` copies of `array`, doubling a block of copies so it
/// takes O(log count) bulk copies instead of one copy per value.
fn repeat(mut block: ArrayRef, mut count: usize) -> Result<ArrayRef> {
    let mut parts = Vec::with_capacity(count.count_ones() as usize);
    while count > 0 {
        if count & 1 == 1 {
            parts.push(Arc::clone(&block));
        }
        count >>= 1;
        if count > 0 {
            block = concat(&[block.as_ref(), block.as_ref()])?;
        }
    }
    Ok(concat(
        &parts.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn item(fields: Vec<Field>) -> FieldRef {
        Arc::new(Field::new_list_field(
            DataType::Struct(fields.into()),
            false,
        ))
    }

    #[test]
    fn test_stack_empty_batch() -> Result<()> {
        let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let values = [
            ColumnarValue::Array(Arc::clone(&empty)),
            ColumnarValue::Array(empty),
        ];
        let item = item(vec![Field::new("col0", DataType::Int64, true)]);
        let list = stack(&values, &item, 2, 0)?;
        assert!(list.is_empty() && list.values().is_empty());
        Ok(())
    }

    #[test]
    fn test_stack_list_size_overflow() {
        let item = item(vec![Field::new("col0", DataType::Int64, true)]);
        let values = [ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))];
        let err = stack(&values, &item, i32::MAX as usize, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("2 rows of 2147483647 generated rows exceed"),
            "{err}"
        );
    }
}
