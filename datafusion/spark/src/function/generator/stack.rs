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
    Array, ArrayRef, AsArray, BooleanBufferBuilder, ListArray, PrimitiveArray,
    StructArray, downcast_primitive, new_empty_array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, concat, interleave};
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
/// The caller (e.g. Comet) is expected to unnest the list.
///
/// As in Spark, `n` must be an integer literal in `[1, 2147483647]`, and the
/// values of a column must have the same type. Only untyped NULLs are coerced, to
/// the type of the first non-NULL value in their column.
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
        let [n, values @ ..] = args.arg_fields else {
            return plan_err!(
                "The `stack` requires > 1 parameters but the actual number is 0."
            );
        };
        if values.is_empty() {
            return plan_err!(
                "The `stack` requires > 1 parameters but the actual number is 1."
            );
        }
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
        let fields = column_types(values, num_fields)?
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
        // Spark does no implicit casts. NULLs are coerced per column at execution.
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
fn column_types(values: &[FieldRef], num_fields: usize) -> Result<Vec<DataType>> {
    let mut types = vec![DataType::Null; num_fields];
    for (i, value) in values.iter().enumerate() {
        let column_type = &mut types[i % num_fields];
        if column_type.is_null() {
            *column_type = value.data_type().clone();
        }
    }
    for (i, value) in values.iter().enumerate() {
        let col = i % num_fields;
        let (expected, actual) = (&types[col], value.data_type());
        // Compare logical types, so `Utf8` and `Utf8View` are both Spark's STRING
        if !actual.is_null()
            && actual != expected
            && NativeType::from(actual) != NativeType::from(expected)
        {
            return plan_err!(
                "The data type of the column ({col}) do not have the same type: \"{expected}\" ({}) <> \"{actual}\" ({}).",
                col + 1,
                i + 1
            );
        }
    }
    Ok(types)
}

/// Generates `n` rows of `item`'s struct type for each of `num_rows` input rows.
/// Field `j` of generated row `i` is `values[i * num_fields + j]`, or NULL past the
/// last value.
fn stack(
    values: &[ColumnarValue],
    item: &FieldRef,
    n: usize,
    num_rows: usize,
) -> Result<ListArray> {
    let DataType::Struct(fields) = item.data_type() else {
        return internal_err!("stack: unexpected element type {item}");
    };
    let offsets = OffsetBuffer::try_from_repeated_length(n, num_rows).map_err(|_| {
        exec_datafusion_err!(
            "stack: {num_rows} rows of {n} generated rows exceed the maximum list size"
        )
    })?;
    let columns = if n == 1 {
        // Every value is a column as is, without copying arrays
        values
            .iter()
            .zip(fields.iter())
            .map(|(value, field)| cast_to(value.to_array(num_rows)?, field.data_type()))
            .collect::<Result<_>>()?
    } else {
        stack_columns(values, fields, n, num_rows)?
    };
    let rows = StructArray::try_new(fields.clone(), columns, None)?;
    Ok(ListArray::try_new(
        Arc::clone(item),
        offsets,
        Arc::new(rows),
        None,
    )?)
}

/// Builds each output column from its `n` values. An array value is read at the
/// input row. A scalar or NULL is a single value array read at index 0, so it is
/// never broadcast.
fn stack_columns(
    values: &[ColumnarValue],
    fields: &Fields,
    n: usize,
    num_rows: usize,
) -> Result<Vec<ArrayRef>> {
    macro_rules! primitive_helper {
        ($t:ty, $sources:ident, $per_row:ident, $num_rows:ident, $data_type:ident) => {
            Ok(stack_primitive::<$t>(
                &$sources, &$per_row, $num_rows, $data_type,
            ))
        };
    }
    let num_fields = fields.len();
    let mut indices = vec![];
    fields
        .iter()
        .enumerate()
        .map(|(col, field)| {
            let data_type = field.data_type();
            if data_type.is_null() {
                return Ok(new_null_array(data_type, num_rows * n));
            }
            let null = new_null_array(data_type, 1);
            let mut sources = Vec::with_capacity(n);
            let mut per_row = Vec::with_capacity(n);
            for row in 0..n {
                let (source, is_array) = match values.get(row * num_fields + col) {
                    Some(ColumnarValue::Array(array)) if !array.data_type().is_null() => {
                        (cast_to(Arc::clone(array), data_type)?, true)
                    }
                    Some(ColumnarValue::Scalar(scalar)) if !scalar.is_null() => {
                        (cast_to(scalar.to_array()?, data_type)?, false)
                    }
                    _ => (Arc::clone(&null), false),
                };
                sources.push(source);
                per_row.push(is_array);
            }
            if !per_row.contains(&true) {
                // The same `n` values for every input row
                let sources = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
                return repeat(concat(&sources)?, num_rows);
            }
            downcast_primitive! {
                data_type => (primitive_helper, sources, per_row, num_rows, data_type),
                _ => {
                    indices.clear();
                    indices.try_reserve(num_rows * n).map_err(|e| {
                        exec_datafusion_err!("stack: failed to allocate indices: {e}")
                    })?;
                    for input_row in 0..num_rows {
                        indices.extend(per_row.iter().enumerate().map(|(i, &per_row)| {
                            (i, if per_row { input_row } else { 0 })
                        }));
                    }
                    let sources = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
                    Ok(interleave(&sources, &indices)?)
                }
            }
        })
        .collect()
}

/// Writes source `i` to every `n`-th slot starting at `i`. This avoids the
/// index lookups of `interleave`, and only visits the NULL slots of the
/// validity. It is about 2-3x faster for primitive types.
fn stack_primitive<T: ArrowPrimitiveType>(
    sources: &[ArrayRef],
    per_row: &[bool],
    num_rows: usize,
    data_type: &DataType,
) -> ArrayRef {
    // `values[i..]` below needs at least one row
    if num_rows == 0 {
        return new_empty_array(data_type);
    }
    let n = sources.len();
    let len = num_rows * n;
    let mut values = vec![T::Native::default(); len];
    let mut validity: Option<BooleanBufferBuilder> = None;
    for (i, (source, &per_row)) in sources.iter().zip(per_row).enumerate() {
        let source = source.as_primitive::<T>();
        let slots = values[i..].iter_mut().step_by(n);
        // `for` loops, as `for_each` on these iterators is 2x slower
        if per_row {
            for (slot, value) in slots.zip(source.values().iter()) {
                *slot = *value;
            }
        } else {
            let value = source.value(0);
            for slot in slots {
                *slot = value;
            }
        }
        if let Some(nulls) = source.nulls().filter(|nulls| nulls.null_count() > 0) {
            let validity = validity.get_or_insert_with(|| {
                let mut validity = BooleanBufferBuilder::new(len);
                validity.append_n(len, true);
                validity
            });
            if per_row {
                for row in (!nulls.inner()).set_indices() {
                    validity.set_bit(row * n + i, false);
                }
            } else {
                for row in 0..num_rows {
                    validity.set_bit(row * n + i, false);
                }
            }
        }
    }
    let nulls = validity.map(|mut validity| NullBuffer::new(validity.finish()));
    Arc::new(
        PrimitiveArray::<T>::new(values.into(), nulls).with_data_type(data_type.clone()),
    )
}

/// Concatenates `count` copies of `array`, doubling a block of copies so it
/// takes O(log count) bulk copies instead of one copy per value.
fn repeat(array: ArrayRef, count: usize) -> Result<ArrayRef> {
    let mut parts = vec![];
    let mut block = array;
    let mut count = count;
    while count > 0 {
        if count & 1 == 1 {
            parts.push(Arc::clone(&block));
        }
        count >>= 1;
        if count > 0 {
            block = concat(&[block.as_ref(), block.as_ref()])?;
        }
    }
    match parts.as_slice() {
        [] => Ok(block.slice(0, 0)),
        [part] => Ok(Arc::clone(part)),
        parts => Ok(concat(
            &parts.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
        )?),
    }
}

fn cast_to(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        Ok(array)
    } else {
        Ok(cast(&array, data_type)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    #[test]
    fn test_stack_matches_interleave() -> Result<()> {
        for num_rows in [0, 1000] {
            check_stack_matches_interleave(num_rows)?;
        }
        Ok(())
    }

    fn check_stack_matches_interleave(num_rows: usize) -> Result<()> {
        let ints: ArrayRef = Arc::new(
            (0..num_rows as i64)
                .map(|i| (i % 7 != 0).then_some(i))
                .collect::<Int64Array>(),
        );
        let strings: ArrayRef = Arc::new(
            (0..num_rows)
                .map(|i| (i % 5 != 0).then(|| format!("s{i}")))
                .collect::<StringArray>(),
        );
        // 3 rows of 3 columns: col0 takes the primitive path, col1 is all
        // scalars and padding, col2 takes the `interleave` path
        let values = [
            ColumnarValue::Array(Arc::clone(&ints)),
            ColumnarValue::Scalar(ScalarValue::from("a")),
            ColumnarValue::Array(strings),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::from("b")),
            ColumnarValue::Array(ints),
        ];
        let fields = Fields::from(vec![
            Field::new("col0", DataType::Int64, true),
            Field::new("col1", DataType::Utf8, true),
            Field::new("col2", DataType::Utf8, true),
        ]);
        let item = Arc::new(Field::new_list_field(
            DataType::Struct(fields.clone()),
            false,
        ));
        let (n, num_fields) = (3, fields.len());
        let list = stack(&values, &item, n, num_rows)?;
        assert_eq!(list.len(), num_rows);
        assert!(list.offsets().lengths().all(|len| len == n));

        let indices = (0..num_rows)
            .flat_map(|row| (0..n).map(move |i| (i, row)))
            .collect::<Vec<_>>();
        for (col, field) in fields.iter().enumerate() {
            let sources = (0..n)
                .map(|i| match values.get(i * num_fields + col) {
                    Some(value) if !value.data_type().is_null() => {
                        value.to_array(num_rows)
                    }
                    _ => Ok(new_null_array(field.data_type(), num_rows)),
                })
                .collect::<Result<Vec<_>>>()?;
            let sources = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            let expected = interleave(&sources, &indices)?;
            assert_eq!(list.values().as_struct().column(col), &expected, "{field}");
        }
        Ok(())
    }

    #[test]
    fn test_stack_list_size_overflow() {
        let item = Arc::new(Field::new_list_field(
            DataType::Struct(vec![Field::new("col0", DataType::Int64, true)].into()),
            false,
        ));
        let values = [ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))];
        let err = stack(&values, &item, i32::MAX as usize, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("2 rows of 2147483647 generated rows exceed"),
            "{err}"
        );
    }
}
