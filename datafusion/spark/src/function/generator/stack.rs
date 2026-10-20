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
    Result, ScalarValue, internal_err, plan_err, resources_datafusion_err,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// The most rows a call generates per batch. Spark streams the rows, but they are
/// materialized here, so a large `n` such as `stack(2000000000, 1)` fails instead of
/// exhausting memory.
const MAX_GENERATED_ROWS: usize = 1 << 24;

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
/// `List<Struct<col0, col1, ...>>`, which is the data type of Spark's Generator. As
/// in Spark, the list is not nullable while its elements and fields are. The
/// caller (e.g. Comet, or `unnest` in SQL) is expected to unnest the list.
///
/// As in Spark, `n` is an `INT` in `[1, 2147483647]`, and the values of a column
/// must have the same type, which is the type of its first non-NULL value. NULLs,
/// Arrow types of the same Spark type (such as `Utf8` and `Utf8View`), and integer
/// literals that fit an `INT` column are cast to it, wherever they are in the
/// column. Unlike Spark, which accepts any foldable `n`, `n` must be an integer
/// literal, because the return type is planned before expressions are folded.
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
        if n.data_type() != &DataType::Int32 {
            return plan_err!(
                "The first parameter requires the \"INT\" type, however \"{}\" has the type \"{}\".",
                n.name(),
                n.data_type()
            );
        }
        let Some(n_value) = args.scalar_arguments.first().copied().flatten() else {
            return plan_err!(
                "The first parameter of `stack` must be an integer literal, however \"{}\" is not one.",
                n.name()
            );
        };
        let num_fields = values.len().div_ceil(parse_n(n_value)?);
        let scalar_arguments = args.scalar_arguments.get(1..).unwrap_or_default();
        let fields = column_types(values, scalar_arguments, num_fields)?
            .into_iter()
            .enumerate()
            .map(|(i, data_type)| Field::new(format!("col{i}"), data_type, true))
            .collect::<Fields>();
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::new_list(DataType::Struct(fields), true),
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
        // All-scalar values return one list, which the caller expands to
        // `number_rows` copies
        check_generated_rows(n, number_rows)?;
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
        // `n` is an `INT` as in Spark, while integer literals are BIGINT here. The
        // values are cast to their column's type at execution, since Spark does no
        // implicit casts.
        let mut arg_types = arg_types.to_vec();
        if let Some(n) = arg_types.first_mut().filter(|n| n.is_integer()) {
            *n = DataType::Int32;
        }
        Ok(arg_types)
    }
}

/// Returns the number of rows `n` to generate per input row.
fn parse_n(n: &ScalarValue) -> Result<usize> {
    match n {
        ScalarValue::Int32(Some(v)) if *v > 0 => Ok(*v as usize),
        _ => plan_err!("The `n` must be between (0, 2147483647] (current value = {n})."),
    }
}

/// Returns the type of each output column, which is the type of its first
/// non-NULL value as in Spark's `StackCoercion`. Errors on the first value, in
/// argument order, whose type differs from its column's.
fn column_types(
    arg_fields: &[FieldRef],
    scalar_arguments: &[Option<&ScalarValue>],
    num_fields: usize,
) -> Result<Vec<DataType>> {
    // Integer literals are BIGINT here but INT in Spark, so one that fits is an INT
    // in a column that has an INT value, wherever it is. Being a literal never
    // changes the column type otherwise, which must not change when constant
    // folding turns an expression into a literal.
    let mut has_int32 = vec![false; num_fields];
    for (i, arg_field) in arg_fields.iter().enumerate() {
        has_int32[i % num_fields] |= arg_field.data_type() == &DataType::Int32;
    }
    let mut types = vec![DataType::Null; num_fields];
    for (i, arg_field) in arg_fields.iter().enumerate() {
        let col = i % num_fields;
        let literal = scalar_arguments.get(i).copied().flatten();
        let actual = if has_int32[col]
            && arg_field.data_type() == &DataType::Int64
            && literal.is_some_and(|literal| literal.cast_to(&DataType::Int32).is_ok())
        {
            &DataType::Int32
        } else {
            arg_field.data_type()
        };
        let expected = &mut types[col];
        if expected.is_null() {
            *expected = actual.clone();
        } else if !actual.is_null() && !same_spark_type(actual, expected) {
            return plan_err!(
                "The data type of the column ({col}) do not have the same type: \"{expected}\" ({}) <> \"{actual}\" ({}).",
                col + 1,
                i + 1
            );
        }
    }
    Ok(types)
}

/// Whether two Arrow types are the same Spark type. Types of one logical type, such
/// as `Utf8` and `Utf8View`, match, and list elements and map entries match
/// regardless of their field names, which Spark types do not have. Nullability
/// must match, as in Spark.
fn same_spark_type(left: &DataType, right: &DataType) -> bool {
    use DataType::*;
    fn same_field(left: &FieldRef, right: &FieldRef) -> bool {
        left.is_nullable() == right.is_nullable()
            && same_spark_type(left.data_type(), right.data_type())
    }
    left == right
        || match (left, right) {
            (
                List(left) | LargeList(left) | ListView(left) | LargeListView(left),
                List(right) | LargeList(right) | ListView(right) | LargeListView(right),
            ) => same_field(left, right),
            // Map casts match the key and value fields by position
            (Map(left, _), Map(right, _)) => {
                match (left.data_type(), right.data_type()) {
                    (Struct(left), Struct(right)) => {
                        left.len() == right.len()
                            && left
                                .iter()
                                .zip(right.iter())
                                .all(|(l, r)| same_field(l, r))
                    }
                    _ => false,
                }
            }
            (Struct(left), Struct(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right.iter()).all(|(left, right)| {
                        left.name() == right.name() && same_field(left, right)
                    })
            }
            _ => NativeType::from(left) == NativeType::from(right),
        }
}

/// Errors if generating `n` rows for each of `number_rows` input rows exceeds
/// `MAX_GENERATED_ROWS`.
fn check_generated_rows(n: usize, number_rows: usize) -> Result<()> {
    if number_rows.saturating_mul(n) > MAX_GENERATED_ROWS {
        return Err(resources_datafusion_err!(
            "stack: generating {n} rows for each of {number_rows} input rows exceeds the limit of {MAX_GENERATED_ROWS} rows per batch"
        ));
    }
    Ok(())
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
    if fields.len() != values.len().div_ceil(n) {
        return internal_err!(
            "stack: {} values in {n} rows need {} fields, got {item}",
            values.len(),
            values.len().div_ceil(n)
        );
    }
    check_generated_rows(n, number_rows)?;
    // Every array is one value per input row, which the kernels rely on
    if let Some(len) = values.iter().find_map(|value| match value {
        ColumnarValue::Array(array) if array.len() != number_rows => Some(array.len()),
        _ => None,
    }) {
        return internal_err!("stack: array of {len} rows in a batch of {number_rows}");
    }
    // The limit keeps the offsets within `i32`
    let offsets = OffsetBuffer::from_repeated_length(n, number_rows);
    let columns = if number_rows == 0 {
        // The column builders below need at least one row
        fields
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect()
    } else {
        // Cast every value to its column's type, as a caller such as Comet may plan
        // the return type itself
        let values = values
            .iter()
            .zip(fields.iter().cycle())
            .map(|(value, field)| value.cast_to(field.data_type(), None))
            .collect::<Result<Vec<_>>>()?;
        if n == 1 {
            // Every value is a column as is, without copying arrays
            ColumnarValue::values_to_arrays(&values)?
        } else {
            stack_columns(&values, fields, n, number_rows)?
        }
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
/// broadcast, and with a single input row both readings agree.
fn stack_columns(
    values: &[ColumnarValue],
    fields: &Fields,
    n: usize,
    number_rows: usize,
) -> Result<Vec<ArrayRef>> {
    let num_fields = fields.len();
    let mut sources = Vec::with_capacity(values.len().div_ceil(num_fields));
    fields
        .iter()
        .enumerate()
        .map(|(col, field)| {
            let data_type = field.data_type();
            sources.clear();
            for value in values.iter().skip(col).step_by(num_fields) {
                sources.push(value.to_array(1)?);
            }
            macro_rules! primitive_helper {
                ($t:ty) => {
                    Ok(stack_primitive::<$t>(&sources, n, number_rows, data_type))
                };
            }
            downcast_primitive! {
                data_type => (primitive_helper),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
                    if sources.iter().all(|source| source.len() == 1) =>
                {
                    // The same `n` values for every input row, such as unpivot
                    // keys, which `tile` copies more than 10x faster than `take`
                    tile(stack_take(&sources, n, 1, data_type)?, number_rows)
                }
                _ => stack_take(&sources, n, number_rows, data_type),
            }
        })
        .collect()
}

/// Writes source `i` to every `n`-th slot starting at `i`, and leaves the slots of
/// padding rows NULL. This avoids the index lookups of `take`, and only visits the
/// NULL slots of the validity. It is about 2-3x faster for primitive types.
fn stack_primitive<T: ArrowPrimitiveType>(
    sources: &[ArrayRef],
    n: usize,
    number_rows: usize,
    data_type: &DataType,
) -> ArrayRef {
    let len = number_rows * n;
    let mut buffer = vec![T::Native::default(); len];
    let mut nulls = NullBufferBuilder::new_with_len(len);
    for (i, source) in sources.iter().enumerate() {
        let source = source.as_primitive::<T>();
        let per_row = source.len() > 1;
        let slots = buffer[i..].iter_mut().step_by(n);
        // `for` loops, as `for_each` on these iterators is 2x slower
        if per_row {
            for (slot, value) in slots.zip(source.values().iter()) {
                *slot = *value;
            }
        } else if source.is_valid(0) {
            let value = source.value(0);
            for slot in slots {
                *slot = value;
            }
        }
        if let Some(source_nulls) = source.nulls().filter(|nulls| nulls.null_count() > 0)
        {
            if per_row {
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

/// Takes every output value from the concatenated sources, plus one NULL at the
/// end when there are padding rows. `take` has dedicated kernels for every type,
/// such as `Boolean`, and keeps the data buffers of view types shared.
fn stack_take(
    sources: &[ArrayRef],
    n: usize,
    number_rows: usize,
    data_type: &DataType,
) -> Result<ArrayRef> {
    let padding = n - sources.len();
    // A NULL only when there is padding, as any NULL makes `take` track validity
    let null = (padding > 0).then(|| new_null_array(data_type, 1));
    let mut arrays = Vec::with_capacity(sources.len() + 1);
    // Source `i` is read at `start + input_row * step`
    let mut steps = Vec::with_capacity(sources.len());
    let mut start = 0;
    for source in sources {
        arrays.push(source.as_ref());
        steps.push((start, u32::from(source.len() > 1)));
        start += source.len() as u32;
    }
    arrays.extend(null.as_deref());
    let null_index = start;
    let concatenated = concat(&arrays)?;
    // Padding slots keep the NULL's index
    let mut indices = vec![null_index; number_rows * n];
    for (i, &(start, step)) in steps.iter().enumerate() {
        for (slot, input_row) in indices[i..].iter_mut().step_by(n).zip(0u32..) {
            *slot = start + input_row * step;
        }
    }
    Ok(take(&concatenated, &UInt32Array::from(indices), None)?)
}

/// Concatenates `count` copies of `block`. It doubles the block up to 256 copies
/// and then concatenates references to it, so the output is written about once.
fn tile(mut block: ArrayRef, count: usize) -> Result<ArrayRef> {
    let len = block.len();
    let mut copies = 1;
    while copies * 2 <= count.min(256) {
        block = concat(&[block.as_ref(), block.as_ref()])?;
        copies *= 2;
    }
    let tail = block.slice(0, (count % copies) * len);
    let mut parts = vec![block.as_ref(); count / copies];
    parts.push(tail.as_ref());
    Ok(concat(&parts)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn item(fields: Vec<Field>) -> FieldRef {
        Arc::new(Field::new_list_field(DataType::Struct(fields.into()), true))
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
    fn test_stack_generated_rows_limit() {
        let item = item(vec![Field::new("col0", DataType::Int64, true)]);
        let values = [ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))];
        let err = stack(&values, &item, i32::MAX as usize, 1).unwrap_err();
        assert!(
            err.to_string().contains(
                "generating 2147483647 rows for each of 1 input rows exceeds the limit"
            ),
            "{err}"
        );
        assert!(stack(&values, &item, MAX_GENERATED_ROWS, 1).is_ok());
    }

    #[test]
    fn test_stack_all_scalar_limit_uses_batch_size() {
        let item = item(vec![Field::new("col0", DataType::Int64, true)]);
        let return_field = Arc::new(Field::new(
            "stack",
            DataType::List(Arc::clone(&item)),
            false,
        ));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1 << 20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            arg_fields: vec![],
            number_rows: 8192,
            return_field,
            config_options: Arc::default(),
        };
        let err = SparkStack::new().invoke_with_args(args).unwrap_err();
        assert!(
            err.to_string().contains(
                "generating 1048576 rows for each of 8192 input rows exceeds the limit"
            ),
            "{err}"
        );
    }

    #[test]
    fn test_column_types_widen_only_bigint_literals() {
        let fields = |types: [DataType; 2]| -> Vec<FieldRef> {
            types.map(|t| Arc::new(Field::new("a", t, true))).to_vec()
        };
        let small = ScalarValue::Int16(Some(0));
        let err = column_types(
            &fields([DataType::Int32, DataType::Int16]),
            &[None, Some(&small)],
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("\"Int32\" (1) <> \"Int16\" (2)"));
        let big = ScalarValue::Int64(Some(0));
        let types = column_types(
            &fields([DataType::Int32, DataType::Int64]),
            &[None, Some(&big)],
            1,
        )
        .unwrap();
        assert_eq!(types, [DataType::Int32]);
    }

    #[test]
    fn test_stack_array_length_mismatch() {
        let item = item(vec![Field::new("col0", DataType::Int64, true)]);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let err = stack(&[ColumnarValue::Array(array)], &item, 1, 2).unwrap_err();
        assert!(err.to_string().contains("array of 3 rows in a batch of 2"));
    }

    #[test]
    fn test_tile_block_boundaries() -> Result<()> {
        let block: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        for count in [0, 1, 2, 255, 256, 257, 511, 512, 513, 1000] {
            let tiled = tile(Arc::clone(&block), count)?;
            let expected = (0..count).flat_map(|_| [1, 2]).collect::<Vec<i64>>();
            assert_eq!(
                tiled.as_primitive::<arrow::datatypes::Int64Type>().values(),
                &expected
            );
        }
        Ok(())
    }
}
