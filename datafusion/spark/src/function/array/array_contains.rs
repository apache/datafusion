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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, FixedSizeListArray,
    GenericListArray, OffsetSizeTrait, StructArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::unary;
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::array_has::array_has_udf;
use std::sync::Arc;

/// Spark-compatible `array_contains` function.
///
/// Float elements are first canonicalized to Spark's equality, where `-0.0`
/// equals `0.0` and all NaNs are equal (also inside lists and structs).
///
/// Calls DataFusion's `array_has` and then applies Spark's null semantics:
/// - If the result from `array_has` is `true`, return `true`.
/// - If the result is `false` and the input array row contains any null elements,
///   return `null` (because the element might have been the null).
/// - If the result is `false` and the input array row has no null elements,
///   return `false`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayContains {
    signature: Signature,
}

impl Default for SparkArrayContains {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayContains {
    fn name(&self) -> &str {
        "array_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut args = args;
        for arg in &mut args.args {
            *arg = canonicalize_floats(arg)?;
        }
        let haystack = args.args[0].clone();
        let array_has_result = array_has_udf().invoke_with_args(args)?;

        let result_array = array_has_result.to_array(1)?;
        let patched = apply_spark_null_semantics(result_array.as_boolean(), &haystack)?;
        Ok(ColumnarValue::Array(Arc::new(patched)))
    }
}

/// Rewrites float values so that bitwise comparison matches Spark's equality:
/// `-0.0` becomes `0.0` and every NaN becomes the canonical NaN. Applies to a
/// float argument and to the elements of a list argument; other values are
/// returned as they are.
fn canonicalize_floats(value: &ColumnarValue) -> Result<ColumnarValue> {
    Ok(match value {
        ColumnarValue::Array(array) => {
            ColumnarValue::Array(canonicalize_float_array(array))
        }
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float32(Some(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(canonical_f32(*v))))
            }
            ScalarValue::Float64(Some(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float64(Some(canonical_f64(*v))))
            }
            ScalarValue::List(list) => ColumnarValue::Scalar(ScalarValue::List(
                canonicalize_float_array(&(Arc::clone(list) as ArrayRef))
                    .as_list::<i32>()
                    .clone()
                    .into(),
            )),
            ScalarValue::LargeList(list) => {
                ColumnarValue::Scalar(ScalarValue::LargeList(
                    canonicalize_float_array(&(Arc::clone(list) as ArrayRef))
                        .as_list::<i64>()
                        .clone()
                        .into(),
                ))
            }
            ScalarValue::FixedSizeList(list) => {
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(
                    canonicalize_float_array(&(Arc::clone(list) as ArrayRef))
                        .as_fixed_size_list()
                        .clone()
                        .into(),
                ))
            }
            ScalarValue::Struct(list) => ColumnarValue::Scalar(ScalarValue::Struct(
                canonicalize_float_array(&(Arc::clone(list) as ArrayRef))
                    .as_struct()
                    .clone()
                    .into(),
            )),
            _ => value.clone(),
        },
    })
}

fn canonical_f32(v: f32) -> f32 {
    if v.is_nan() {
        f32::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn canonical_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn canonicalize_float_array(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => Arc::new(unary::<Float32Type, _, Float32Type>(
            array.as_primitive::<Float32Type>(),
            canonical_f32,
        )),
        DataType::Float64 => Arc::new(unary::<Float64Type, _, Float64Type>(
            array.as_primitive::<Float64Type>(),
            canonical_f64,
        )),
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            Arc::new(GenericListArray::<i32>::new(
                Arc::clone(field),
                list.offsets().clone(),
                canonicalize_float_array(list.values()),
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            Arc::new(GenericListArray::<i64>::new(
                Arc::clone(field),
                list.offsets().clone(),
                canonicalize_float_array(list.values()),
                list.nulls().cloned(),
            ))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            Arc::new(FixedSizeListArray::new(
                Arc::clone(field),
                *size,
                canonicalize_float_array(list.values()),
                list.nulls().cloned(),
            ))
        }
        // An empty struct has no child to infer its length from, so leave it.
        DataType::Struct(fields) if !fields.is_empty() => {
            let array = array.as_struct();
            Arc::new(StructArray::new(
                fields.clone(),
                array
                    .columns()
                    .iter()
                    .map(canonicalize_float_array)
                    .collect(),
                array.nulls().cloned(),
            ))
        }
        _ => Arc::clone(array),
    }
}

/// For each row where `array_has` returned `false`, set the output to null
/// if that row's input array contains any null elements.
fn apply_spark_null_semantics(
    result: &BooleanArray,
    haystack_arg: &ColumnarValue,
) -> Result<BooleanArray> {
    // happy path
    if haystack_arg.data_type() == DataType::Null || !result.has_false() {
        return Ok(result.clone());
    }

    let haystack = haystack_arg.to_array_of_size(result.len())?;

    let row_has_nulls = compute_row_has_nulls(&haystack)?;

    // A row keeps its validity when result is true OR the row has no nulls.
    let keep_mask = result.values() | &!&row_has_nulls;
    let new_validity = match result.nulls() {
        Some(n) => n.inner() & &keep_mask,
        None => keep_mask,
    };

    Ok(BooleanArray::new(
        result.values().clone(),
        Some(NullBuffer::new(new_validity)),
    ))
}

/// Returns a per-row bitmap where bit i is set if row i's list contains any null element.
fn compute_row_has_nulls(haystack: &dyn Array) -> Result<BooleanBuffer> {
    match haystack.data_type() {
        DataType::List(_) => generic_list_row_has_nulls(haystack.as_list::<i32>()),
        DataType::LargeList(_) => generic_list_row_has_nulls(haystack.as_list::<i64>()),
        DataType::FixedSizeList(_, _) => {
            let list = haystack.as_fixed_size_list();
            let buf = match list.values().nulls() {
                Some(nulls) => {
                    let validity = nulls.inner();
                    let vl = list.value_length() as usize;
                    let mut builder = BooleanBufferBuilder::new(list.len());
                    for i in 0..list.len() {
                        builder.append(validity.slice(i * vl, vl).count_set_bits() < vl);
                    }
                    builder.finish()
                }
                None => BooleanBuffer::new_unset(list.len()),
            };
            Ok(mask_with_list_nulls(buf, list.nulls()))
        }
        dt => exec_err!("compute_row_has_nulls: unsupported data type {dt}"),
    }
}

/// Computes per-row null presence for `List` and `LargeList` arrays.
fn generic_list_row_has_nulls<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Result<BooleanBuffer> {
    let buf = match list.values().nulls() {
        Some(nulls) => {
            let validity = nulls.inner();
            let offsets = list.offsets();
            let mut builder = BooleanBufferBuilder::new(list.len());
            for i in 0..list.len() {
                let s = offsets[i].as_usize();
                let len = offsets[i + 1].as_usize() - s;
                builder.append(validity.slice(s, len).count_set_bits() < len);
            }
            builder.finish()
        }
        None => BooleanBuffer::new_unset(list.len()),
    };
    Ok(mask_with_list_nulls(buf, list.nulls()))
}

/// Rows where the list itself is null should not be marked as "has nulls".
fn mask_with_list_nulls(
    buf: BooleanBuffer,
    list_nulls: Option<&NullBuffer>,
) -> BooleanBuffer {
    match list_nulls {
        Some(n) => &buf & n.inner(),
        None => buf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float32Array, Float64Array, Int32Array, LargeListArray, ListArray, ListBuilder,
        builder::Float64Builder,
    };
    use arrow::datatypes::{Field, Fields, Int32Type};
    use datafusion_common::config::ConfigOptions;

    fn invoke(args: Vec<ColumnarValue>, rows: usize) -> Result<BooleanArray> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("a{i}"), a.data_type(), true).into())
            .collect();
        let out = SparkArrayContains::new().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Field::new("r", DataType::Boolean, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        Ok(out.to_array(rows)?.as_boolean().clone())
    }

    fn f64_list(values: Vec<Option<f64>>) -> ListArray {
        let mut builder = ListBuilder::new(Float64Builder::new());
        builder.values().append_slice(&[]);
        for v in values {
            builder.values().append_option(v);
        }
        builder.append(true);
        builder.finish()
    }

    fn neg_nan() -> f64 {
        -f64::NAN
    }

    #[test]
    fn float_needle_scalar_and_array() -> Result<()> {
        let haystack = Arc::new(f64_list(vec![Some(-0.0), Some(neg_nan())]));
        for needle in [0.0, f64::NAN] {
            let scalar = invoke(
                vec![
                    ColumnarValue::Array(Arc::clone(&haystack) as ArrayRef),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(needle))),
                ],
                1,
            )?;
            assert!(scalar.value(0));
            let array = invoke(
                vec![
                    ColumnarValue::Array(Arc::clone(&haystack) as ArrayRef),
                    ColumnarValue::Array(Arc::new(Float64Array::from(vec![needle]))),
                ],
                1,
            )?;
            assert!(array.value(0));
        }
        Ok(())
    }

    #[test]
    fn f32_needle() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(-0.0f32),
            Some(-f32::NAN),
        ])]);
        for needle in [0.0f32, f32::NAN] {
            let out = invoke(
                vec![
                    ColumnarValue::Array(Arc::new(list.clone())),
                    ColumnarValue::Array(Arc::new(Float32Array::from(vec![needle]))),
                ],
                1,
            )?;
            assert!(out.value(0));
        }
        Ok(())
    }

    #[test]
    fn scalar_list_haystack_variants() -> Result<()> {
        let list = f64_list(vec![Some(-0.0)]);
        let large =
            LargeListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
                Some(-0.0f64),
            ])]);
        let fixed = FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
            vec![Some(vec![Some(-0.0f64)])],
            1,
        );
        for haystack in [
            ScalarValue::List(Arc::new(list)),
            ScalarValue::LargeList(Arc::new(large)),
            ScalarValue::FixedSizeList(Arc::new(fixed)),
        ] {
            let out = invoke(
                vec![
                    ColumnarValue::Scalar(haystack),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                ],
                1,
            )?;
            assert!(out.value(0));
        }
        Ok(())
    }

    fn struct_of(v: f64) -> StructArray {
        StructArray::new(
            Fields::from(vec![Field::new("v", DataType::Float64, true)]),
            vec![Arc::new(Float64Array::from(vec![v])) as ArrayRef],
            None,
        )
    }

    #[test]
    fn struct_fields_nan_payload() -> Result<()> {
        let haystack = ListArray::new(
            Arc::new(Field::new("item", struct_of(0.0).data_type().clone(), true)),
            arrow::buffer::OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(struct_of(neg_nan())),
            None,
        );
        let array_needle = invoke(
            vec![
                ColumnarValue::Array(Arc::new(haystack.clone())),
                ColumnarValue::Array(Arc::new(struct_of(f64::NAN))),
            ],
            1,
        )?;
        assert!(array_needle.value(0));
        let scalar_needle = invoke(
            vec![
                ColumnarValue::Array(Arc::new(haystack)),
                ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(struct_of(f64::NAN)))),
            ],
            1,
        )?;
        assert!(scalar_needle.value(0));
        Ok(())
    }

    #[test]
    fn non_float_values_are_unchanged() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
        ])]);
        let hit = invoke(
            vec![
                ColumnarValue::Array(Arc::new(list.clone())),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![2]))),
            ],
            1,
        )?;
        assert!(hit.value(0));
        let miss = invoke(
            vec![
                ColumnarValue::Array(Arc::new(list)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
            ],
            1,
        )?;
        assert!(!miss.value(0));
        Ok(())
    }
}
