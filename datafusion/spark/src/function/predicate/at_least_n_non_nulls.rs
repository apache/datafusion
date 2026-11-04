// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type, Schema};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ExpressionPlacement};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Spark's internal `AtLeastNNonNulls` predicate, used by `DataFrame.na.drop`.
///
/// Counts values that are neither NULL nor NaN, stopping evaluation of a row's
/// children once `n` is reached. Non-positive thresholds return true without
/// evaluating children. An unreachable positive threshold still evaluates every
/// child. Empty batches evaluate no children.
///
/// Complex values use their outer validity: empty collections and non-null
/// structs with null fields count as valid values. Only Float32 and Float64 NaNs
/// are excluded.
///
/// This predicate has no Spark SQL function name. Consumers construct it directly
/// from already-planned, Spark-compatible children; it does not perform coercion.
/// Callers must account for shared subexpressions hoisted out of this expression,
/// which may require evaluation even on rows that have reached the threshold.
///
/// Children reporting Column or Literal placement are read for the entire batch.
/// This fast path assumes those children only read existing values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkAtLeastNNonNullsExpr {
    n: i32,
    children: Vec<Arc<dyn PhysicalExpr>>,
}

impl SparkAtLeastNNonNullsExpr {
    /// Construct a predicate with a fixed threshold and ordered children.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array};
    /// use arrow::record_batch::RecordBatch;
    /// use datafusion::physical_expr::expressions::Column;
    /// use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    /// use datafusion_spark::function::predicate::at_least_n_non_nulls::SparkAtLeastNNonNullsExpr;
    ///
    /// let batch = RecordBatch::try_from_iter([
    ///     ("a", Arc::new(Float64Array::from(vec![Some(f64::NAN), None, Some(1.0)])) as ArrayRef),
    ///     ("b", Arc::new(Int32Array::from(vec![Some(2), None, Some(3)])) as ArrayRef),
    /// ])?;
    /// let expr = SparkAtLeastNNonNullsExpr::new(2, vec![
    ///     Arc::new(Column::new("a", 0)),
    ///     Arc::new(Column::new("b", 1)),
    /// ]);
    /// let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
    /// assert_eq!(result.as_ref(), &BooleanArray::from(vec![false, false, true]));
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn new(n: i32, children: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { n, children }
    }
}

impl Display for SparkAtLeastNNonNullsExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "atleastnnonnulls({}", self.n)?;
        for child in &self.children {
            write!(f, ", {child}")?;
        }
        write!(f, ")")
    }
}

impl PhysicalExpr for SparkAtLeastNNonNullsExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "atleastnnonnulls({}", self.n)?;
        for child in &self.children {
            write!(f, ", ")?;
            child.fmt_sql(f)?;
        }
        write!(f, ")")
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "atleastnnonnulls",
            DataType::Boolean,
            false,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.n <= 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))));
        }
        // The standard na.drop("all"/"any") cases reduce to bitmap OR/AND and do not
        // need a per-row counter. For AND, still evaluate every child: Spark does not
        // skip later expressions just because a row can no longer reach the threshold.
        if self.n == 1 || self.n as usize == self.children.len() {
            let all = self.n != 1;
            let mut result = if all {
                BooleanBuffer::new_set(batch.num_rows())
            } else {
                BooleanBuffer::new_unset(batch.num_rows())
            };
            for child in &self.children {
                if batch.num_rows() == 0
                    || (!all && result.count_set_bits() == batch.num_rows())
                {
                    break;
                }
                let value = if all
                    || matches!(
                        child.placement(),
                        ExpressionPlacement::Column | ExpressionPlacement::Literal
                    ) {
                    child.evaluate(batch)?
                } else {
                    child.evaluate_selection(batch, &BooleanArray::new(!&result, None))?
                };
                let valid = valid_values(&value.into_array(batch.num_rows())?)?;
                result = if all {
                    &result & &valid
                } else {
                    &result | &valid
                };
            }
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
                result, None,
            ))));
        }
        // One bit plane per binary digit holds 64 row counts in each chunk.
        // Increment these counters with a half-adder, saturating at n by masking
        // out rows that have already reached it.
        let bits = self.n.ilog2() as usize + 1;
        let chunks = batch.num_rows().div_ceil(64);
        let mut counts = vec![0_u64; chunks * bits];
        let mut result = vec![0_u64; chunks];
        let mut remaining = batch.num_rows();
        for child in &self.children {
            if remaining == 0 {
                break;
            }
            let value = if remaining == batch.num_rows()
                || matches!(
                    child.placement(),
                    ExpressionPlacement::Column | ExpressionPlacement::Literal
                ) {
                child.evaluate(batch)?
            } else {
                let selection = BooleanArray::new(
                    BooleanBuffer::collect_bool(batch.num_rows(), |row| {
                        result[row / 64] & (1 << (row % 64)) == 0
                    }),
                    None,
                );
                child.evaluate_selection(batch, &selection)?
            };
            let scalar = matches!(value, ColumnarValue::Scalar(_));
            let array = value.into_array(if scalar { 1 } else { batch.num_rows() })?;
            let valid = valid_values(&array)?;
            let valid = if scalar {
                if !valid.value(0) {
                    continue;
                }
                BooleanBuffer::new_set(batch.num_rows())
            } else {
                valid
            };
            for ((counts, result), valid) in counts
                .chunks_exact_mut(bits)
                .zip(&mut result)
                .zip(valid.bit_chunks().iter_padded())
            {
                let mut carry = valid & !*result;
                if carry == 0 {
                    continue;
                }
                for count in counts.iter_mut() {
                    let next = *count & carry;
                    *count ^= carry;
                    carry = next;
                    if carry == 0 {
                        break;
                    }
                }
                // Counts never exceed n, so intersecting its set bit planes
                // identifies equality without comparing the zero bit planes.
                let mut threshold = (self.n as u32) ^ (1 << (bits - 1));
                let mut reached = counts[bits - 1];
                while threshold != 0 && reached != 0 {
                    reached &= counts[threshold.trailing_zeros() as usize];
                    threshold &= threshold - 1;
                }
                remaining -= (reached & !*result).count_ones() as usize;
                *result |= reached;
            }
        }
        // Arrow stores bitmap bytes in little-endian order on every platform.
        let result = result.into_iter().map(u64::to_le).collect::<Vec<_>>();
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            BooleanBuffer::new(Buffer::from_vec(result), 0, batch.num_rows()),
            None,
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(self.n, children)))
    }
}

fn valid_values(array: &ArrayRef) -> Result<BooleanBuffer> {
    // Dictionary validity also includes null dictionary values. Floating-point dictionaries
    // need decoding to inspect NaNs; other types need only their logical validity bitmap.
    let array = match array.data_type() {
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), DataType::Float32 | DataType::Float64) =>
        {
            cast(array, value_type)?
        }
        _ => Arc::clone(array),
    };
    let valid = array.logical_nulls().map_or_else(
        || BooleanBuffer::new_set(array.len()),
        |n| n.inner().clone(),
    );
    // Computing NaN bits independently of nulls lets Arrow scan the values without
    // a per-element validity branch. The final AND removes values beneath nulls.
    Ok(match array.data_type() {
        DataType::Float32 => {
            let not_nan =
                BooleanArray::from_unary(array.as_primitive::<Float32Type>(), |v| {
                    !v.is_nan()
                });
            &valid & not_nan.values()
        }
        DataType::Float64 => {
            let not_nan =
                BooleanArray::from_unary(array.as_primitive::<Float64Type>(), |v| {
                    !v.is_nan()
                });
            &valid & not_nan.values()
        }
        _ => valid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Float32Array, Float64Array, Int32Array, ListArray, NullArray,
        StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::CastOptions;
    use arrow::datatypes::Int32Type;
    use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    fn batch(arrays: Vec<ArrayRef>) -> RecordBatch {
        let schema = Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn columns(batch: &RecordBatch) -> Vec<Arc<dyn PhysicalExpr>> {
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| Arc::new(Column::new(f.name(), i)) as _)
            .collect()
    }

    fn result(expr: &dyn PhysicalExpr, batch: &RecordBatch) -> Vec<bool> {
        let array = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(array.null_count(), 0);
        array.as_boolean().values().iter().collect()
    }

    #[test]
    fn at_least_n_non_nulls_survives_rewrite() {
        let input = batch(vec![Arc::new(Int32Array::from(vec![Some(1), None]))]);
        let expr = Arc::new(SparkAtLeastNNonNullsExpr::new(2, vec![]));
        let rewritten = expr.with_new_children(columns(&input)).unwrap();
        assert_eq!(result(rewritten.as_ref(), &input), [false, false]);
        assert_eq!(
            fmt_sql(rewritten.as_ref()).to_string(),
            "atleastnnonnulls(2, c0)"
        );
        assert_eq!(rewritten.to_string(), "atleastnnonnulls(2, c0@0)");
    }

    #[test]
    fn at_least_n_non_nulls_mixed_types() {
        let batch = batch(vec![
            Arc::new(StringArray::from(vec![
                Some(""),
                None,
                Some("NaN"),
                None,
                None,
                None,
            ])),
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(-0.0),
                None,
                None,
                // Native arrays preserve NaN signs and payloads that Parquet canonicalizes.
                Some(f32::from_bits(0xffc0_0001)),
                Some(-0.0),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
                Some(-0.0),
                Some(f64::from_bits(0xfff8_0000_0000_0001)),
            ])),
            Arc::new(NullArray::new(6)),
            Arc::new(StructArray::new(
                vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
                vec![Arc::new(Int32Array::new_null(6))],
                Some(NullBuffer::from(vec![true, true, false, false, true, true])),
            )),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![]),
                None,
                Some(vec![None]),
                None,
                Some(vec![]),
                Some(vec![Some(1)]),
            ])),
        ]);
        for n in [-1, 0, 1, 2, 3, 4, 5, 6, 7, i32::MAX] {
            let expr = SparkAtLeastNNonNullsExpr::new(n, columns(&batch));
            assert_eq!(
                result(&expr, &batch),
                [4, 2, 3, 0, 3, 3].map(|count| count >= n)
            );
            assert!(!expr.nullable(batch.schema_ref()).unwrap());
        }
        let sliced = batch.slice(1, 2);
        assert_eq!(
            result(
                &SparkAtLeastNNonNullsExpr::new(3, columns(&sliced)),
                &sliced
            ),
            [false, true]
        );
        assert!(
            result(
                &SparkAtLeastNNonNullsExpr::new(1, columns(&batch)),
                &batch.slice(0, 0)
            )
            .is_empty()
        );
    }

    #[test]
    fn at_least_n_non_nulls_sliced_bitmaps() {
        let batch = batch(
            (0..5)
                .map(|column| {
                    Arc::new(Float64Array::from_iter((0..133).map(|row| {
                        match (row + column) % 7 {
                            0 => None,
                            1 => Some(f64::NAN),
                            _ => Some(row as f64),
                        }
                    }))) as ArrayRef
                })
                .collect(),
        )
        .slice(3, 129);
        for n in 0..=6 {
            let expected = (3..132)
                .map(|row| (0..5).filter(|column| (row + column) % 7 > 1).count() >= n)
                .collect::<Vec<_>>();
            assert_eq!(
                result(
                    &SparkAtLeastNNonNullsExpr::new(n as i32, columns(&batch)),
                    &batch
                ),
                expected
            );
        }
    }

    #[test]
    fn at_least_n_non_nulls_dictionary_values_and_scalars() {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(Float64Array::from(vec![Some(3.0), Some(f64::NAN), None])),
        )
        .unwrap();
        let strings = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(StringArray::from(vec![Some(""), Some("NaN"), None])),
        )
        .unwrap();
        let batch = batch(vec![Arc::new(dict), Arc::new(strings)]);
        let mut children = columns(&batch);
        children.push(Arc::new(Literal::new(ScalarValue::Utf8(Some("".into())))));
        children.push(Arc::new(Literal::new(ScalarValue::Float32(Some(f32::NAN)))));
        assert_eq!(
            result(&SparkAtLeastNNonNullsExpr::new(3, children.clone()), &batch),
            [true, false, false, false]
        );
        assert_eq!(
            result(
                &SparkAtLeastNNonNullsExpr::new(1, vec![Arc::new(Column::new("c1", 1))]),
                &batch
            ),
            [true, true, false, false]
        );
        assert_eq!(
            result(&SparkAtLeastNNonNullsExpr::new(1, vec![]), &batch),
            [false; 4]
        );
    }

    #[test]
    fn at_least_n_non_nulls_short_circuits_per_row() {
        let batch = batch(vec![
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["bad", "7"])),
        ]);
        let children: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("c0", 0)),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("c1", 1)),
                DataType::Int32,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )),
        ];
        assert_eq!(
            result(&SparkAtLeastNNonNullsExpr::new(1, children.clone()), &batch),
            [true, true]
        );
        // Also exercise the general counter path, not only the n=1 bitmap path.
        let mut general = children.clone();
        general.insert(0, Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))));
        assert_eq!(
            result(&SparkAtLeastNNonNullsExpr::new(2, general), &batch),
            [true, true]
        );
        // Both non-positive thresholds and an empty batch must avoid evaluating the bad cast.
        assert_eq!(
            result(&SparkAtLeastNNonNullsExpr::new(0, children.clone()), &batch),
            [true, true]
        );
        assert!(
            result(
                &SparkAtLeastNNonNullsExpr::new(2, children.clone()),
                &batch.slice(0, 0)
            )
            .is_empty()
        );
        // An impossible threshold is not permission to skip evaluations Spark would perform.
        let mut required = children;
        required[0] = Arc::new(Literal::new(ScalarValue::Int32(None)));
        for n in [2, 3] {
            assert!(
                SparkAtLeastNNonNullsExpr::new(n, required.clone())
                    .evaluate(&batch)
                    .is_err()
            );
        }
    }

    #[test]
    fn at_least_n_non_nulls_counter_bit_boundaries() {
        // Every possible count up to 65 occurs on both sides of word boundaries.
        // Rotating the valid children also exercises different carry orders.
        let input = batch(
            (0..65)
                .map(|column| {
                    Arc::new(Float64Array::from_iter((0..200).map(|row| {
                        if (column + row) % 65 < row % 66 {
                            Some(row as f64)
                        } else if column % 2 == 0 {
                            Some(f64::NAN)
                        } else {
                            None
                        }
                    }))) as ArrayRef
                })
                .collect(),
        );
        for offset in [0, 1, 7, 63] {
            for len in [0, 1, 31, 32, 63, 64, 65, 127, 128, 129] {
                let sliced = input.slice(offset, len);
                for n in (0..=66).chain([i32::MAX as usize]) {
                    let expected = (offset..offset + len)
                        .map(|row| row % 66 >= n)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        result(
                            &SparkAtLeastNNonNullsExpr::new(n as i32, columns(&sliced)),
                            &sliced
                        ),
                        expected,
                        "offset={offset}, len={len}, n={n}"
                    );
                }
            }
        }
    }

    #[test]
    fn at_least_n_non_nulls_bitmap_dictionary_and_scalars() {
        let keys = Int32Array::from_iter(
            (0..137).map(|row| if row % 4 == 3 { None } else { Some(row % 4) }),
        );
        let floats = DictionaryArray::<Int32Type>::try_new(
            keys.clone(),
            Arc::new(Float64Array::from(vec![Some(3.0), Some(f64::NAN), None])),
        )
        .unwrap();
        let strings = DictionaryArray::<Int32Type>::try_new(
            keys,
            Arc::new(StringArray::from(vec![Some(""), Some("NaN"), None])),
        )
        .unwrap();
        let input = batch(vec![Arc::new(floats), Arc::new(strings)]).slice(3, 129);
        let mut children = columns(&input);
        children.push(Arc::new(Literal::new(ScalarValue::Utf8(Some("".into())))));
        children.push(Arc::new(Literal::new(ScalarValue::Float32(Some(f32::NAN)))));
        for n in 1..=5 {
            let expected = (3..132)
                .map(|row| [3, 2, 1, 1][row % 4] >= n)
                .collect::<Vec<_>>();
            assert_eq!(
                result(&SparkAtLeastNNonNullsExpr::new(n, children.clone()), &input),
                expected
            );
        }
    }

    #[test]
    fn at_least_n_non_nulls_mixed_completion_before_cast() {
        let input = batch(vec![
            Arc::new(Int32Array::from_iter(
                (0..137).map(|row| if row % 3 == 0 { None } else { Some(1) }),
            )),
            Arc::new(StringArray::from_iter(
                (0..137).map(|row| if row % 3 == 0 { Some("7") } else { Some("bad") }),
            )),
        ]);
        let children: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("c0", 0)),
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("c1", 1)),
                DataType::Int32,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )),
            Arc::new(Literal::new(ScalarValue::Int32(None))),
        ];
        for len in [1, 63, 64, 65, 129] {
            let sliced = input.slice(7, len);
            let expr = |n| SparkAtLeastNNonNullsExpr::new(n, children.clone());
            assert_eq!(result(&expr(2), &sliced), vec![true; len]);
            // These rows still need the cast when n=3, so its error is required.
            assert!(expr(3).evaluate(&sliced).is_err());
        }
    }
}
