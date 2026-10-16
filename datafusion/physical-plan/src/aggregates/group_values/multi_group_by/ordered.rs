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

use std::mem;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use arrow_ord::partition::partition;
use datafusion_common::{Result, not_impl_err};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupSelection};

use super::{GroupColumn, GroupValuesColumn};
use crate::aggregates::group_values::GroupValues;

/// Columnar group keys for input fully ordered by all grouping expressions.
///
/// Equal keys must be contiguous across input batches. Within a batch Arrow's
/// partition kernel finds those runs. Only the first run can continue the last
/// buffered group; compare it with the stored columns, without retaining the
/// input batch. New group representatives use the existing column builders.
///
/// This removes hashing and hash-table storage, but does not change the dense
/// group-id or emission contracts. In particular, `First(n)` shifts the remaining
/// keys and their ids together. Partially ordered inputs must use a hash table.
pub(crate) struct GroupValuesOrdered {
    schema: SchemaRef,
    columns: Vec<Box<dyn GroupColumn>>,
    new_groups: Vec<usize>,
}

impl GroupValuesOrdered {
    /// Types for which adjacent Arrow equality agrees with GROUP BY equality.
    /// Keep single-column specializations and floats/nested/encoded keys on the
    /// established path until their semantics and performance are validated.
    pub(crate) fn supports_schema(schema: &Schema) -> bool {
        schema.fields().len() > 1
            && schema.fields().iter().all(|field| {
                matches!(
                    field.data_type(),
                    DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Timestamp(_, _)
                        | DataType::Date32
                        | DataType::Date64
                        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
                        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
                        | DataType::Duration(_)
                        | DataType::Interval(_)
                        | DataType::Decimal32(_, _)
                        | DataType::Decimal64(_, _)
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                        | DataType::FixedSizeBinary(0..)
                )
            })
    }

    pub(crate) fn try_new(schema: SchemaRef) -> Result<Self> {
        if !Self::supports_schema(&schema) {
            return not_impl_err!(
                "Unsupported schema for fully ordered group values: {schema}"
            );
        }
        let columns = GroupValuesColumn::<true>::build_group_columns(&schema)?;
        Ok(Self {
            schema,
            columns,
            new_groups: Vec::new(),
        })
    }
}

impl GroupValues for GroupValuesOrdered {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        groups.clear();
        let ranges = partition(cols)?.ranges();
        if ranges.is_empty() {
            return Ok(());
        }

        let old_len = self.len();
        let continues = old_len > 0
            && self
                .columns
                .iter()
                .zip(cols)
                .all(|(stored, input)| stored.equal_to(old_len - 1, input, 0));

        self.new_groups.clear();
        self.new_groups.extend(
            ranges
                .iter()
                .skip(usize::from(continues))
                .map(|range| range.start),
        );
        for (stored, input) in self.columns.iter_mut().zip(cols) {
            stored.vectorized_append(input, &self.new_groups)?;
        }

        let first_group = old_len - usize::from(continues);
        for (index, range) in ranges.iter().enumerate() {
            groups.resize(range.end, first_group + index);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.columns
            .iter()
            .map(|column| column.size())
            .sum::<usize>()
            + self.new_groups.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.columns[0].len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(match emit_to {
            EmitTo::All => {
                let fresh = GroupValuesColumn::<true>::build_group_columns(&self.schema)?;
                mem::replace(&mut self.columns, fresh)
                    .into_iter()
                    .map(|column| column.build())
                    .collect()
            }
            EmitTo::First(n) => self
                .columns
                .iter_mut()
                .map(|column| column.take_n(n))
                .collect(),
        })
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.len())?;
        self.columns
            .iter()
            .map(|column| column.values_preserving(selection))
            .collect()
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.columns = GroupValuesColumn::<true>::build_group_columns(&self.schema)
            .expect("schema validated by GroupValuesOrdered::try_new");
        self.new_groups.clear();
        self.new_groups.shrink_to(num_rows);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt32Array;
    use arrow::array::{
        Array, BooleanArray, Decimal32Array, Decimal64Array, Decimal128Array,
        Decimal256Array, FixedSizeBinaryArray, Int32Array, Int64Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
        StringArray,
    };
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::compute::{cast, take};
    use arrow::datatypes::{
        Field, IntervalDayTime, IntervalMonthDayNano, IntervalUnit, i256,
    };
    use datafusion_expr::GroupSelection;

    use super::*;

    // The values repeat in seven-row runs, with four keys per value of the
    // first column. Construct logical types directly where casting would
    // rescale decimals or cannot represent all interval components.
    fn ordered_key_array(
        key_type: &DataType,
        rows: usize,
        nullable: bool,
    ) -> Result<ArrayRef> {
        let keys = (0..rows)
            .map(|row| {
                let key = ((row / 7) % 4) as i32;
                (!nullable || key != 0).then_some(key)
            })
            .collect::<Vec<_>>();
        Ok(match key_type {
            DataType::Boolean => Arc::new(BooleanArray::from_iter(
                keys.iter().map(|key| key.map(|key| key >= 2)),
            )),
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => {
                let strings = StringArray::from_iter(keys.iter().map(|key| {
                    key.map(|key| {
                        format!(
                            "key-{key}-{}",
                            "x".repeat(if key == 3 { 128 } else { 0 })
                        )
                    })
                }));
                cast(&strings, key_type)?
            }
            DataType::Decimal32(precision, scale) => Arc::new(
                Decimal32Array::from_iter(keys.iter().map(|key| key.map(|key| key - 2)))
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            DataType::Decimal64(precision, scale) => Arc::new(
                Decimal64Array::from_iter(
                    keys.iter().map(|key| key.map(|key| i64::from(key - 2))),
                )
                .with_precision_and_scale(*precision, *scale)?,
            ),
            DataType::Decimal128(precision, scale) => {
                // Equal low 64 bits must not hide differences in the high bits.
                let values = [
                    -(1_i128 << 100),
                    0,
                    1_i128 << 100,
                    (1_i128 << 100) + (1_i128 << 64),
                ];
                let array = Decimal128Array::from_iter(
                    keys.iter().map(|key| key.map(|key| values[key as usize])),
                );
                array.validate_decimal_precision(*precision)?;
                Arc::new(array.with_precision_and_scale(*precision, *scale)?)
            }
            DataType::Decimal256(precision, scale) => {
                // These values share their low 128 bits but exceed i128's range.
                let values = [
                    i256::from_parts(0, -(1_i128 << 100)),
                    i256::from_i128(0),
                    i256::from_parts(0, 1_i128 << 100),
                    i256::from_parts(0, (1_i128 << 100) + 1),
                ];
                let array = Decimal256Array::from_iter(
                    keys.iter().map(|key| key.map(|key| values[key as usize])),
                );
                array.validate_decimal_precision(*precision)?;
                Arc::new(array.with_precision_and_scale(*precision, *scale)?)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let values = [-13, 0, 1, 13];
                Arc::new(IntervalYearMonthArray::from_iter(
                    keys.iter().map(|key| key.map(|key| values[key as usize])),
                ))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let values = [
                    IntervalDayTime::new(-1, 0),
                    IntervalDayTime::new(0, 0),
                    IntervalDayTime::new(0, 1),
                    IntervalDayTime::new(1, 1),
                ];
                Arc::new(IntervalDayTimeArray::from_iter(
                    keys.iter().map(|key| key.map(|key| values[key as usize])),
                ))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let values = [
                    IntervalMonthDayNano::new(0, 0, 0),
                    IntervalMonthDayNano::new(0, 0, 1),
                    IntervalMonthDayNano::new(0, 1, 1),
                    IntervalMonthDayNano::new(1, 1, 1),
                ];
                Arc::new(IntervalMonthDayNanoArray::from_iter(
                    keys.iter().map(|key| key.map(|key| values[key as usize])),
                ))
            }
            DataType::FixedSizeBinary(width) => {
                let bytes = keys
                    .iter()
                    .flat_map(|key| {
                        std::iter::repeat_n(
                            key.unwrap_or_default() as u8,
                            *width as usize,
                        )
                    })
                    .collect::<Vec<_>>();
                let nulls = nullable.then(|| {
                    NullBuffer::from(keys.iter().map(Option::is_some).collect::<Vec<_>>())
                });
                Arc::new(FixedSizeBinaryArray::try_new_with_len(
                    *width,
                    Buffer::from(bytes),
                    nulls,
                    rows,
                )?)
            }
            DataType::Date64 => {
                let values = Int64Array::from_iter(
                    keys.iter()
                        .map(|key| key.map(|key| i64::from(key) * 86_400_000)),
                );
                cast(&values, key_type)?
            }
            DataType::Timestamp(_, _) | DataType::Time64(_) | DataType::Duration(_) => {
                let values =
                    Int64Array::from_iter(keys.iter().map(|key| key.map(i64::from)));
                cast(&values, key_type)?
            }
            _ => cast(&Int32Array::from(keys), key_type)?,
        })
    }

    // Compare with the existing hash implementation, changing actual input
    // boundaries and removing completed groups between batches. This checks the
    // semantic contract rather than duplicating the adjacent-run algorithm.
    #[test]
    fn ordered_keys_match_hash_grouping_across_batches_and_emits() -> Result<()> {
        let mut key_types = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Decimal32(9, 2),
            DataType::Decimal32(9, -2),
            DataType::Decimal64(18, 2),
            DataType::Decimal64(18, -2),
            DataType::Decimal128(38, 2),
            DataType::Decimal128(38, -2),
            DataType::Decimal256(76, 2),
            DataType::Decimal256(76, -2),
            DataType::FixedSizeBinary(3),
            DataType::FixedSizeBinary(16),
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            key_types.push(DataType::Duration(unit));
            for timezone in [None, Some("UTC"), Some("Europe/Paris"), Some("+05:30")] {
                key_types.push(DataType::Timestamp(unit, timezone.map(Into::into)));
            }
        }
        for key_type in key_types {
            for nullable in [false, true] {
                let rows = 1025;
                let first: ArrayRef =
                    Arc::new(Int32Array::from_iter((0..rows).map(|row| {
                        let group = (row / 7) as i32;
                        (!nullable || group >= 4).then_some(group / 4)
                    })));
                let second = ordered_key_array(&key_type, rows, nullable)?;
                for descending in [false, true] {
                    let mut input = vec![Arc::clone(&first), Arc::clone(&second)];
                    if descending {
                        let reverse =
                            UInt32Array::from_iter_values((0..rows as u32).rev());
                        input = input
                            .iter()
                            .map(|array| take(array, &reverse, None))
                            .collect::<std::result::Result<_, _>>()?;
                    }
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("a", DataType::Int32, nullable),
                        Field::new("b", key_type.clone(), nullable),
                    ]));
                    assert!(GroupValuesOrdered::supports_schema(&schema), "{key_type}");
                    for batch_size in [1, 2, 7, 8, 63, 1024] {
                        for emit_limit in [0, 1, 17, usize::MAX] {
                            let mut ordered =
                                GroupValuesOrdered::try_new(Arc::clone(&schema))?;
                            let mut hashed =
                                GroupValuesColumn::<true>::try_new(Arc::clone(&schema))?;
                            let mut actual = Vec::new();
                            let mut expected = Vec::new();
                            for offset in (0..rows).step_by(batch_size) {
                                let length = batch_size.min(rows - offset);
                                let batch = input
                                    .iter()
                                    .map(|array| array.slice(offset, length))
                                    .collect::<Vec<_>>();
                                ordered.intern(&batch, &mut actual)?;
                                hashed.intern(&batch, &mut expected)?;
                                assert_eq!(
                                    actual, expected,
                                    "{key_type:?}, nullable={nullable}, batch={batch_size}, offset={offset}, descending={descending}"
                                );
                                assert_eq!(ordered.len(), hashed.len());
                                let selection = GroupSelection::all(ordered.len());
                                let values = ordered.values_preserving(selection)?;
                                assert_eq!(values, hashed.values_preserving(selection)?);
                                assert_eq!(values[1].data_type(), &key_type);
                                // A zero-row batch must neither forget the boundary
                                // key nor leave stale group ids in the output buffer.
                                let empty = batch
                                    .iter()
                                    .map(|array| array.slice(0, 0))
                                    .collect::<Vec<_>>();
                                ordered.intern(&empty, &mut actual)?;
                                assert!(actual.is_empty());
                                let emit =
                                    emit_limit.min(ordered.len().saturating_sub(1));
                                let values = ordered.emit(EmitTo::First(emit))?;
                                assert_eq!(values, hashed.emit(EmitTo::First(emit))?);
                                assert_eq!(values[1].data_type(), &key_type);
                            }
                            let indices = [ordered.len() - 1, 0, ordered.len() - 1];
                            let selection = GroupSelection::try_from_indices(
                                &indices,
                                ordered.len(),
                            )?;
                            assert_eq!(
                                ordered.values_preserving(selection)?,
                                hashed.values_preserving(selection)?
                            );
                            let values = ordered.emit(EmitTo::All)?;
                            assert_eq!(values, hashed.emit(EmitTo::All)?);
                            assert_eq!(values[1].data_type(), &key_type);
                            assert!(ordered.is_empty());
                            // All resets the ordered boundary state. The hash reference
                            // also needs its lookup table cleared after draining keys.
                            hashed.clear_shrink(0);
                            ordered.intern(&input, &mut actual)?;
                            hashed.intern(&input, &mut expected)?;
                            assert_eq!(actual, expected);
                            // Clearing populated storage must reset it as well.
                            ordered.clear_shrink(0);
                            hashed.clear_shrink(0);
                            ordered.intern(&input, &mut actual)?;
                            hashed.intern(&input, &mut expected)?;
                            assert_eq!(actual, expected);
                            let values = ordered.emit(EmitTo::All)?;
                            assert_eq!(values, hashed.emit(EmitTo::All)?);
                            assert_eq!(values[1].data_type(), &key_type);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn zero_width_binary_preserves_emitted_lengths_and_nulls() -> Result<()> {
        for nullable in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::FixedSizeBinary(0), nullable),
            ]));
            let first: ArrayRef = Arc::new(Int32Array::from(vec![7; 6]));
            let second: ArrayRef = Arc::new(FixedSizeBinaryArray::try_new_with_len(
                0,
                Buffer::default(),
                nullable.then(|| {
                    NullBuffer::from(vec![true, false, false, true, true, true])
                }),
                6,
            )?);
            let input = [first.slice(1, 4), second.slice(1, 4)];
            let mut ordered = GroupValuesOrdered::try_new(schema)?;
            let mut groups = Vec::new();
            for offset in [0, 2] {
                let batch = input
                    .iter()
                    .map(|array| array.slice(offset, 2))
                    .collect::<Vec<_>>();
                ordered.intern(&batch, &mut groups)?;
                assert_eq!(groups, vec![usize::from(nullable && offset == 2); 2]);
            }
            let values = ordered.values_preserving(GroupSelection::all(ordered.len()))?;
            assert_eq!(values[1].len(), if nullable { 2 } else { 1 });
            assert_eq!(values[1].null_count(), usize::from(nullable));
            let values = ordered.emit(EmitTo::First(1))?;
            assert_eq!(values[1].len(), 1);
            assert_eq!(values[1].null_count(), usize::from(nullable));
            let values = ordered.emit(EmitTo::All)?;
            assert_eq!(values[1].len(), usize::from(nullable));
            assert_eq!(values[1].null_count(), 0);
            assert_eq!(values[1].data_type(), &DataType::FixedSizeBinary(0));
        }
        Ok(())
    }

    #[test]
    fn ordered_schema_gate_keeps_unvalidated_types_on_existing_paths() {
        for data_type in [
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Null,
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::FixedSizeBinary(-1),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", data_type, true),
            ]);
            assert!(!GroupValuesOrdered::supports_schema(&schema));
            assert!(GroupValuesOrdered::try_new(Arc::new(schema)).is_err());
        }
        assert!(!GroupValuesOrdered::supports_schema(&Schema::new(vec![
            Field::new("a", DataType::Int32, false)
        ])));
    }

    #[tokio::test]
    async fn ordered_single_and_partial_final_match_unordered_execution() -> Result<()> {
        use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
        use crate::test::TestMemoryExec;
        use crate::{ExecutionPlan, InputOrderMode, collect};
        use arrow::compute::{SortOptions, take_record_batch};
        use arrow::record_batch::RecordBatch;
        use arrow::row::{RowConverter, SortField};
        use datafusion_execution::TaskContext;
        use datafusion_functions_aggregate::sum::sum_udaf;
        use datafusion_physical_expr::aggregate::AggregateExprBuilder;
        use datafusion_physical_expr::expressions::col;
        use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
            Field::new("include", DataType::Boolean, false),
        ]));
        let source = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter(
                    (0..257).map(|row| (row >= 28).then_some(row / 28)),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..257).map(|row| format!("key-{}", (row / 7) % 4)),
                )),
                Arc::new(Int64Array::from_iter_values(0..257)),
                Arc::new(BooleanArray::from_iter(
                    (0..257).map(|row| Some(row % 3 != 0)),
                )),
            ],
        )?;
        let aggregate = Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("sum_v")
                .build()?,
        );

        for descending in [false, true] {
            let source = if descending {
                let reverse = UInt32Array::from_iter_values((0..257).rev());
                take_record_batch(&source, &reverse)?
            } else {
                source.clone()
            };
            for input_batch_size in [1, 7, 31, 128] {
                let batches = (0..257)
                    .step_by(input_batch_size)
                    .map(|offset| {
                        source.slice(offset, input_batch_size.min(257 - offset))
                    })
                    .collect::<Vec<_>>();
                for two_stage in [false, true] {
                    let mut results = Vec::new();
                    for sorted in [false, true] {
                        let input = TestMemoryExec::try_new_exec(
                            std::slice::from_ref(&batches),
                            Arc::clone(&schema),
                            None,
                        )?;
                        let input: Arc<dyn ExecutionPlan> = if sorted {
                            let options = if descending {
                                SortOptions::default().desc().nulls_last()
                            } else {
                                SortOptions::default()
                            };
                            let ordering = LexOrdering::new(vec![
                                PhysicalSortExpr::new(col("a", &schema)?, options),
                                PhysicalSortExpr::new(col("b", &schema)?, options),
                            ])
                            .unwrap();
                            Arc::new(
                                input
                                    .as_ref()
                                    .clone()
                                    .try_with_sort_information(vec![ordering])?,
                            )
                        } else {
                            input
                        };
                        let group_by = PhysicalGroupBy::new_single(vec![
                            (col("a", &schema)?, "a".into()),
                            (col("b", &schema)?, "b".into()),
                        ]);
                        let plan = AggregateExec::try_new(
                            if two_stage {
                                AggregateMode::Partial
                            } else {
                                AggregateMode::Single
                            },
                            group_by.clone(),
                            vec![Arc::clone(&aggregate)],
                            vec![Some(col("include", &schema)?)],
                            input,
                            Arc::clone(&schema),
                        )?;
                        if sorted {
                            assert_eq!(plan.input_order_mode(), &InputOrderMode::Sorted);
                        }
                        let plan: Arc<dyn ExecutionPlan> = if two_stage {
                            let plan = AggregateExec::try_new(
                                AggregateMode::Final,
                                group_by,
                                vec![Arc::clone(&aggregate)],
                                vec![None],
                                Arc::new(plan),
                                Arc::clone(&schema),
                            )?;
                            if sorted {
                                assert_eq!(
                                    plan.input_order_mode(),
                                    &InputOrderMode::Sorted
                                );
                            }
                            Arc::new(plan)
                        } else {
                            Arc::new(plan)
                        };
                        let converter = RowConverter::new(
                            plan.schema()
                                .fields()
                                .iter()
                                .map(|field| SortField::new(field.data_type().clone()))
                                .collect(),
                        )?;
                        let output =
                            collect(plan, Arc::new(TaskContext::default())).await?;
                        let mut rows = Vec::new();
                        for batch in output {
                            rows.extend(
                                converter
                                    .convert_columns(batch.columns())?
                                    .iter()
                                    .map(|row| row.as_ref().to_vec()),
                            );
                        }
                        rows.sort();
                        results.push(rows);
                    }
                    assert_eq!(
                        results[0], results[1],
                        "batch_size={input_batch_size}, two_stage={two_stage}, descending={descending}"
                    );
                }
            }
        }
        Ok(())
    }
}
