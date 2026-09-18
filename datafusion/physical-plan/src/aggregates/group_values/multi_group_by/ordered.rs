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
use arrow::datatypes::{DataType, Schema, SchemaRef};
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
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
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
    use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
    use arrow::compute::{cast, take};
    use arrow::datatypes::Field;
    use datafusion_expr::GroupSelection;

    use super::*;

    // Compare with the existing hash implementation, changing actual input
    // boundaries and removing completed groups between batches. This checks the
    // semantic contract rather than duplicating the adjacent-run algorithm.
    #[test]
    fn ordered_keys_match_hash_grouping_across_batches_and_emits() -> Result<()> {
        for key_type in [
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
        ] {
            let rows = 1025;
            let first: ArrayRef = Arc::new(Int32Array::from_iter((0..rows).map(|row| {
                let group = row / 7;
                (group >= 4).then_some(group / 4)
            })));
            let second: ArrayRef = match key_type {
                DataType::Boolean => {
                    Arc::new(BooleanArray::from_iter((0..rows).map(|row| {
                        let key = (row / 7) % 4;
                        (key != 0).then_some(key >= 2)
                    })))
                }
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView => {
                    let strings = StringArray::from_iter((0..rows).map(|row| {
                        let key = (row / 7) % 4;
                        (key != 0).then(|| {
                            format!(
                                "key-{key}-{}",
                                "x".repeat(if key == 3 { 128 } else { 0 })
                            )
                        })
                    }));
                    cast(&strings, &key_type)?
                }
                _ => {
                    let values = Int32Array::from_iter((0..rows).map(|row| {
                        let key = (row / 7) % 4;
                        (key != 0).then_some(key)
                    }));
                    cast(&values, &key_type)?
                }
            };
            for descending in [false, true] {
                let mut input = vec![Arc::clone(&first), Arc::clone(&second)];
                if descending {
                    let reverse = UInt32Array::from_iter_values((0..rows as u32).rev());
                    input = input
                        .iter()
                        .map(|array| take(array, &reverse, None))
                        .collect::<std::result::Result<_, _>>()?;
                }
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", key_type.clone(), true),
                ]));
                for batch_size in [1, 2, 7, 8, 63, 1024] {
                    for emit_limit in [0, 1, 17, usize::MAX] {
                        let mut ordered =
                            GroupValuesOrdered::try_new(Arc::clone(&schema))?;
                        let mut hashed =
                            GroupValuesColumn::<true>::try_new(Arc::clone(&schema))?;
                        let mut actual = Vec::new();
                        let mut expected = Vec::new();
                        for offset in (0..rows as usize).step_by(batch_size) {
                            let length = batch_size.min(rows as usize - offset);
                            let batch = input
                                .iter()
                                .map(|array| array.slice(offset, length))
                                .collect::<Vec<_>>();
                            ordered.intern(&batch, &mut actual)?;
                            hashed.intern(&batch, &mut expected)?;
                            assert_eq!(
                                actual, expected,
                                "{key_type:?}, batch={batch_size}, offset={offset}, descending={descending}"
                            );
                            assert_eq!(ordered.len(), hashed.len());
                            let selection = GroupSelection::all(ordered.len());
                            assert_eq!(
                                ordered.values_preserving(selection)?,
                                hashed.values_preserving(selection)?
                            );
                            // A zero-row batch must neither forget the boundary
                            // key nor leave stale group ids in the output buffer.
                            let empty = batch
                                .iter()
                                .map(|array| array.slice(0, 0))
                                .collect::<Vec<_>>();
                            ordered.intern(&empty, &mut actual)?;
                            assert!(actual.is_empty());
                            let emit = emit_limit.min(ordered.len().saturating_sub(1));
                            assert_eq!(
                                ordered.emit(EmitTo::First(emit))?,
                                hashed.emit(EmitTo::First(emit))?
                            );
                        }
                        assert_eq!(ordered.emit(EmitTo::All)?, hashed.emit(EmitTo::All)?);
                        assert!(ordered.is_empty());
                        // All and clear_shrink reset the cross-batch state.
                        ordered.clear_shrink(0);
                        hashed.clear_shrink(0);
                        ordered.intern(&input, &mut actual)?;
                        hashed.intern(&input, &mut expected)?;
                        assert_eq!(actual, expected);
                        assert_eq!(ordered.emit(EmitTo::All)?, hashed.emit(EmitTo::All)?);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn ordered_schema_gate_keeps_unvalidated_types_on_existing_paths() {
        for data_type in [
            DataType::Float32,
            DataType::Float64,
            DataType::Null,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", data_type, true),
            ]);
            assert!(!GroupValuesOrdered::supports_schema(&schema));
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

        for input_batch_size in [1, 7, 31, 128] {
            let batches = (0..257)
                .step_by(input_batch_size)
                .map(|offset| source.slice(offset, input_batch_size.min(257 - offset)))
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
                        let ordering = LexOrdering::new(vec![
                            PhysicalSortExpr::new_default(col("a", &schema)?),
                            PhysicalSortExpr::new_default(col("b", &schema)?),
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
                            assert_eq!(plan.input_order_mode(), &InputOrderMode::Sorted);
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
                    let output = collect(plan, Arc::new(TaskContext::default())).await?;
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
                    "batch_size={input_batch_size}, two_stage={two_stage}"
                );
            }
        }
        Ok(())
    }
}
