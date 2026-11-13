// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Replay flat, sorted DISTINCT COUNT spill rows with constant state per group.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::async_try_stream;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::StreamExt;

use crate::SendableRecordBatchStream;
use crate::aggregates::group_values::encode_array_if_necessary;
use crate::stream::RecordBatchStreamAdapter;

pub(super) fn replay(
    mut input: SendableRecordBatchStream,
    schema: SchemaRef,
    batch_size: usize,
    buffer_bytes: usize,
    reservation: MemoryReservation,
) -> Result<SendableRecordBatchStream> {
    let num_keys = schema.fields().len() - 1;
    let converter = RowConverter::new(
        schema.fields()[..num_keys]
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let output_schema = Arc::clone(&schema);
    let stream = async_try_stream(move |mut emitter| async move {
        let mut current: Option<OwnedRow> = None;
        let mut previous_value: Option<ScalarValue> = None;
        let mut count = 0_i64;
        let mut keys = Vec::new();
        let mut counts = Vec::new();
        let mut output_bytes = 0;
        while let Some(batch) = input.next().await.transpose()? {
            let rows = converter.convert_columns(&batch.columns()[..num_keys])?;
            let batch_bytes =
                batch.get_array_memory_size() + rows.size() + converter.size();
            for row in 0..batch.num_rows() {
                let key = rows.row(row);
                if current.as_ref().is_none_or(|current| current.row() != key) {
                    if let Some(completed) = current.take() {
                        output_bytes += completed.row().as_ref().len();
                        keys.push(completed);
                        counts.push(count);
                    }
                    current = Some(key.owned());
                    previous_value = None;
                    count = 0;
                }
                let value = ScalarValue::try_from_array(batch.column(num_keys), row)?;
                if !value.is_null() && previous_value.as_ref() != Some(&value) {
                    count += 1;
                    previous_value = Some(value);
                }
                let current_bytes =
                    current.as_ref().map_or(0, |key| key.row().as_ref().len());
                reservation.try_resize(
                    batch_bytes
                        + output_bytes
                        + current_bytes
                        + keys.capacity() * size_of::<OwnedRow>()
                        + counts.capacity() * size_of::<i64>(),
                )?;
                if keys.len() >= batch_size || output_bytes >= buffer_bytes / 4 {
                    let output =
                        output_batch(&converter, &output_schema, &mut keys, &mut counts)?;
                    output_bytes = 0;
                    reservation.try_resize(
                        batch_bytes
                            + current_bytes
                            + output.get_array_memory_size()
                            + keys.capacity() * size_of::<OwnedRow>(),
                    )?;
                    emitter.emit(output).await;
                }
            }
        }
        if let Some(key) = current {
            keys.push(key);
            counts.push(count);
        }
        if !keys.is_empty() {
            let output =
                output_batch(&converter, &output_schema, &mut keys, &mut counts)?;
            reservation.free();
            emitter.emit(output).await;
        }
        Ok(())
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

fn output_batch(
    converter: &RowConverter,
    schema: &SchemaRef,
    keys: &mut Vec<OwnedRow>,
    counts: &mut Vec<i64>,
) -> Result<RecordBatch> {
    let mut columns = converter.convert_rows(keys.iter().map(OwnedRow::row))?;
    for (column, field) in columns.iter_mut().zip(schema.fields()) {
        *column = encode_array_if_necessary(column, field.data_type())?;
    }
    keys.clear();
    columns.push(Arc::new(Int64Array::from(std::mem::take(counts))) as ArrayRef);
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::common::collect;
    use crate::test::TestMemoryExec;
    use arrow::array::{Array, AsArray, BooleanArray, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{
        FairSpillPool, GreedyMemoryPool, MemoryPool, PeakRecordingPool,
    };
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("tag", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
            Field::new("keep", DataType::Boolean, false),
        ]))
    }

    fn aggregate(batches: Vec<RecordBatch>) -> Result<AggregateExec> {
        let schema = batches[0].schema();
        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![
                (col("k", &schema)?, "k".into()),
                (col("tag", &schema)?, "tag".into()),
            ]),
            vec![Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("n")
                    .distinct()
                    .build()?,
            )],
            vec![Some(col("keep", &schema)?)],
            input,
            schema,
        )
    }

    async fn run(
        batches: Vec<RecordBatch>,
        limit: usize,
        fair: bool,
    ) -> Result<Vec<RecordBatch>> {
        let aggregate = aggregate(batches)?;
        let inner: Arc<dyn MemoryPool> = if fair {
            Arc::new(FairSpillPool::new(limit))
        } else {
            Arc::new(GreedyMemoryPool::new(limit))
        };
        let pool = Arc::new(PeakRecordingPool::new(inner));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new().with_batch_size(1024).set_bool(
                        "datafusion.execution.enable_migration_aggregate",
                        true,
                    ),
                )
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                        .build_arc()?,
                ),
        );
        let result = collect(aggregate.execute(0, Arc::clone(&context))?).await;
        assert!(
            pool.peak_reserved() <= limit,
            "peak {} exceeds limit {limit}",
            pool.peak_reserved()
        );
        assert_eq!(pool.reserved(), 0);
        let progress = context.runtime_env().disk_manager.spilling_progress();
        assert_eq!(progress.current_bytes, 0);
        assert_eq!(progress.active_files_count, 0);
        if result.is_ok() {
            assert!(aggregate.metrics().unwrap().spill_count().unwrap() > 1);
        }
        result
    }

    #[tokio::test]
    async fn distinct_spill_concurrent_union_shares_memory() -> Result<()> {
        const PARTITIONS: usize = 16;
        const LIMIT: usize = 1024 * 1024;
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 0, 1, 0, 1, 0, 1, 0])),
                Arc::new(StringArray::from(vec!["group"; 8])),
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 1, 1, 2, 2])),
                Arc::new(BooleanArray::from(vec![true; 8])),
            ],
        )?;
        for fair in [false, true] {
            let inner: Arc<dyn MemoryPool> = if fair {
                Arc::new(FairSpillPool::new(LIMIT))
            } else {
                Arc::new(GreedyMemoryPool::new(LIMIT))
            };
            let pool = Arc::new(PeakRecordingPool::new(inner));
            let context = Arc::new(
                TaskContext::default().with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                        .build_arc()?,
                ),
            );
            let inputs = (0..PARTITIONS)
                .map(|_| {
                    Ok(Arc::new(aggregate(vec![batch.clone()])?)
                        as Arc<dyn ExecutionPlan>)
                })
                .collect::<Result<Vec<_>>>()?;
            let union = crate::union::UnionExec::try_new(inputs)?;
            // Keep every stream alive before polling any of them. UNION and
            // coalescing can initialize all its children under the same pool.
            let streams = (0..PARTITIONS)
                .map(|partition| union.execute(partition, Arc::clone(&context)))
                .collect::<Result<Vec<_>>>()?;
            let outputs =
                futures::future::try_join_all(streams.into_iter().map(collect)).await?;
            for batches in outputs {
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
                for batch in batches {
                    assert!(
                        batch
                            .column(2)
                            .as_primitive::<Int64Type>()
                            .values()
                            .iter()
                            .all(|&count| count == 2)
                    );
                }
            }
            assert!(pool.peak_reserved() <= LIMIT);
            assert_eq!(pool.reserved(), 0);
            assert_eq!(
                context
                    .runtime_env()
                    .disk_manager
                    .spilling_progress()
                    .active_files_count,
                0
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_concurrent_wide_keys_share_workspace() -> Result<()> {
        const PARTITIONS: usize = 4;
        const DISTINCT: i64 = 12_000;
        const LIMIT: usize = 2 * 1024 * 1024;
        let tag = "wide key ".repeat(32);
        let mut batches = Vec::new();
        for start in (0..DISTINCT).step_by(256) {
            let end = (start + 256).min(DISTINCT);
            let len = (end - start) as usize;
            batches.push(RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (start..end).map(|value| value % 2),
                    )),
                    Arc::new(StringArray::from(vec![tag.as_str(); len])),
                    Arc::new(Int64Array::from_iter_values(start..end)),
                    Arc::new(BooleanArray::from(vec![true; len])),
                ],
            )?);
        }
        let pool = Arc::new(PeakRecordingPool::new(Arc::new(FairSpillPool::new(LIMIT))));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(1024))
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                        .build_arc()?,
                ),
        );
        let inputs = (0..PARTITIONS)
            .map(|_| Ok(Arc::new(aggregate(batches.clone())?) as Arc<dyn ExecutionPlan>))
            .collect::<Result<Vec<_>>>()?;
        let union = crate::union::UnionExec::try_new(inputs.clone())?;
        let streams = (0..PARTITIONS)
            .map(|partition| union.execute(partition, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        let outputs =
            futures::future::try_join_all(streams.into_iter().map(collect)).await?;
        for batches in outputs {
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            for batch in batches {
                assert!(
                    batch
                        .column(2)
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .all(|&count| count == DISTINCT / 2)
                );
            }
        }
        for input in inputs {
            assert!(input.metrics().unwrap().spill_count().unwrap() > 0);
        }
        assert!(pool.peak_reserved() <= LIMIT);
        assert_eq!(pool.reserved(), 0);
        let progress = context.runtime_env().disk_manager.spilling_progress();
        assert_eq!(progress.current_bytes, 0);
        assert_eq!(progress.active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_counts_a_group_larger_than_memory() -> Result<()> {
        const DISTINCT: i64 = 50_000;
        let mut batches = Vec::new();
        for round in 0..3 {
            for start in (0..DISTINCT).step_by(512) {
                let end = (start + 512).min(DISTINCT);
                let len = (end - start) as usize;
                let values = (start..end).map(|value| {
                    Some(if round == 1 {
                        DISTINCT - 1 - value
                    } else {
                        value
                    })
                });
                batches.push(RecordBatch::try_new(
                    schema(),
                    vec![
                        Arc::new(Int64Array::from_iter(
                            std::iter::repeat_n(Some(0), len).chain([
                                None,
                                Some(1),
                                Some(2),
                            ]),
                        )),
                        Arc::new(StringArray::from_iter(
                            std::iter::repeat_n(None, len).chain([
                                Some("null key"),
                                Some("null values"),
                                Some("filtered"),
                            ]),
                        )),
                        Arc::new(Int64Array::from_iter(values.chain([
                            Some(7),
                            None,
                            Some(start),
                        ]))),
                        Arc::new(BooleanArray::from_iter(
                            std::iter::repeat_n(Some(true), len + 2).chain([Some(false)]),
                        )),
                    ],
                )?);
            }
        }
        for fair in [false, true] {
            let output = run(batches.clone(), 1024 * 1024, fair).await?;
            let mut actual = std::collections::BTreeMap::new();
            for batch in output {
                let keys = batch.column(0).as_primitive::<Int64Type>();
                let counts = batch.column(2).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    assert!(
                        actual
                            .insert(
                                keys.is_valid(row).then(|| keys.value(row)),
                                counts.value(row)
                            )
                            .is_none()
                    );
                }
            }
            assert_eq!(
                actual,
                [(None, 1), (Some(0), DISTINCT), (Some(1), 0), (Some(2), 0)]
                    .into_iter()
                    .collect()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_integer_extremes_and_null_filters() -> Result<()> {
        const DISTINCT: i64 = 50_000;
        for data_type in [DataType::Int64, DataType::UInt64] {
            let mut fields = schema().fields().to_vec();
            fields[2] = Arc::new(Field::new("v", data_type.clone(), true));
            fields[3] = Arc::new(Field::new("keep", DataType::Boolean, true));
            let schema = Arc::new(Schema::new(fields));
            let mut batches = Vec::new();
            for round in 0..3 {
                for start in (0..DISTINCT).step_by(512) {
                    let end = (start + 512).min(DISTINCT);
                    let len = (end - start) as usize;
                    let values = (start..end).map(|value| {
                        if round == 1 {
                            DISTINCT - 1 - value
                        } else {
                            value
                        }
                    });
                    let values: ArrayRef = match data_type {
                        DataType::Int64 => {
                            Arc::new(Int64Array::from_iter(values.map(Some).chain([
                                Some(i64::MIN),
                                Some(-1),
                                Some(i64::MAX),
                                None,
                                Some(42),
                                Some(6),
                                Some(7),
                            ])))
                        }
                        DataType::UInt64 => Arc::new(UInt64Array::from_iter(
                            values.map(|value| Some(value as u64)).chain([
                                Some(1_u64 << 63),
                                Some((1_u64 << 63) + 1),
                                Some(u64::MAX),
                                None,
                                Some(42),
                                Some(6),
                                Some(7),
                            ]),
                        )),
                        _ => unreachable!(),
                    };
                    batches.push(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from_iter_values(
                                std::iter::repeat_n(0, len).chain([0, 0, 0, 1, 2, 3, 3]),
                            )),
                            Arc::new(StringArray::from(vec!["typed"; len + 7])),
                            values,
                            Arc::new(BooleanArray::from_iter(
                                std::iter::repeat_n(Some(true), len + 4).chain([
                                    None,
                                    None,
                                    Some(true),
                                ]),
                            )),
                        ],
                    )?);
                }
            }
            let output = run(batches, 1024 * 1024, false).await?;
            let mut actual = std::collections::BTreeMap::new();
            for batch in output {
                let keys = batch.column(0).as_primitive::<Int64Type>();
                let counts = batch.column(2).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    assert!(actual.insert(keys.value(row), counts.value(row)).is_none());
                }
            }
            assert_eq!(
                actual,
                [(0, DISTINCT + 3), (1, 0), (2, 0), (3, 1)]
                    .into_iter()
                    .collect(),
                "{data_type:?}",
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_keeps_many_empty_groups() -> Result<()> {
        let mut batches = Vec::new();
        for start in (0_i64..20_000).step_by(256) {
            let end = (start + 256).min(20_000);
            batches.push(RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values((start..end).rev())),
                    Arc::new(StringArray::from_iter_values(
                        (start..end).rev().map(|key| format!("key-{key}")),
                    )),
                    Arc::new(Int64Array::from_iter(
                        (start..end).map(|key| (key % 2 == 0).then_some(key)),
                    )),
                    Arc::new(BooleanArray::from_iter(
                        (start..end).map(|key| Some(key % 2 != 0)),
                    )),
                ],
            )?);
        }
        let output = run(batches, 1024 * 1024, false).await?;
        let mut groups = std::collections::BTreeSet::new();
        for batch in output {
            let keys = batch.column(0).as_primitive::<Int64Type>();
            let counts = batch.column(2).as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                assert!(groups.insert(keys.value(row)));
                assert_eq!(counts.value(row), 0);
            }
        }
        assert_eq!(groups.len(), 20_000);
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_rejects_a_key_larger_than_memory() -> Result<()> {
        let tag = "x".repeat(1024 * 1024);
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(StringArray::from(vec![tag.as_str()])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )?;
        for fair in [false, true] {
            let error = run(vec![batch.clone()], 1024 * 1024, fair)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    datafusion_common::DataFusionError::ResourcesExhausted(_)
                ),
                "{error}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_spill_preserves_dictionary_keys() -> Result<()> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut fields = schema().fields().to_vec();
        fields[1] = Arc::new(Field::new("tag", dictionary_type.clone(), true));
        let schema = Arc::new(Schema::new(fields));
        let mut batches = Vec::new();
        for start in (0_i64..50_000).step_by(512) {
            let end = (start + 512).min(50_000);
            let len = (end - start) as usize;
            let tags = StringArray::from(vec!["dictionary key"; len]);
            batches.push(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![0; len])),
                    arrow::compute::cast(&tags, &dictionary_type)?,
                    Arc::new(Int64Array::from_iter_values(start..end)),
                    Arc::new(BooleanArray::from(vec![true; len])),
                ],
            )?);
        }
        let output = run(batches, 1024 * 1024, false).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(output[0].column(1).data_type(), &dictionary_type);
        assert_eq!(
            output[0].column(2).as_primitive::<Int64Type>().value(0),
            50_000
        );
        Ok(())
    }
}
