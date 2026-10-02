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

//! Public-plan regressions for large, multi-batch hash-join builds.

use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListBuilder, Int8Array, Int32Array,
    Int32Builder, ListArray, ListBuilder, MapBuilder, RecordBatch, StringArray,
    StringBuilder, StructArray, new_null_array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{JoinSide, JoinType, NullEquality, Result, ScalarValue};
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{TaskContext, config::SessionConfig};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, IsNotNullExpr, cast as physical_cast, col, lit,
};

use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::joins::{HashJoinExec, HashJoinExecBuilder, PartitionMode};
use crate::test::TestMemoryExec;
use crate::{ExecutionPlan, common};

const COMPACT_BUILD_BYTES: usize = 64 * 1024 * 1024;
const PADDING_BYTES: usize = 65 * 1024 * 1024;

fn string_with_backing(rows: usize, values: Buffer) -> ArrayRef {
    let mut offsets = vec![values.len() as i32; rows + 1];
    offsets[0] = 0;
    Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        None,
    ))
}

/// Two batches exceed the compact-build threshold while sharing one allocation.
fn padding(rows: usize) -> ArrayRef {
    static VALUES: OnceLock<Buffer> = OnceLock::new();
    let values = VALUES.get_or_init(|| Buffer::from_vec(vec![b'p'; PADDING_BYTES]));
    string_with_backing(rows, values.clone())
}

fn assert_large_build(batches: &[RecordBatch]) {
    assert!(batches.len() > 1);
    assert!(batches.iter().all(|batch| batch.num_rows() > 0));
    assert!(
        batches
            .iter()
            .map(get_record_batch_memory_size)
            .sum::<usize>()
            > COMPACT_BUILD_BYTES
    );
}

fn padded_build(keys: Vec<ArrayRef>) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("build_key", keys[0].data_type().clone(), true),
        Field::new("padding", DataType::Utf8, false),
    ]));
    let batches = keys
        .into_iter()
        .map(|key| {
            let rows = key.len();
            RecordBatch::try_new(Arc::clone(&schema), vec![key, padding(rows)])
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    assert_large_build(&batches);
    Ok(batches)
}

fn with_payload(
    batches: Vec<RecordBatch>,
    payload: ArrayRef,
) -> Result<Vec<RecordBatch>> {
    let mut fields = batches[0].schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "payload",
        payload.data_type().clone(),
        true,
    )));
    let schema = Arc::new(Schema::new(fields));
    let mut offset = 0;
    let output = batches
        .into_iter()
        .map(|batch| {
            let mut columns = batch.columns().to_vec();
            columns.push(payload.slice(offset, batch.num_rows()));
            offset += batch.num_rows();
            RecordBatch::try_new(Arc::clone(&schema), columns).map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(offset, payload.len());
    Ok(output)
}

fn nested_payload(ids: &[i32]) -> ArrayRef {
    let mut lists = ListBuilder::new(StringBuilder::new());
    for id in ids {
        if id % 3 != 0 {
            lists.values().append_value(format!("response{id}"));
            lists.values().append_null();
        }
        lists.append(id % 3 != 0);
    }
    let lists: ArrayRef = Arc::new(lists.finish());
    Arc::new(StructArray::new(
        vec![Field::new("labels", lists.data_type().clone(), true)].into(),
        vec![lists],
        Some(NullBuffer::from(
            ids.iter().map(|id| id % 3 != 1).collect::<Vec<_>>(),
        )),
    ))
}

fn scalar_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<ScalarValue>>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array.as_ref(), row))
                    .collect()
            })
        })
        .collect()
}

fn probe(values: ArrayRef) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "probe_key",
        values.data_type().clone(),
        true,
    )]));
    let batches = if values.is_empty() {
        vec![]
    } else {
        vec![RecordBatch::try_new(Arc::clone(&schema), vec![values])?]
    };
    Ok((schema, batches))
}

fn join_builder(
    build: &[RecordBatch],
    probe: (SchemaRef, Vec<RecordBatch>),
    join_type: JoinType,
    projection: Vec<usize>,
) -> Result<HashJoinExecBuilder> {
    let (probe_schema, probe_batches) = probe;
    let left = TestMemoryExec::try_new_exec(&[build.to_vec()], build[0].schema(), None)?;
    let right = TestMemoryExec::try_new_exec(&[probe_batches], probe_schema, None)?;
    join_builder_from_plans(left, right, join_type, projection)
}

fn join_builder_from_plans(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    projection: Vec<usize>,
) -> Result<HashJoinExecBuilder> {
    let on = vec![(
        col("build_key", &left.schema())?,
        col("probe_key", &right.schema())?,
    )];
    Ok(HashJoinExecBuilder::new(left, right, on, join_type)
        .with_projection(Some(projection))
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_null_equality(NullEquality::NullEqualsNothing))
}

fn task_context(batch_size: usize, use_perfect_hash: bool) -> Arc<TaskContext> {
    let mut config = SessionConfig::default().with_batch_size(batch_size);
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = false;
    config
        .options_mut()
        .execution
        .perfect_hash_join_small_build_threshold =
        if use_perfect_hash { usize::MAX } else { 0 };
    config
        .options_mut()
        .execution
        .perfect_hash_join_min_key_density =
        if use_perfect_hash { 0.0 } else { f64::INFINITY };
    Arc::new(TaskContext::default().with_session_config(config))
}

fn assert_perfect_hash(join: &HashJoinExec, expected: bool) {
    let count = join
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("array_map_created_count"))
        .map_or(0, |metric| metric.as_usize());
    assert_eq!(count > 0, expected);
}

async fn collect_join(
    join: HashJoinExec,
    batch_size: usize,
    use_perfect_hash: bool,
) -> Result<Vec<RecordBatch>> {
    let output =
        common::collect(join.execute(0, task_context(batch_size, use_perfect_hash))?)
            .await?;
    assert_perfect_hash(&join, use_perfect_hash);
    Ok(output)
}

fn int_values(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
        })
        .collect()
}

#[track_caller]
fn assert_batch_size(batches: &[RecordBatch], batch_size: usize) {
    assert!(
        batches.iter().all(|batch| batch.num_rows() <= batch_size),
        "expected at most {batch_size} rows, got {:?}",
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
    );
}

fn string_key_rows(batches: &[RecordBatch]) -> Result<Vec<(String, String)>> {
    let mut rows = vec![];
    for batch in batches {
        let left = cast(batch.column(0), &DataType::Utf8)?;
        let right = cast(batch.column(1), &DataType::Utf8)?;
        let left = left.as_any().downcast_ref::<StringArray>().unwrap();
        let right = right.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(left.null_count(), 0);
        assert_eq!(right.null_count(), 0);
        for row in 0..batch.num_rows() {
            rows.push((left.value(row).to_owned(), right.value(row).to_owned()));
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn coerced_dictionary_and_plain_keys_join_across_batches() -> Result<()> {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "missing"]));
    let dictionary = |keys: Vec<i32>| -> Result<ArrayRef> {
        Ok(Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(keys),
            Arc::clone(&values),
        )?))
    };

    for dictionary_on_build in [true, false] {
        let (build_keys, probe_keys): (Vec<ArrayRef>, ArrayRef) = if dictionary_on_build {
            (
                vec![dictionary(vec![0])?, dictionary(vec![1])?],
                Arc::new(StringArray::from(vec!["x", "x", "y", "missing"])),
            )
        } else {
            (
                vec![
                    Arc::new(StringArray::from(vec!["x"])),
                    Arc::new(StringArray::from(vec!["y"])),
                ],
                dictionary(vec![0, 0, 1, 2])?,
            )
        };
        let build = padded_build(build_keys)?;
        let probe = probe(probe_keys)?;
        let mut left_key = col("build_key", &build[0].schema())?;
        let mut right_key = col("probe_key", &probe.0)?;
        if dictionary_on_build {
            left_key = physical_cast(left_key, &build[0].schema(), DataType::Utf8)?;
        } else {
            right_key = physical_cast(right_key, &probe.0, DataType::Utf8)?;
        }
        let join = join_builder(&build, probe, JoinType::Inner, vec![0, 2])?
            .with_on(vec![(left_key, right_key)])
            .build()?;
        let output = collect_join(join, 8192, false).await?;
        let mut actual = string_key_rows(&output)?;
        actual.sort_unstable();
        let expected = [("x", "x"), ("x", "x"), ("y", "y")]
            .map(|(left, right)| (left.to_owned(), right.to_owned()));
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[tokio::test]
async fn computed_dictionary_keys_join_across_batches() -> Result<()> {
    let dictionary_type =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let dictionary = |values: Vec<&str>| -> Result<ArrayRef> {
        Ok(Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values(0..values.len() as i32),
            Arc::new(StringArray::from(values)),
        )?))
    };

    for computed_on_build in [true, false] {
        let build_keys: Vec<ArrayRef> = if computed_on_build {
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec!["y"])),
            ]
        } else {
            vec![dictionary(vec!["x"])?, dictionary(vec!["y"])?]
        };
        let probe_keys = if computed_on_build {
            dictionary(vec!["y", "missing", "x", "y"])?
        } else {
            Arc::new(StringArray::from(vec!["y", "missing", "x", "y"])) as ArrayRef
        };
        let build = padded_build(build_keys)?;
        let build_schema = build[0].schema();
        let probe = probe(probe_keys)?;
        let mut left_key = col("build_key", &build_schema)?;
        let mut right_key = col("probe_key", &probe.0)?;
        if computed_on_build {
            left_key = physical_cast(left_key, &build_schema, dictionary_type.clone())?;
        } else {
            right_key = physical_cast(right_key, &probe.0, dictionary_type.clone())?;
        }
        let join = join_builder(&build, probe, JoinType::Inner, vec![0, 2])?
            .with_on(vec![(left_key, right_key)])
            .build()?;
        let output = collect_join(join, 8192, false).await?;
        let expected = [("y", "y"), ("x", "x"), ("y", "y")]
            .map(|(left, right)| (left.to_owned(), right.to_owned()));
        assert_eq!(string_key_rows(&output)?, expected);
    }
    Ok(())
}

/// Find the small Arrow dictionary shape from the partial-concat regression.
/// Every trial references 128 dictionary slots, so the three-way merge uses
/// the same interner capacity. Keep only values that do not collide there.
/// Using Arrow itself avoids a new ahash dependency or platform-specific hashes.
fn partial_concat_dictionaries() -> Result<(Vec<ArrayRef>, String)> {
    let mut values = Vec::<String>::new();
    for candidate in 0..4096 {
        values.push(format!("payload{candidate:06}"));
        let dictionaries = (0..3)
            .map(|_| {
                let strings = (0..128)
                    .map(|index| values.get(index).unwrap_or(&values[0]).as_str());
                Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from_iter_values(0..=i8::MAX),
                    Arc::new(StringArray::from_iter_values(strings)),
                )?) as ArrayRef)
            })
            .collect::<Result<Vec<_>>>()?;
        let arrays = dictionaries
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        let distinct = match concat(&arrays) {
            Ok(array) => Some(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int8Type>>()
                    .unwrap()
                    .values()
                    .len(),
            ),
            Err(ArrowError::DictionaryKeyOverflowError) => None,
            Err(error) => return Err(error.into()),
        };
        if distinct != Some(values.len()) {
            values.pop();
        } else if values.len() == 128 {
            if matches!(
                concat(&arrays[..2]),
                Err(ArrowError::DictionaryKeyOverflowError)
            ) {
                return Ok((dictionaries, values[0].clone()));
            }
            values.pop();
        }
    }
    panic!("could not construct the partial-concat dictionary regression");
}

#[tokio::test]
async fn dictionary_payloads_preserve_values_across_batches() -> Result<()> {
    let (payloads, expected) = partial_concat_dictionaries()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("build_key", DataType::Int32, false),
        Field::new("payload", payloads[0].data_type().clone(), false),
        Field::new("padding", DataType::Utf8, false),
    ]));
    let build = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let bytes = if index == 2 { 65 * 1024 * 1024 } else { 0 };
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (index * 128..(index + 1) * 128).map(|key| key as i32),
                    )),
                    payload,
                    string_with_backing(128, Buffer::from_vec(vec![b'p'; bytes])),
                ],
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    assert_large_build(&build);
    assert!(
        build[..2]
            .iter()
            .map(get_record_batch_memory_size)
            .sum::<usize>()
            < 8 * 1024 * 1024
    );

    // The original all-at-once concat succeeds, but coalescing the two small
    // batches first overflows Int8 dictionary keys. Both hash implementations
    // must preserve values without introducing that partial-concat failure.
    for use_perfect_hash in [false, true] {
        let probe = probe(Arc::new(Int32Array::from(vec![0, 128])))?;
        let join = join_builder(&build, probe, JoinType::Inner, vec![1])?.build()?;
        let output = collect_join(join, 8192, use_perfect_hash).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in output {
            let decoded = cast(batch.column(0), &DataType::Utf8)?;
            let decoded = decoded.as_any().downcast_ref::<StringArray>().unwrap();
            assert!(decoded.iter().all(|value| value == Some(expected.as_str())));
        }
    }
    Ok(())
}

#[tokio::test]
async fn hidden_null_utf8_payload_does_not_overflow_on_fanout() -> Result<()> {
    const MATCHES: usize = 66;
    const BATCH_SIZE: usize = 8192;

    // Repeating the hidden span for every match would overflow Utf8 offsets.
    let payload = StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(vec![0, PADDING_BYTES as i32])),
        Buffer::from_vec(vec![b'p'; PADDING_BYTES]),
        Some(NullBuffer::new_null(1)),
    );
    assert!(payload.value_length(0) as usize * MATCHES > i32::MAX as usize);
    let payload: ArrayRef = Arc::new(payload);
    let schema = Arc::new(Schema::new(vec![
        Field::new("build_key", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let build = [7, 8]
        .into_iter()
        .map(|key| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![key])), Arc::clone(&payload)],
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    assert_large_build(&build);
    let expected = (0..MATCHES)
        .map(|index| 7 + (index % 2) as i32)
        .collect::<Vec<_>>();

    for use_perfect_hash in [false, true] {
        let probe = probe(Arc::new(Int32Array::from(expected.clone())))?;
        let join =
            join_builder(&build, probe, JoinType::Inner, vec![0, 1, 2])?.build()?;
        let output = collect_join(join, BATCH_SIZE, use_perfect_hash).await?;
        assert_batch_size(&output, BATCH_SIZE);
        let mut actual = vec![];
        for batch in &output {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let payload = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let probe_keys = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(keys.null_count(), 0);
            assert_eq!(payload.null_count(), batch.num_rows());
            assert_eq!(probe_keys, keys);
            actual.extend(keys.values().iter().copied());
        }
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[tokio::test]
async fn nested_payloads_survive_join_filters_and_outer_rows() -> Result<()> {
    let mut maps = MapBuilder::new(
        None,
        StringBuilder::new(),
        ListBuilder::new(StringBuilder::new()),
    );
    for id in 0..4 {
        if id == 0 || id == 3 {
            maps.keys().append_value(format!("detector{id}"));
            if id == 0 {
                maps.values().values().append_value("email");
                maps.values().values().append_null();
            }
            maps.values().append(id == 0);
        }
        maps.append(id != 2)?;
    }
    let payload: ArrayRef = Arc::new(maps.finish());
    let build_keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 2, 3]));
    let build = with_payload(
        padded_build(vec![build_keys.slice(0, 2), build_keys.slice(2, 2)])?,
        Arc::clone(&payload),
    )?;
    let probe_keys: ArrayRef = Arc::new(Int32Array::from(vec![2, 2, 5]));
    let response = nested_payload(&[0, 1, 2]);
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("probe_key", DataType::Int32, false),
        Field::new("response", response.data_type().clone(), true),
    ]));
    let probe_batch = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![Arc::clone(&probe_keys), Arc::clone(&response)],
    )?;
    let filter_schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        payload.data_type().clone(),
        true,
    )]));
    let filter = JoinFilter::new(
        Arc::new(IsNotNullExpr::new(col("payload", &filter_schema)?)),
        vec![ColumnIndex {
            index: 2,
            side: JoinSide::Left,
        }],
        filter_schema,
    );
    let scalar_at = |array: &ArrayRef, row: Option<usize>| {
        row.map_or_else(
            || ScalarValue::try_from(array.data_type()),
            |row| ScalarValue::try_from_array(array.as_ref(), row),
        )
    };
    let cases = [
        (
            JoinType::Inner,
            false,
            vec![
                (Some(1), Some(0)),
                (Some(2), Some(0)),
                (Some(1), Some(1)),
                (Some(2), Some(1)),
            ],
        ),
        (
            JoinType::Full,
            true,
            vec![
                (Some(1), Some(0)),
                (Some(1), Some(1)),
                (Some(0), None),
                (Some(2), None),
                (Some(3), None),
                (None, Some(2)),
            ],
        ),
    ];
    for (join_type, filtered, pairs) in cases {
        let expected = pairs
            .into_iter()
            .map(|(left, right)| {
                Ok(vec![
                    scalar_at(&build_keys, left)?,
                    scalar_at(&payload, left)?,
                    scalar_at(&probe_keys, right)?,
                    scalar_at(&response, right)?,
                ])
            })
            .collect::<Result<Vec<_>>>()?;
        for use_perfect_hash in [false, true] {
            let join = join_builder(
                &build,
                (Arc::clone(&probe_schema), vec![probe_batch.clone()]),
                join_type,
                vec![0, 2, 3, 4],
            )?
            .with_filter(filtered.then(|| filter.clone()))
            .build()?;
            let output = collect_join(join, 8192, use_perfect_hash).await?;
            let mut actual = scalar_rows(&output)?;
            assert_eq!(actual.len(), expected.len());
            for row in &expected {
                let index = actual.iter().position(|actual| actual == row).unwrap();
                actual.swap_remove(index);
            }
            assert!(actual.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn nested_null_payloads_do_not_copy_hidden_children_on_fanout() -> Result<()> {
    const MATCHES: usize = 66;
    let bytes = Buffer::from_vec(vec![b'p'; PADDING_BYTES]);
    assert!(bytes.len() * MATCHES > i32::MAX as usize);
    // Hide the bytes at each level: struct, list, and string.
    for null_level in 0..3 {
        let wrap = |strings: ArrayRef| -> ArrayRef {
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Utf8, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
                strings,
                (null_level == 1).then(|| NullBuffer::new_null(1)),
            ));
            Arc::new(StructArray::new(
                vec![Field::new("items", list.data_type().clone(), true)].into(),
                vec![list],
                (null_level == 0).then(|| NullBuffer::new_null(1)),
            ))
        };
        let payload = wrap(Arc::new(StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(vec![0, PADDING_BYTES as i32])),
            bytes.clone(),
            (null_level == 2).then(|| NullBuffer::new_null(1)),
        )));
        let expected_payload = ScalarValue::try_from_array(
            wrap(new_null_array(&DataType::Utf8, 1)).as_ref(),
            0,
        )?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("build_key", DataType::Int32, false),
            Field::new("payload", payload.data_type().clone(), true),
        ]));
        let build = [7, 8]
            .into_iter()
            .map(|key| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(vec![key])), Arc::clone(&payload)],
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_large_build(&build);
        let keys = (0..MATCHES)
            .map(|index| 7 + (index % 2) as i32)
            .collect::<Vec<_>>();
        let expected = keys
            .iter()
            .map(|&key| {
                vec![
                    ScalarValue::Int32(Some(key)),
                    expected_payload.clone(),
                    ScalarValue::Int32(Some(key)),
                ]
            })
            .collect::<Vec<_>>();
        for use_perfect_hash in [false, true] {
            let probe = probe(Arc::new(Int32Array::from(keys.clone())))?;
            let join =
                join_builder(&build, probe, JoinType::Inner, vec![0, 1, 2])?.build()?;
            let output = collect_join(join, 8192, use_perfect_hash).await?;
            assert_eq!(scalar_rows(&output)?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_left_anti_is_bounded_and_emitted_once_with_fetch() -> Result<()> {
    const BATCH_SIZE: usize = 4;
    const FETCH: usize = 5;
    let payload = nested_payload(&(0..34).collect::<Vec<_>>());
    let build = with_payload(
        padded_build(vec![
            Arc::new(Int32Array::from_iter_values(0..17)),
            Arc::new(Int32Array::from_iter_values(17..34)),
        ])?,
        Arc::clone(&payload),
    )?;
    let (probe_schema, first_probe) = probe(Arc::new(Int32Array::from(vec![0, 17])))?;
    let (_, second_probe) = probe(Arc::new(Int32Array::from(vec![1, 18])))?;
    let build_schema = build[0].schema();
    let left: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[build], build_schema, None)?;
    let right: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[first_probe, second_probe], probe_schema, None)?;
    let expected = (0..34)
        .filter(|value| ![0, 1, 17, 18].contains(value))
        .map(Some)
        .collect::<Vec<_>>();

    for use_perfect_hash in [false, true] {
        let mut unlimited = vec![];
        for fetch in [None, Some(FETCH)] {
            let join = join_builder_from_plans(
                Arc::clone(&left),
                Arc::clone(&right),
                JoinType::LeftAnti,
                vec![0, 2],
            )?
            .with_fetch(fetch)
            .build()?;
            let context = task_context(BATCH_SIZE, use_perfect_hash);
            let first = join.execute(0, Arc::clone(&context))?;
            let second = join.execute(1, context)?;
            let (mut output, second_output) =
                futures::try_join!(common::collect(first), common::collect(second))?;
            output.extend(second_output);
            assert_perfect_hash(&join, use_perfect_hash);
            assert_batch_size(&output, BATCH_SIZE);
            let actual = int_values(&output);
            let rows = scalar_rows(&output)?;
            for (row, key) in rows.iter().zip(&actual) {
                assert_eq!(
                    row[1],
                    ScalarValue::try_from_array(payload.as_ref(), key.unwrap() as usize)?
                );
            }
            if fetch.is_some() {
                assert_eq!(rows, unlimited[..FETCH]);
            } else {
                let mut sorted = actual;
                sorted.sort_unstable();
                assert_eq!(sorted, expected);
                unlimited = rows;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_null_aware_left_anti_semantics() -> Result<()> {
    let build = padded_build(vec![
        Arc::new(Int32Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int32Array::from(vec![Some(3), Some(4), None])),
    ])?;
    let cases = [
        (vec![], vec![None, None, Some(1), Some(2), Some(3), Some(4)]),
        (vec![Some(1)], vec![Some(2), Some(3), Some(4)]),
        (vec![Some(1), None], vec![]),
    ];

    for use_perfect_hash in [false, true] {
        for (probe_values, expected) in &cases {
            let probe = probe(Arc::new(Int32Array::from(probe_values.clone())))?;
            let join = join_builder(&build, probe, JoinType::LeftAnti, vec![0])?
                .with_null_aware(true)
                .build()?;
            let output = collect_join(join, 2, use_perfect_hash).await?;
            assert_batch_size(&output, 2);
            let mut actual = int_values(&output);
            actual.sort_unstable();
            assert_eq!(&actual, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn null_aware_left_mark_preserves_unknown_across_batches() -> Result<()> {
    let keys = [Some(1), None, Some(2), Some(3), Some(4), None];
    let build = padded_build(vec![
        Arc::new(Int32Array::from(keys[..3].to_vec())),
        Arc::new(Int32Array::from(keys[3..].to_vec())),
    ])?;
    let cases = [
        (vec![], vec![Some(false); 6]),
        (
            vec![Some(1)],
            vec![
                Some(true),
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
            ],
        ),
        (
            vec![Some(1), None],
            vec![Some(true), None, None, None, None, None],
        ),
    ];
    for use_perfect_hash in [false, true] {
        for (probe_values, marks) in &cases {
            let join = join_builder(
                &build,
                probe(Arc::new(Int32Array::from(probe_values.clone())))?,
                JoinType::LeftMark,
                vec![0, 2],
            )?
            .with_null_aware(true)
            .build()?;
            let output = collect_join(join, 2, use_perfect_hash).await?;
            assert_batch_size(&output, 2);
            let expected = keys
                .iter()
                .zip(marks)
                .map(|(&key, &mark)| {
                    vec![ScalarValue::Int32(key), ScalarValue::Boolean(mark)]
                })
                .collect::<Vec<_>>();
            assert_rows_unordered(scalar_rows(&output)?, &expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn computed_composite_keys_preserve_cross_batch_duplicates() -> Result<()> {
    let build_schema = Arc::new(Schema::new(vec![
        Field::new("build_key", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("padding", DataType::Utf8, false),
        Field::new("build_id", DataType::Int32, false),
    ]));
    let build = [
        (vec![1, 2, 2], vec!["a", "a", "b"], vec![0, 1, 2]),
        (vec![2, 3, 4], vec!["b", "a", "b"], vec![3, 4, 5]),
    ]
    .into_iter()
    .map(|(keys, labels, ids)| {
        RecordBatch::try_new(
            Arc::clone(&build_schema),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(labels)),
                padding(3),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .map_err(Into::into)
    })
    .collect::<Result<Vec<_>>>()?;
    assert_large_build(&build);
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("probe_key", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("probe_id", DataType::Int32, false),
    ]));
    let probe_batch = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![
            Arc::new(Int32Array::from(vec![12, 12, 14, 13])),
            Arc::new(StringArray::from(vec!["b", "a", "b", "missing"])),
            Arc::new(Int32Array::from(vec![10, 11, 12, 13])),
        ],
    )?;
    let on = vec![
        (
            Arc::new(BinaryExpr::new(
                col("build_key", &build_schema)?,
                Operator::Plus,
                lit(10i32),
            )) as _,
            col("probe_key", &probe_schema)?,
        ),
        (col("label", &build_schema)?, col("label", &probe_schema)?),
    ];
    for use_perfect_hash in [false, true] {
        let join = join_builder(
            &build,
            (Arc::clone(&probe_schema), vec![probe_batch.clone()]),
            JoinType::Inner,
            vec![3, 6],
        )?
        .with_on(on.clone())
        .build()?;
        let output =
            common::collect(join.execute(0, task_context(2, use_perfect_hash))?).await?;
        assert_perfect_hash(&join, false);
        assert_batch_size(&output, 2);
        assert_rows_unordered(
            scalar_rows(&output)?,
            &[(2, 10), (3, 10), (1, 11), (5, 12)]
                .map(|(left, right)| vec![left.into(), right.into()]),
        );
    }
    Ok(())
}

fn assert_rows_unordered(
    mut actual: Vec<Vec<ScalarValue>>,
    expected: &[Vec<ScalarValue>],
) {
    assert_eq!(actual.len(), expected.len(), "{actual:?} != {expected:?}");
    for row in expected {
        let index = actual
            .iter()
            .position(|actual| actual == row)
            .unwrap_or_else(|| panic!("missing {row:?} in {actual:?}"));
        actual.swap_remove(index);
    }
}

#[tokio::test]
async fn dictionary_keys_with_distinct_dictionaries_and_logical_nulls() -> Result<()> {
    let dictionary =
        |keys: Vec<Option<i8>>, values: Vec<Option<&str>>| -> Result<ArrayRef> {
            Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(keys),
                Arc::new(StringArray::from(values)),
            )?))
        };
    let build = padded_build(vec![
        dictionary(
            vec![Some(0), Some(1), None, Some(2)],
            vec![Some("red"), None, Some("blue")],
        )?,
        dictionary(
            vec![Some(1), Some(0), Some(2), Some(1)],
            vec![Some("blue"), Some("red"), None],
        )?,
    ])?;
    let probe_keys = dictionary(
        vec![Some(2), Some(1), Some(0), None],
        vec![Some("red"), None, Some("blue")],
    )?;
    for null_equality in [
        NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull,
    ] {
        let join = join_builder(
            &build,
            probe(Arc::clone(&probe_keys))?,
            JoinType::Inner,
            vec![0, 2],
        )?
        .with_null_equality(null_equality)
        .build()?;
        let output = collect_join(join, 2, false).await?;
        assert_batch_size(&output, 2);
        let mut actual = vec![];
        for batch in output {
            let left = cast(batch.column(0), &DataType::Utf8)?;
            let right = cast(batch.column(1), &DataType::Utf8)?;
            for row in 0..batch.num_rows() {
                actual.push(vec![
                    ScalarValue::try_from_array(left.as_ref(), row)?,
                    ScalarValue::try_from_array(right.as_ref(), row)?,
                ]);
            }
        }
        let mut expected = vec![vec!["red".into(), "red".into()]; 3];
        expected.extend(vec![vec!["blue".into(), "blue".into()]; 2]);
        if null_equality == NullEquality::NullEqualsNull {
            expected.extend(vec![vec![ScalarValue::Utf8(None); 2]; 6]);
        }
        assert_rows_unordered(actual, &expected);
    }
    Ok(())
}

#[tokio::test]
async fn dictionary_inlist_overflow_keeps_batchwise_join() -> Result<()> {
    let dictionary = |start: usize, end: usize| -> Result<ArrayRef> {
        Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from_iter_values((0..end - start).map(|key| key as i8)),
            Arc::new(StringArray::from_iter_values(
                (start..end).map(|value| format!("value{value:03}")),
            )),
        )?))
    };
    // Each input is representable, but a single Int8 dictionary cannot hold
    // all 130 distinct keys. Optional IN-list construction must not fail the join.
    let build = padded_build(vec![dictionary(0, 65)?, dictionary(65, 130)?])?;
    let probe_values = ["value000", "value064", "value065", "value129", "missing"];
    let probe_keys = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from_iter_values(0..probe_values.len() as i8),
        Arc::new(StringArray::from(probe_values.to_vec())),
    )?) as ArrayRef;
    let join =
        join_builder(&build, probe(probe_keys)?, JoinType::Inner, vec![0, 2])?.build()?;
    let mut config = task_context(2, false).session_config().clone();
    config
        .options_mut()
        .optimizer
        .hash_join_inlist_pushdown_max_distinct_values = 512;
    let context = Arc::new(TaskContext::default().with_session_config(config));
    let output = common::collect(join.execute(0, context)?).await?;
    assert_batch_size(&output, 2);
    assert_eq!(
        string_key_rows(&output)?,
        probe_values[..4]
            .iter()
            .map(|value| (value.to_string(), value.to_string()))
            .collect::<Vec<_>>(),
    );
    Ok(())
}

#[tokio::test]
async fn large_build_succeeds_without_room_for_contiguous_copy() -> Result<()> {
    const LIMIT: usize = 96 * 1024 * 1024;
    for shape in ["plain", "computed", "dictionary", "nested"] {
        let keys: Vec<ArrayRef> = if shape == "dictionary" {
            [vec!["one", "two"], vec!["two", "three"]]
                .into_iter()
                .map(|values| {
                    Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
                        Int8Array::from(vec![0, 1]),
                        Arc::new(StringArray::from(values)),
                    )?) as ArrayRef)
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![2, 3])),
            ]
        };
        let mut build = padded_build(keys)?;
        if shape == "nested" {
            build = with_payload(build, nested_payload(&[0, 1, 2, 3]))?;
        }
        let mut counter =
            datafusion_common::utils::memory::RecordBatchMemoryCounter::new();
        for batch in &build {
            counter.count_batch(batch);
        }
        let unique_bytes = counter.memory_usage();
        assert!(unique_bytes > COMPACT_BUILD_BYTES && unique_bytes < LIMIT);
        let concat_bytes = build
            .iter()
            .map(|batch| batch.column(1).to_data().get_slice_memory_size())
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>();
        // Both padding strings use the full backing, so concatenating them
        // really needs this destination even though their sources share memory.
        assert!(concat_bytes > LIMIT - unique_bytes);

        let probe_keys: ArrayRef = if shape == "dictionary" {
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["two"])),
            )?)
        } else {
            Arc::new(Int32Array::from(vec![2]))
        };
        let probe = probe(probe_keys)?;
        let mut builder = join_builder(&build, probe.clone(), JoinType::Inner, vec![0])?;
        if shape == "computed" {
            builder = builder.with_on(vec![(
                Arc::new(BinaryExpr::new(
                    col("build_key", &build[0].schema())?,
                    Operator::Plus,
                    lit(1i32),
                )),
                Arc::new(BinaryExpr::new(
                    col("probe_key", &probe.0)?,
                    Operator::Plus,
                    lit(1i32),
                )),
            )]);
        }
        let join = builder.build()?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(task_context(2, false).session_config().clone())
                .with_runtime(runtime),
        );
        let output = common::collect(join.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in &output {
            let keys = cast(batch.column(0), &DataType::Utf8)?;
            let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
            let expected = if shape == "dictionary" { "two" } else { "2" };
            assert!(keys.iter().all(|key| key == Some(expected)));
        }
        assert!(pool.reserved() <= LIMIT);
        drop(output);
        drop(join);
        drop(context);
        assert_eq!(pool.reserved(), 0, "{shape}");
    }
    Ok(())
}

#[tokio::test]
async fn all_join_types_preserve_rows_across_build_batches() -> Result<()> {
    let build = with_payload(
        padded_build(vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 4])),
        ])?,
        Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
    )?;
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("probe_key", DataType::Int32, false),
        Field::new("probe_id", DataType::Int32, false),
    ]));
    let probe_batch = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 2])),
            Arc::new(Int32Array::from(vec![10, 11, 12])),
        ],
    )?;
    let pair = |left: Option<i32>, right: Option<i32>| {
        vec![ScalarValue::Int32(left), ScalarValue::Int32(right)]
    };
    let matches = vec![
        pair(Some(1), Some(10)),
        pair(Some(2), Some(10)),
        pair(Some(1), Some(12)),
        pair(Some(2), Some(12)),
    ];
    let mut left_outer = matches.clone();
    left_outer.extend([pair(Some(0), None), pair(Some(3), None)]);
    let mut right_outer = matches.clone();
    right_outer.push(pair(None, Some(11)));
    let mut full = left_outer.clone();
    full.push(pair(None, Some(11)));
    let cases = [
        (JoinType::Inner, vec![2, 4], matches),
        (JoinType::Left, vec![2, 4], left_outer),
        (JoinType::Right, vec![2, 4], right_outer),
        (JoinType::Full, vec![2, 4], full),
        (
            JoinType::LeftSemi,
            vec![2],
            vec![vec![1.into()], vec![2.into()]],
        ),
        (
            JoinType::LeftAnti,
            vec![2],
            vec![vec![0.into()], vec![3.into()]],
        ),
        (
            JoinType::RightSemi,
            vec![1],
            vec![vec![10.into()], vec![12.into()]],
        ),
        (JoinType::RightAnti, vec![1], vec![vec![11.into()]]),
        (
            JoinType::LeftMark,
            vec![2, 3],
            vec![
                vec![0.into(), false.into()],
                vec![1.into(), true.into()],
                vec![2.into(), true.into()],
                vec![3.into(), false.into()],
            ],
        ),
        (
            JoinType::RightMark,
            vec![1, 2],
            vec![
                vec![10.into(), true.into()],
                vec![11.into(), false.into()],
                vec![12.into(), true.into()],
            ],
        ),
    ];
    for use_perfect_hash in [false, true] {
        for (join_type, projection, expected) in &cases {
            let join = join_builder(
                &build,
                (Arc::clone(&probe_schema), vec![probe_batch.clone()]),
                *join_type,
                projection.clone(),
            )?
            .build()?;
            let output = collect_join(join, 2, use_perfect_hash).await?;
            // Probe-preserving outer alignment adds the unmatched key 3 to
            // the lookup's two matches. The coalescer does not split that batch.
            let max_batch_rows = if matches!(join_type, JoinType::Right | JoinType::Full)
            {
                3
            } else {
                2
            };
            assert!(
                output
                    .iter()
                    .all(|batch| batch.num_rows() <= max_batch_rows),
                "{join_type:?}, perfect hash {use_perfect_hash}: {:?}",
                output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            );
            assert_rows_unordered(scalar_rows(&output)?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn residual_membership_joins_evaluate_both_build_batches() -> Result<()> {
    let build = with_payload(
        padded_build(vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 4])),
        ])?,
        Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
    )?;
    let filter_schema = Arc::new(Schema::new(vec![Field::new(
        "build_id",
        DataType::Int32,
        false,
    )]));
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            col("build_id", &filter_schema)?,
            Operator::GtEq,
            lit(2i32),
        )),
        vec![ColumnIndex {
            index: 2,
            side: JoinSide::Left,
        }],
        filter_schema,
    );
    let cases = [
        (JoinType::LeftSemi, vec![2], vec![vec![2.into()]]),
        (
            JoinType::LeftAnti,
            vec![2],
            vec![vec![0.into()], vec![1.into()], vec![3.into()]],
        ),
        (
            JoinType::RightSemi,
            vec![0],
            vec![vec![2.into()], vec![2.into()]],
        ),
        (JoinType::RightAnti, vec![0], vec![vec![3.into()]]),
        (
            JoinType::LeftMark,
            vec![2, 3],
            vec![
                vec![0.into(), false.into()],
                vec![1.into(), false.into()],
                vec![2.into(), true.into()],
                vec![3.into(), false.into()],
            ],
        ),
        (
            JoinType::RightMark,
            vec![0, 1],
            vec![
                vec![2.into(), true.into()],
                vec![3.into(), false.into()],
                vec![2.into(), true.into()],
            ],
        ),
    ];
    for use_perfect_hash in [false, true] {
        for (join_type, projection, expected) in &cases {
            let join = join_builder(
                &build,
                probe(Arc::new(Int32Array::from(vec![2, 3, 2])))?,
                *join_type,
                projection.clone(),
            )?
            .with_filter(Some(filter.clone()))
            .build()?;
            let output = collect_join(join, 2, use_perfect_hash).await?;
            assert_batch_size(&output, 2);
            assert_rows_unordered(scalar_rows(&output)?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn all_null_build_keys_keep_preserved_rows() -> Result<()> {
    let build = with_payload(
        padded_build(vec![
            Arc::new(Int32Array::from(vec![None, None])),
            Arc::new(Int32Array::from(vec![None])),
        ])?,
        Arc::new(Int32Array::from(vec![10, 11, 12])),
    )?;
    let cases = [
        (JoinType::Inner, vec![2], vec![]),
        (
            JoinType::Full,
            vec![2, 3],
            vec![
                vec![10.into(), ScalarValue::Int32(None)],
                vec![11.into(), ScalarValue::Int32(None)],
                vec![12.into(), ScalarValue::Int32(None)],
                vec![ScalarValue::Int32(None), 1.into()],
                vec![ScalarValue::Int32(None), ScalarValue::Int32(None)],
            ],
        ),
        (
            JoinType::LeftAnti,
            vec![2],
            vec![vec![10.into()], vec![11.into()], vec![12.into()]],
        ),
        (
            JoinType::LeftMark,
            vec![2, 3],
            vec![
                vec![10.into(), false.into()],
                vec![11.into(), false.into()],
                vec![12.into(), false.into()],
            ],
        ),
        (
            JoinType::RightMark,
            vec![0, 1],
            vec![
                vec![1.into(), false.into()],
                vec![ScalarValue::Int32(None), false.into()],
            ],
        ),
    ];
    for (join_type, projection, expected) in cases {
        let join = join_builder(
            &build,
            probe(Arc::new(Int32Array::from(vec![Some(1), None])))?,
            join_type,
            projection,
        )?
        .build()?;
        let output = collect_join(join, 2, false).await?;
        assert_batch_size(&output, 2);
        assert_rows_unordered(scalar_rows(&output)?, &expected);
    }
    Ok(())
}

#[tokio::test]
async fn fixed_size_list_payloads_preserve_child_and_parent_nulls() -> Result<()> {
    let mut values = FixedSizeListBuilder::new(Int32Builder::new(), 2);
    for (children, valid) in [
        ([Some(10), None], true),
        ([Some(20), Some(21)], false),
        ([None, Some(31)], true),
        ([Some(40), Some(41)], true),
    ] {
        for child in children {
            values.values().append_option(child);
        }
        values.append(valid);
    }
    let payload: ArrayRef = Arc::new(values.finish());
    let build = with_payload(
        padded_build(vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 4])),
        ])?,
        Arc::clone(&payload),
    )?;
    let expected = [Some(1), Some(2), None]
        .into_iter()
        .map(|row| {
            Ok(vec![row.map_or_else(
                || ScalarValue::try_from(payload.data_type()),
                |row| ScalarValue::try_from_array(payload.as_ref(), row),
            )?])
        })
        .collect::<Result<Vec<_>>>()?;
    for use_perfect_hash in [false, true] {
        let join = join_builder(
            &build,
            probe(Arc::new(Int32Array::from(vec![2, 3])))?,
            JoinType::Right,
            vec![2],
        )?
        .build()?;
        let output = collect_join(join, 2, use_perfect_hash).await?;
        assert_rows_unordered(scalar_rows(&output)?, &expected);
    }
    Ok(())
}
