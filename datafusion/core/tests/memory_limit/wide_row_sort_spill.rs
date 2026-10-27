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

//! An external sort over wide rows must finish under a memory limit that holds a few rows,
//! even though `batch_size` rows do not fit.
//!
//! The input arrives in small batches, so every input batch fits. Before the fix, the sort
//! wrote its spill files in batches of up to `batch_size` rows. With wide rows, one spilled
//! batch was larger than the memory pool, and the final merge of the spill files, which
//! reserves memory for the largest batch of each file, failed with `ResourcesExhausted`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryViewArray, DictionaryArray, Int32Array, Int64Array, ListArray,
    ListBuilder, RecordBatch, StringArray, StringBuilder, StringViewArray, StructArray,
    UInt32Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{concat_batches, take};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};

/// 64 KiB per row.
const ROW_BYTES: usize = 64 * 1024;
/// 32 MiB of rows in total.
const ROWS: usize = 512;
/// Rows per input batch: 256 KiB, which fits in the pool.
const INPUT_BATCH_ROWS: usize = 4;
/// Holds about 190 rows, less than half of `ROWS`, so the sort must spill.
const POOL_BYTES: usize = 12 * 1024 * 1024;
const SPILL_RESERVATION_BYTES: usize = 1024 * 1024;

fn payload(i: usize) -> String {
    format!("{i:08}{}", "x".repeat(ROW_BYTES))
}

/// The row ranges of the input batches.
fn input_ranges() -> impl Iterator<Item = std::ops::Range<usize>> {
    (0..ROWS)
        .step_by(INPUT_BATCH_ROWS)
        .map(|start| start..start + INPUT_BATCH_ROWS)
}

/// Builds the input batches: an `id` column followed by the given columns for each range.
fn input(
    fields: Vec<Field>,
    mut columns: impl FnMut(std::ops::Range<usize>) -> Vec<ArrayRef>,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut all = vec![Field::new("id", DataType::Int64, false)];
    all.extend(fields);
    let schema = Arc::new(Schema::new(all));
    let batches = input_ranges()
        .map(|rows| {
            let ids = Int64Array::from_iter_values(rows.clone().map(|i| i as i64));
            let mut cols: Vec<ArrayRef> = vec![Arc::new(ids)];
            cols.extend(columns(rows));
            RecordBatch::try_new(Arc::clone(&schema), cols).unwrap()
        })
        .collect();
    (schema, batches)
}

async fn run_sort(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    pool_bytes: Option<usize>,
) -> (Vec<RecordBatch>, usize) {
    let table = MemTable::try_new(Arc::clone(schema), vec![batches.to_vec()]).unwrap();
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(pool_bytes) = pool_bytes {
        runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)));
    }
    let config = SessionConfig::new()
        .with_target_partitions(1)
        // 512 rows of 64 KiB is 32 MiB, more than the pool.
        .with_batch_size(ROWS)
        .with_sort_spill_reservation_bytes(SPILL_RESERVATION_BYTES);
    let ctx = SessionContext::new_with_config_rt(config, runtime.build_arc().unwrap());
    ctx.register_table("t", Arc::new(table)).unwrap();
    let plan = ctx
        .sql("SELECT * FROM t ORDER BY id DESC")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let result = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("the sort must spill and finish");
    let spill_count = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
    (result, spill_count)
}

/// Sorts the input under the memory limit, checks that it spilled, and checks that the
/// result equals a sort of the same input without a limit, column by column.
async fn sort_spills_and_matches(schema: SchemaRef, batches: Vec<RecordBatch>) {
    let (result, spill_count) = run_sort(&schema, &batches, Some(POOL_BYTES)).await;
    assert!(spill_count > 0, "the sort must spill");
    let (expected, _) = run_sort(&schema, &batches, None).await;

    let result = concat_batches(&schema, &result).unwrap();
    let expected = concat_batches(&schema, &expected).unwrap();
    assert_eq!(result.num_rows(), ROWS);
    let ids: Vec<i64> = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(ids, (0..ROWS as i64).rev().collect::<Vec<_>>());
    for (i, field) in schema.fields().iter().enumerate() {
        assert_eq!(
            result.column(i).as_ref(),
            expected.column(i).as_ref(),
            "column {} differs",
            field.name()
        );
    }
}

async fn sort_wide_rows(data_type: DataType) {
    let field = Field::new("payload", data_type.clone(), false);
    let (schema, batches) = input(vec![field], |rows| {
        let payload: ArrayRef = match data_type {
            DataType::Utf8 => Arc::new(StringArray::from_iter_values(rows.map(payload))),
            DataType::Utf8View => {
                Arc::new(StringViewArray::from_iter_values(rows.map(payload)))
            }
            DataType::List(_) => {
                // Four elements of a quarter row each.
                let mut builder = ListBuilder::new(StringBuilder::new());
                for i in rows {
                    let quarter = format!("{i:08}{}", "x".repeat(ROW_BYTES / 4));
                    for _ in 0..4 {
                        builder.values().append_value(&quarter);
                    }
                    builder.append(true);
                }
                Arc::new(builder.finish())
            }
            _ => unreachable!(),
        };
        vec![payload]
    });
    sort_spills_and_matches(schema, batches).await;
}

#[tokio::test]
async fn sort_wide_utf8_rows_spills_and_finishes() {
    sort_wide_rows(DataType::Utf8).await;
}

#[tokio::test]
async fn sort_wide_utf8_view_rows_spills_and_finishes() {
    sort_wide_rows(DataType::Utf8View).await;
}

/// A list column keeps its whole child array alive when it is sliced, so the spill writer
/// must measure and copy only the child range that each piece uses.
#[tokio::test]
async fn sort_wide_list_rows_spills_and_finishes() {
    sort_wide_rows(DataType::new_list(DataType::Utf8, true)).await;
}

fn view_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8View, true)
}

/// Input batches that are slices of a few shared view arrays, as a scan or a `take` over a
/// larger batch emits. Each group of 4 input batches shares one parent's buffers (1 MiB of
/// values), so the sort's in-memory batches and its spilled batches reference the same
/// buffers. The sort charges each input slice for its parent's buffers, so a parent must
/// fit in the pool.
#[tokio::test]
async fn sort_view_slices_sharing_buffers() {
    let group = 4 * INPUT_BATCH_ROWS;
    let parents: Vec<StringViewArray> = (0..ROWS)
        .step_by(group)
        .map(|start| {
            StringViewArray::from_iter_values((start..start + group).map(payload))
        })
        .collect();
    let (schema, batches) = input(vec![view_field("v")], |rows| {
        let parent = &parents[rows.start / group];
        vec![Arc::new(parent.slice(rows.start % group, rows.len()))]
    });
    sort_spills_and_matches(schema, batches).await;
}

/// Input batches that are sparse `take`s of shared view arrays: not slices, so their
/// buffers hold values of other batches' rows that only garbage collection drops.
#[tokio::test]
async fn sort_sparse_taken_views_sharing_buffers() {
    // Each parent holds the rows of 4 input batches, interleaved.
    let group = 4 * INPUT_BATCH_ROWS;
    let parents: Vec<ArrayRef> = (0..ROWS)
        .step_by(group)
        .map(|start| {
            Arc::new(StringViewArray::from_iter_values(
                (start..start + group).map(payload),
            )) as ArrayRef
        })
        .collect();
    let (schema, batches) = input(vec![view_field("v")], |rows| {
        let offset = rows.start % group;
        let indices = UInt32Array::from_iter_values(
            (0..INPUT_BATCH_ROWS).map(|i| (offset + i) as u32),
        );
        vec![take(&parents[rows.start / group], &indices, None).unwrap()]
    });
    sort_spills_and_matches(schema, batches).await;
}

/// Two view columns of each batch share one set of buffers, in different row orders,
/// with short inline values and nulls mixed in.
#[tokio::test]
async fn sort_view_columns_sharing_buffers_with_inline_and_null_values() {
    let (schema, batches) = input(vec![view_field("a"), view_field("b")], |rows| {
        let a: ArrayRef =
            Arc::new(StringViewArray::from_iter(rows.clone().map(|i| {
                match i % 3 {
                    0 => None,
                    1 => Some(format!("s{i}")),
                    _ => Some(payload(i)),
                }
            })));
        let reversed = UInt32Array::from_iter_values((0..rows.len() as u32).rev());
        let b = take(&a, &reversed, None).unwrap();
        vec![a, b]
    });
    sort_spills_and_matches(schema, batches).await;
}

#[tokio::test]
async fn sort_wide_binary_view_rows() {
    let field = Field::new("b", DataType::BinaryView, false);
    let (schema, batches) = input(vec![field], |rows| {
        vec![Arc::new(BinaryViewArray::from_iter_values(
            rows.map(|i| payload(i).into_bytes()),
        ))]
    });
    sort_spills_and_matches(schema, batches).await;
}

fn dictionary_field(values: DataType) -> Field {
    Field::new(
        "d",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(values)),
        false,
    )
}

/// Each input batch has its own dictionary, with one wide value per row plus unused
/// values. The sort concatenates and slices these, so the spilled batches carry merged
/// dictionaries that the spill writer prunes.
async fn sort_wide_dictionary_rows(values: DataType) {
    let (schema, batches) = input(vec![dictionary_field(values.clone())], |rows| {
        // Used values first, then as many unused ones.
        let strings = rows
            .clone()
            .chain(rows.clone().map(|i| i + ROWS))
            .map(payload);
        let values: ArrayRef = match values {
            DataType::Utf8 => Arc::new(StringArray::from_iter_values(strings)),
            DataType::Utf8View => Arc::new(StringViewArray::from_iter_values(strings)),
            _ => unreachable!(),
        };
        let keys = Int32Array::from_iter_values(0..rows.len() as i32);
        vec![Arc::new(DictionaryArray::try_new(keys, values).unwrap())]
    });
    sort_spills_and_matches(schema, batches).await;
}

#[tokio::test]
async fn sort_wide_dictionary_rows_spills_and_finishes() {
    sort_wide_dictionary_rows(DataType::Utf8).await;
}

#[tokio::test]
async fn sort_wide_dictionary_of_views_rows_spills_and_finishes() {
    sort_wide_dictionary_rows(DataType::Utf8View).await;
}

/// Views below the top level, in a struct and in a list.
#[tokio::test]
async fn sort_wide_nested_view_rows() {
    let struct_fields = Fields::from(vec![view_field("v")]);
    let item = Arc::new(Field::new_list_field(DataType::Utf8View, true));
    let fields = vec![
        Field::new("s", DataType::Struct(struct_fields.clone()), false),
        Field::new("l", DataType::List(Arc::clone(&item)), false),
    ];
    let (schema, batches) = input(fields, |rows| {
        let views = |rows: std::ops::Range<usize>| -> ArrayRef {
            Arc::new(StringViewArray::from_iter_values(
                rows.map(|i| format!("{i:08}{}", "x".repeat(ROW_BYTES / 2))),
            ))
        };
        let structs =
            StructArray::try_new(struct_fields.clone(), vec![views(rows.clone())], None)
                .unwrap();
        let lists = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths(vec![1; rows.len()]),
            views(rows),
            None,
        )
        .unwrap();
        vec![Arc::new(structs), Arc::new(lists)]
    });
    sort_spills_and_matches(schema, batches).await;
}
