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

//! Benchmarks for schema-driven nested projection pruning in Parquet.
//!
//! A table's declared (logical) schema can be *narrower* than the physical
//! parquet type of a nested column — e.g. the table declares
//! `events: LIST<STRUCT<x, y>>` while the file contains
//! `events: LIST<STRUCT<x, y, pad_0, ..., pad_7>>`. Engines like Spark
//! communicate nested projection pruning to the scan exactly this way
//! (a clipped read schema), so the reader should fetch and decode only the
//! leaves the declared schema names.
//!
//! Each dataset shape is measured three ways:
//!
//! 1. **narrow_schema**: wide file, narrow declared table schema — the
//!    interesting case; ideally close to (3)
//! 2. **full_schema**: wide file, full table schema — the cost of reading
//!    everything
//! 3. **physically_narrow**: a file that only contains the narrow columns —
//!    the floor
//!
//! At setup the benchmark prints the `bytes_scanned` metric for (1) and (2)
//! so the IO pattern is visible in addition to wall time.

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::ListingTableUrl;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const NUM_BATCHES: usize = 2;
const ROWS_PER_BATCH: usize = 256;
const ROW_GROUP_ROW_COUNT: usize = 256;
const ELEMS_PER_ROW: usize = 3;
const NUM_PAD_FIELDS: usize = 8;
const PAD_LEN: usize = 2048;

/// The narrow item fields: the subset of the struct the table declares.
fn narrow_item_fields() -> Fields {
    Fields::from(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Utf8, true),
    ])
}

/// The wide item fields as written to the file: the narrow fields plus
/// `NUM_PAD_FIELDS` fat string fields the table schema does not mention.
fn wide_item_fields() -> Fields {
    let mut fields = vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, true),
    ];
    for i in 0..NUM_PAD_FIELDS {
        fields.push(Field::new(format!("pad_{i}"), DataType::Utf8, false));
    }
    Fields::from(fields)
}

fn list_schema(item_fields: Fields) -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Struct(item_fields), true));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("events", DataType::List(item), true),
    ]))
}

fn struct_schema(item_fields: Fields) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(item_fields), true),
    ]))
}

/// Distinct pad values so dictionary encoding cannot collapse them.
fn pad_values(count: usize, seed: usize) -> ArrayRef {
    let base = "x".repeat(PAD_LEN);
    let values: Vec<String> = (0..count)
        .map(|i| format!("{:08}{base}", seed + i))
        .collect();
    Arc::new(StringArray::from(values))
}

/// Struct children for `count` elements, restricted to `fields`.
fn item_columns(fields: &Fields, count: usize, seed: usize) -> Vec<ArrayRef> {
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| match field.name().as_str() {
            "x" => Arc::new(Int64Array::from_iter_values(
                (0..count).map(|j| (seed + j) as i64),
            )) as ArrayRef,
            "y" => Arc::new(StringArray::from_iter_values(
                (0..count).map(|j| format!("y-{}", seed + j)),
            )) as ArrayRef,
            _ => pad_values(count, seed * (i + 1)),
        })
        .collect()
}

fn list_batch(fields: &Fields, batch_id: usize) -> RecordBatch {
    let num_elems = ROWS_PER_BATCH * ELEMS_PER_ROW;
    let seed = batch_id * num_elems;
    let struct_array =
        StructArray::new(fields.clone(), item_columns(fields, num_elems, seed), None);
    let item = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));
    let events = ListArray::new(
        item,
        OffsetBuffer::from_lengths(std::iter::repeat_n(ELEMS_PER_ROW, ROWS_PER_BATCH)),
        Arc::new(struct_array),
        None,
    );
    let ids = Int32Array::from_iter_values(
        (0..ROWS_PER_BATCH).map(|i| (batch_id * ROWS_PER_BATCH + i) as i32),
    );
    RecordBatch::try_new(
        list_schema(fields.clone()),
        vec![Arc::new(ids), Arc::new(events)],
    )
    .unwrap()
}

fn struct_batch(fields: &Fields, batch_id: usize) -> RecordBatch {
    let seed = batch_id * ROWS_PER_BATCH;
    let struct_array = StructArray::new(
        fields.clone(),
        item_columns(fields, ROWS_PER_BATCH, seed),
        None,
    );
    let ids =
        Int32Array::from_iter_values((0..ROWS_PER_BATCH).map(|i| (seed + i) as i32));
    RecordBatch::try_new(
        struct_schema(fields.clone()),
        vec![Arc::new(ids), Arc::new(struct_array)],
    )
    .unwrap()
}

fn generate_file(
    schema: SchemaRef,
    batch_fn: impl Fn(usize) -> RecordBatch,
    prefix: &str,
) -> NamedTempFile {
    let mut named_file = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut named_file, schema, Some(properties)).unwrap();
    for batch_id in 0..NUM_BATCHES {
        writer.write(&batch_fn(batch_id)).unwrap();
    }
    let metadata = writer.close().unwrap();
    println!(
        "Generated {} ({} rows, {} row groups, {} bytes)",
        named_file.path().display(),
        metadata.file_metadata().num_rows(),
        metadata.row_groups().len(),
        std::fs::metadata(named_file.path()).unwrap().len(),
    );
    named_file
}

/// Register `path` as `table`, declaring `table_schema` (which may be narrower
/// than the file's physical schema).
fn register_table(
    ctx: &SessionContext,
    rt: &Runtime,
    table: &str,
    path: &str,
    table_schema: SchemaRef,
) {
    let url = ListingTableUrl::parse(path).unwrap();
    let config = rt
        .block_on(ListingTableConfig::new(url).infer_options(&ctx.state()))
        .unwrap()
        .with_schema(table_schema);
    let provider = ListingTable::try_new(config).unwrap();
    ctx.register_table(table, Arc::new(provider)).unwrap();
}

fn query(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

/// Print the parquet scan's `bytes_scanned` for a query so the IO pattern is
/// visible alongside the wall-time measurements.
fn report_bytes_scanned(ctx: &SessionContext, rt: &Runtime, label: &str, sql: &str) {
    let df = rt
        .block_on(ctx.sql(&format!("EXPLAIN ANALYZE {sql}")))
        .unwrap();
    let batches = rt.block_on(df.collect()).unwrap();
    let text = pretty_format_batches(&batches).unwrap().to_string();
    let bytes_scanned = text
        .split("bytes_scanned=")
        .nth(1)
        .map(|rest| {
            rest.split([',', ']'])
                .next()
                .unwrap_or("")
                .trim()
                .to_string()
        })
        .unwrap_or_else(|| "<not found>".to_string());
    println!("{label}: bytes_scanned={bytes_scanned}  ({sql})");
}

struct Fixture {
    ctx: SessionContext,
    /// Same tables, `nested_projection_pruning` disabled: the pre-pruning
    /// behavior, kept so the before/after stays visible.
    ctx_pruning_disabled: SessionContext,
    rt: Runtime,
    _files: Vec<NamedTempFile>,
}

/// Tables:
///   `<name>_narrow_schema`: wide file, narrow declared schema
///   `<name>_full_schema`: wide file, full declared schema
///   `<name>_physically_narrow`: narrow file, narrow declared schema
fn setup(
    name: &str,
    schema_fn: fn(Fields) -> SchemaRef,
    batch_fn: fn(&Fields, usize) -> RecordBatch,
) -> Fixture {
    let rt = Runtime::new().unwrap();
    let ctx = SessionContext::new();
    let mut cfg_off = SessionConfig::new();
    cfg_off
        .options_mut()
        .execution
        .parquet
        .nested_projection_pruning = false;
    let ctx_pruning_disabled = SessionContext::new_with_config(cfg_off);

    let wide = wide_item_fields();
    let narrow = narrow_item_fields();

    let wide_file = generate_file(schema_fn(wide.clone()), |i| batch_fn(&wide, i), name);
    let narrow_file = generate_file(
        schema_fn(narrow.clone()),
        |i| batch_fn(&narrow, i),
        &format!("{name}_narrow"),
    );
    let wide_path = wide_file.path().display().to_string();
    let narrow_path = narrow_file.path().display().to_string();

    register_table(
        &ctx,
        &rt,
        &format!("{name}_narrow_schema"),
        &wide_path,
        schema_fn(narrow.clone()),
    );
    register_table(
        &ctx,
        &rt,
        &format!("{name}_full_schema"),
        &wide_path,
        schema_fn(wide.clone()),
    );
    register_table(
        &ctx,
        &rt,
        &format!("{name}_physically_narrow"),
        &narrow_path,
        schema_fn(narrow.clone()),
    );
    register_table(
        &ctx_pruning_disabled,
        &rt,
        &format!("{name}_narrow_schema"),
        &wide_path,
        schema_fn(narrow.clone()),
    );

    Fixture {
        ctx,
        ctx_pruning_disabled,
        rt,
        _files: vec![wide_file, narrow_file],
    }
}

fn list_struct_benchmarks(c: &mut Criterion) {
    let f = setup("list_struct", list_schema, list_batch);
    let (ctx, rt) = (&f.ctx, &f.rt);

    for table in [
        "list_struct_narrow_schema",
        "list_struct_full_schema",
        "list_struct_physically_narrow",
    ] {
        report_bytes_scanned(ctx, rt, table, &format!("SELECT events FROM {table}"));
    }
    report_bytes_scanned(
        &f.ctx_pruning_disabled,
        rt,
        "list_struct_narrow_schema (pruning disabled)",
        "SELECT events FROM list_struct_narrow_schema",
    );

    let mut group = c.benchmark_group("list_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    // wide file, narrow declared schema: should only read the narrow leaves
    group.bench_function("select_events_narrow_schema", |b| {
        b.iter(|| query(ctx, rt, "SELECT events FROM list_struct_narrow_schema"))
    });

    // wide file, full schema: the cost of reading everything
    group.bench_function("select_events_full_schema", |b| {
        b.iter(|| query(ctx, rt, "SELECT events FROM list_struct_full_schema"))
    });

    // narrow file: the floor
    group.bench_function("select_events_physically_narrow", |b| {
        b.iter(|| query(ctx, rt, "SELECT events FROM list_struct_physically_narrow"))
    });

    // wide file, narrow schema, pruning disabled: the pre-pruning behavior
    group.bench_function("select_events_narrow_schema_pruning_disabled", |b| {
        b.iter(|| {
            query(
                &f.ctx_pruning_disabled,
                rt,
                "SELECT events FROM list_struct_narrow_schema",
            )
        })
    });

    // aggregation over one narrow leaf through unnest
    group.bench_function("sum_x_narrow_schema", |b| {
        b.iter(|| {
            query(
                ctx,
                rt,
                "SELECT SUM(e['x']) FROM (SELECT UNNEST(events) AS e FROM list_struct_narrow_schema)",
            )
        })
    });

    group.finish();
}

fn top_level_struct_benchmarks(c: &mut Criterion) {
    let f = setup("struct", struct_schema, struct_batch);
    let (ctx, rt) = (&f.ctx, &f.rt);

    for table in [
        "struct_narrow_schema",
        "struct_full_schema",
        "struct_physically_narrow",
    ] {
        report_bytes_scanned(ctx, rt, table, &format!("SELECT s FROM {table}"));
    }
    report_bytes_scanned(
        &f.ctx_pruning_disabled,
        rt,
        "struct_narrow_schema (pruning disabled)",
        "SELECT s FROM struct_narrow_schema",
    );

    let mut group = c.benchmark_group("top_level_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("select_struct_narrow_schema", |b| {
        b.iter(|| query(ctx, rt, "SELECT s FROM struct_narrow_schema"))
    });

    group.bench_function("select_struct_full_schema", |b| {
        b.iter(|| query(ctx, rt, "SELECT s FROM struct_full_schema"))
    });

    group.bench_function("select_struct_physically_narrow", |b| {
        b.iter(|| query(ctx, rt, "SELECT s FROM struct_physically_narrow"))
    });

    // wide file, narrow schema, pruning disabled: the pre-pruning behavior
    group.bench_function("select_struct_narrow_schema_pruning_disabled", |b| {
        b.iter(|| {
            query(
                &f.ctx_pruning_disabled,
                rt,
                "SELECT s FROM struct_narrow_schema",
            )
        })
    });

    // get_field on a schema-narrowed struct column: the expression-level
    // pruning path interacting with the schema-level narrowing
    group.bench_function("sum_x_narrow_schema", |b| {
        b.iter(|| query(ctx, rt, "SELECT SUM(s['x']) FROM struct_narrow_schema"))
    });

    group.finish();
}

criterion_group!(benches, list_struct_benchmarks, top_level_struct_benchmarks);
criterion_main!(benches);
