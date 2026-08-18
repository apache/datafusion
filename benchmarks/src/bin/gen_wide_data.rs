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

//! Synthesizes parquet datasets for the `wide_schema` / `narrow_schema`
//! benchmark suites.
//!
//! The base schema has 8 columns with deterministic synthetic data:
//!
//!   id        Int64    monotonic primary key
//!   value     Float64  numeric value (~5 figures)
//!   count     Int64    auxiliary integer
//!   ts        Date32   spread across ~6 years
//!   category  Utf8     low-cardinality (7 values)
//!   flag      Utf8     low-cardinality (3 values)
//!   status    Utf8     low-cardinality (2 values)
//!   text      Utf8     ~40-char synthetic string
//!
//! `--width-factor N` replicates the schema with `_2`, `_3`, …, `_N`
//! suffix copies. The suffix-renamed copies are zero-filled arrays of
//! the same datatype, laid out **before** the base columns in the
//! output schema — so the base data columns sit at the end. This
//! makes column lookup for the filter / project columns traverse
//! past all the padding, exercising any per-column-position cost in
//! the scanner / planner.
//!
//! Zero-filled rather than null because the parquet reader can
//! shortcut on all-null statistics, muting the wide-schema slowdown
//! by ~35 %. Despite the wide schema, every column still has its own
//! footer / page index / column-chunk metadata; the on-disk size
//! stays small — the point being to measure per-file metadata
//! overhead, not row IO.
//! Zero-filled was chosen over random data or other options because
//! it’s simple to implement, avoids any questions about what
//! the data distribution or shape should look like, and still achieves
//! the goal of having a wide table.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use clap::Parser;
use datafusion::error::Result;
use datafusion_common::exec_datafusion_err;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

#[derive(Debug, Parser)]
#[command(
    name = "gen_wide_data",
    about = "Synthesize a wide-schema parquet dataset for the wide_schema benchmark suite"
)]
struct Cli {
    /// Destination directory. Output files are written as
    /// `events_0001.parquet`, `events_0002.parquet`, …
    #[arg(long)]
    dst_dir: PathBuf,

    /// Number of times to replicate each base column. Copy 1 keeps the
    /// original column names; copies 2..N append `_2`, `_3`, …, all
    /// stored as zero-filled arrays. With the 8-column base schema,
    /// factor 128 yields 1024 columns. Set to 1 for the narrow
    /// variant (no replication, base columns only).
    #[arg(long, default_value_t = 128)]
    width_factor: usize,

    /// Number of output files to write. Each file gets one row group.
    #[arg(long, default_value_t = 256)]
    num_files: usize,

    /// Rows per output file.
    #[arg(long, default_value_t = 50_000)]
    rows_per_file: usize,

    /// Per-batch row count for synthesis. Smaller batches reduce peak
    /// memory; doesn't change output.
    #[arg(long, default_value_t = 16_384)]
    batch_size: usize,
}

const CATEGORIES: &[&str] = &["c0", "c1", "c2", "c3", "c4", "c5", "c6"];
const FLAGS: &[&str] = &["f0", "f1", "f2"];
const STATUSES: &[&str] = &["s0", "s1"];

/// Build a zero-filled array of the given datatype. Used for the
/// suffix-renamed padding columns. Zero-filled rather than all-null so
/// the parquet reader can't shortcut on null-array statistics — the
/// wide-schema slowdown reproduces ~35 % wider with zeros than with
/// nulls.
fn zero_array(dt: &DataType, n: usize) -> ArrayRef {
    match dt {
        DataType::Int32 => {
            Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(0i32, n)))
        }
        DataType::Int64 => {
            Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(0i64, n)))
        }
        DataType::Float64 => Arc::new(Float64Array::from_iter_values(
            std::iter::repeat_n(0.0f64, n),
        )),
        DataType::Date32 => {
            Arc::new(Date32Array::from_iter_values(std::iter::repeat_n(0i32, n)))
        }
        DataType::Utf8 => {
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n("", n)))
        }
        _ => panic!("zero_array: unsupported datatype {dt:?}"),
    }
}

/// Eight-column base schema. All fields nullable so the schema is
/// uniform across base and zero-filled replicated copies.
fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("count", DataType::Int64, true),
        Field::new("ts", DataType::Date32, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("flag", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
    ]))
}

/// Synthesize one batch of length `n` covering rows `[start, start+n)`.
fn synthesize_batch(start: usize, n: usize, schema: &SchemaRef) -> Result<RecordBatch> {
    let id = Int64Array::from_iter_values((start..start + n).map(|i| i as i64));
    let value = Float64Array::from_iter_values(
        (start..start + n).map(|i| 900.0 + ((i as f64 * 13.7) % 99100.0)),
    );
    let count =
        Int64Array::from_iter_values((start..start + n).map(|i| (i % 1000) as i64));
    // Date32 spread across ~6 years (epoch days; 8035 ≈ 1992-01-01).
    let ts = Date32Array::from_iter_values(
        (start..start + n).map(|i| 8035 + ((i % 2200) as i32)),
    );
    let category = StringArray::from_iter_values(
        (start..start + n).map(|i| CATEGORIES[i % CATEGORIES.len()]),
    );
    let flag =
        StringArray::from_iter_values((start..start + n).map(|i| FLAGS[i % FLAGS.len()]));
    let status = StringArray::from_iter_values(
        (start..start + n).map(|i| STATUSES[i % STATUSES.len()]),
    );
    let text = StringArray::from_iter_values(
        (start..start + n).map(|i| format!("synthetic event row {i:010} payload text")),
    );

    let cols: Vec<ArrayRef> = vec![
        Arc::new(id),
        Arc::new(value),
        Arc::new(count),
        Arc::new(ts),
        Arc::new(category),
        Arc::new(flag),
        Arc::new(status),
        Arc::new(text),
    ];

    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| exec_datafusion_err!("building synthetic batch: {e}"))
}

/// Builds the wide schema by laying out the suffix-renamed zero-padded
/// copies first and the unsuffixed base columns last. Putting the base
/// columns at the *end* of the schema is deliberate — column lookup
/// for the filter / project columns has to traverse past all the
/// padding entries, exercising any per-column-position cost in the
/// scanner / planner.
fn widen_schema(src: &SchemaRef, factor: usize) -> SchemaRef {
    let src_fields = src.fields();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(src_fields.len() * factor);
    for copy in 2..=factor {
        for f in src_fields {
            fields.push(Arc::new(Field::new(
                format!("{}_{}", f.name(), copy),
                f.data_type().clone(),
                true,
            )));
        }
    }
    for f in src_fields {
        fields.push(Arc::new(Field::new(
            f.name().clone(),
            f.data_type().clone(),
            f.is_nullable(),
        )));
    }
    Arc::new(Schema::new(fields))
}

fn widen_batch(
    batch: &RecordBatch,
    wide_schema: &SchemaRef,
    factor: usize,
) -> Result<RecordBatch> {
    let cols = batch.columns();
    let n_rows = batch.num_rows();
    let mut wide = Vec::with_capacity(cols.len() * factor);
    // Zero-padded copies first…
    for _ in 2..=factor {
        for c in cols {
            wide.push(zero_array(c.data_type(), n_rows));
        }
    }
    // …then the base data columns at the end.
    for c in cols {
        wide.push(Arc::clone(c));
    }
    RecordBatch::try_new(Arc::clone(wide_schema), wide)
        .map_err(|e| exec_datafusion_err!("building wide batch: {e}"))
}

fn open_writer(
    path: &Path,
    schema: SchemaRef,
    rows_per_group: usize,
) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| exec_datafusion_err!("creating {}: {e}", parent.display()))?;
    }
    let file = File::create(path)
        .map_err(|e| exec_datafusion_err!("creating {}: {e}", path.display()))?;
    let writer_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group.max(1)))
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .build();
    ArrowWriter::try_new(file, schema, Some(writer_props))
        .map_err(|e| exec_datafusion_err!("creating ArrowWriter: {e}"))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.width_factor == 0 {
        return Err(exec_datafusion_err!("--width-factor must be >= 1"));
    }
    if cli.num_files == 0 {
        return Err(exec_datafusion_err!("--num-files must be >= 1"));
    }
    if cli.rows_per_file == 0 {
        return Err(exec_datafusion_err!("--rows-per-file must be >= 1"));
    }
    if cli.batch_size == 0 {
        return Err(exec_datafusion_err!("--batch-size must be >= 1"));
    }

    let raw_schema = base_schema();
    let wide_schema = widen_schema(&raw_schema, cli.width_factor);
    let cap_rows = cli.rows_per_file.saturating_mul(cli.num_files);

    println!(
        "Synthesizing: {} base cols × {} factor = {} cols × {} files of {} rows each (total {} rows, ZSTD(1), 1 row group/file).",
        raw_schema.fields().len(),
        cli.width_factor,
        wide_schema.fields().len(),
        cli.num_files,
        cli.rows_per_file,
        cap_rows,
    );

    std::fs::create_dir_all(&cli.dst_dir)
        .map_err(|e| exec_datafusion_err!("creating {}: {e}", cli.dst_dir.display()))?;

    let mut file_idx: usize = 0;
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut rows_in_current: usize = 0;
    let mut total_written: usize = 0;
    let mut row_cursor: usize = 0;

    'outer: while total_written < cap_rows {
        let batch_n = cli.batch_size.min(cap_rows - total_written);
        let base_batch = synthesize_batch(row_cursor, batch_n, &raw_schema)?;
        let wide_batch = widen_batch(&base_batch, &wide_schema, cli.width_factor)?;
        row_cursor += batch_n;

        let mut remaining = wide_batch;
        while remaining.num_rows() > 0 {
            if writer.is_none() {
                if file_idx >= cli.num_files {
                    break 'outer;
                }
                file_idx += 1;
                let path = if cli.num_files == 1 {
                    cli.dst_dir.join("events.parquet")
                } else {
                    cli.dst_dir.join(format!("events_{file_idx:04}.parquet"))
                };
                writer = Some(open_writer(
                    &path,
                    Arc::clone(&wide_schema),
                    cli.rows_per_file,
                )?);
                rows_in_current = 0;
            }
            let space = cli.rows_per_file.saturating_sub(rows_in_current);
            let take = remaining.num_rows().min(space.max(1));
            let chunk = remaining.slice(0, take);
            writer
                .as_mut()
                .unwrap()
                .write(&chunk)
                .map_err(|e| exec_datafusion_err!("writing chunk: {e}"))?;
            rows_in_current += take;
            total_written += take;
            if take == remaining.num_rows() {
                remaining = remaining.slice(0, 0);
            } else {
                remaining = remaining.slice(take, remaining.num_rows() - take);
            }
            if rows_in_current >= cli.rows_per_file {
                writer
                    .take()
                    .unwrap()
                    .close()
                    .map_err(|e| exec_datafusion_err!("closing writer: {e}"))?;
            }
        }
    }
    if let Some(w) = writer.take() {
        w.close()
            .map_err(|e| exec_datafusion_err!("closing writer: {e}"))?;
    }

    println!(
        "Wrote {} rows across {} files in {}",
        total_written,
        file_idx,
        cli.dst_dir.display(),
    );
    Ok(())
}
