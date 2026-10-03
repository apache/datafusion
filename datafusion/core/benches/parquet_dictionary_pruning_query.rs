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

//! End-to-end wall-clock benchmark for row-group pruning by Parquet
//! dictionaries (`datafusion.execution.parquet.dictionary_filter_on_read`).
//!
//! `datafusion/datasource-parquet/benches/parquet_dictionary_pruning.rs`
//! only measures the *cost* of the pruning decision itself (calling
//! `prune_by_statistics` / `prune_by_bloom_filters` / `prune_by_dictionary`
//! directly, no `SessionContext`, no scan). This benchmark instead runs
//! full SQL queries end to end (`SessionContext::sql` -> `DataFrame::collect`)
//! against real Parquet files, over a dataset shaped like observability
//! span/log data -- the case this optimization is unambiguously the right
//! tool for:
//!
//! - Rows land in arrival (timestamp) order. Spans belonging to one trace
//!   are emitted close together, so a given `trace_id` is *spatially
//!   constrained* to a single row group, but many traces are interleaved
//!   within it (see `generate_row_group`'s shuffle). Each trace has
//!   `SPANS_PER_TRACE` spans (default 32, a representative distributed-trace
//!   fan-out), which keeps the `trace_id` dictionary a small fraction of the
//!   row group -- see `ATTRIBUTE_PAD_LEN` below for why that fraction
//!   matters.
//! - `trace_id` is random hex, so every row group's `[min, max]` spans
//!   essentially the whole domain -- min/max statistics prune *nothing*,
//!   ever, for a `trace_id` predicate.
//! - Each row group's distinct trace set is small and bounded, so the
//!   column chunk stays fully dictionary encoded -- its dictionary is the
//!   *exact* membership index for that row group.
//! - Each span carries an `attributes` payload padded to `ATTRIBUTE_PAD_LEN`
//!   hex characters (default 160, ~200 bytes/row after the surrounding
//!   pseudo-JSON), so it dominates file bytes the way real span attribute
//!   payloads do. The bigger this payload relative to a dictionary page,
//!   the more a row group skipped by dictionary pruning actually saves.
//!
//! Bloom filters can answer the same lookups, but only probabilistically
//! (5% false-positive rate by default), must be enabled at write time (off
//! by default), and cost extra file bytes -- dictionary pages are already
//! there. Three query shapes exercise this:
//!
//! - `trace_lookup`: `trace_id = <needle>`, a single-value point lookup.
//! - `trace_lookup_in`: `trace_id IN (<TRACE_IN_LIST_LEN needles>)`, default
//!   32 literals all drawn from one row group. A bloom filter's
//!   false-positive rate compounds per literal: a non-matching row group
//!   survives with probability `1 - (1 - fpp)^N`, so at `N = 32` bloom
//!   filters retain most row groups even though none of them contain a
//!   needle. Dictionaries stay exact at any `N`.
//! - `tenant_not_in`: `tenant NOT IN (...)`, the one direction bloom filters
//!   structurally cannot serve at all -- they can prove "this value might be
//!   present" but never "this row group contains nothing but excluded
//!   values" (see the two-directional `contained` logic in
//!   `datafusion/datasource-parquet/src/dictionary_filter.rs`). 3 of every 4
//!   row groups here are "noisy" (synthetic-monitoring/health-check
//!   traffic, drawn only from the excluded tenants), so dictionary pruning
//!   removes 75% of the scan; the query also sums `octet_length(attributes)`
//!   so the skipped row groups are the ones carrying the dominant payload
//!   column, not just a `count(*)` a reader could serve from metadata alone.
//!
//! `print_evidence_table` also reports measured bytes read per (query,
//! variant) pair, from `ParquetFileMetrics::bytes_scanned` -- incremented by
//! every `get_bytes` / `get_byte_ranges` call the async reader makes, so it
//! covers data-page reads, bloom filter reads, and dictionary-page reads,
//! but **not** the footer or page-index metadata (`get_metadata` never
//! touches it). It must not be read as total file I/O.
//!
//! For `stats_only`, `dictionary`, and `bloom_filter` (each reads at most
//! one kind of index), the report further splits `bytes_scanned` into index
//! bytes and data-page bytes. The split is derived from file metadata, not
//! separately measured: this dataset's statistics never prune a row group
//! (asserted below), so `load_bloom_filters` / `load_dictionaries` always
//! visit every row group for the predicate column, and their per-row-group
//! byte cost is exactly `bloom_filter_length()` (bloom) or
//! `data_page_offset() - dictionary_page_offset()` (dictionary) -- so
//! `data_page_bytes = bytes_scanned - index_bytes` follows exactly.
//! `bloom_and_dictionary` has no such closed form (dictionaries are only
//! read for whichever row groups survive the probabilistic bloom filter
//! stage), so its report prints the total with `n/a` for the split.
//!
//! Run with:
//! ```text
//! cargo bench -p datafusion --bench parquet_dictionary_pruning_query
//! ```
//!
//! Dataset size knobs (env-overridable, see `BenchConfig::from_env`):
//! `ROW_GROUPS` (default 64), `ROWS_PER_ROW_GROUP` (default 65536),
//! `SPANS_PER_TRACE` (default 32), `ATTRIBUTE_PAD_LEN` (default 160),
//! `TRACE_IN_LIST_LEN` (default 32).

use std::collections::HashMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::format::MetricType;
use datafusion_datasource_parquet::is_fully_dictionary_encoded;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Fixed seed so the generated dataset (and the needle trace ids computed
/// against it) is identical from run to run.
const SEED: u64 = 0x5EED_7A3C_5DA7_A5E7;
const SPAN_ID_SALT: u64 = 0x5BA1_1D00_5BA1_1D00;

const NORMAL_TENANT_COUNT: u32 = 20;
const SERVICE_COUNT: u32 = 24;
const NAME_COUNT: u32 = 120;
/// Predicate column names, shared between dataset generation, the query
/// builders, and the byte-accounting report so they can't drift apart.
const TRACE_ID_COLUMN: &str = "trace_id";
const TENANT_COLUMN: &str = "tenant";
// These values sort between `tenant-010` and `tenant-011`, keeping them
// inside normal row groups' min/max range so statistics cannot prove either
// that a row group matches or that it does not match `tenant_not_in`.
const TENANT_OPS: &str = "tenant-010-ops";
const TENANT_SYNTHETIC: &str = "tenant-010-synthetic";
/// Every `NORMAL_ROW_GROUP_STRIDE`-th row group draws `tenant` from the full
/// normal domain; the rest are "noisy" row groups whose `tenant` column is
/// drawn only from the two excluded tenants above, so their dictionary is a
/// subset of `{tenant-010-ops, tenant-010-synthetic}` while `min != max`
/// (statistics can't prune them, only the dictionary can -- see
/// `tenant_not_in`). Synthetic-monitoring and health-check traffic
/// genuinely dominates span volume and arrives in scheduled bursts, so it
/// clusters into its own row groups -- the shape `NOT IN` pruning exists
/// for: 3 of every 4 row groups here are noisy.
const NORMAL_ROW_GROUP_STRIDE: usize = 4;

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    row_groups: usize,
    rows_per_row_group: usize,
    spans_per_trace: usize,
    attribute_pad_len: usize,
    trace_in_list_len: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        let config = Self {
            row_groups: env_usize("ROW_GROUPS", 64),
            rows_per_row_group: env_usize("ROWS_PER_ROW_GROUP", 65_536),
            spans_per_trace: env_usize("SPANS_PER_TRACE", 32),
            attribute_pad_len: env_usize("ATTRIBUTE_PAD_LEN", 160),
            trace_in_list_len: env_usize("TRACE_IN_LIST_LEN", 32),
        };
        assert_eq!(
            config.rows_per_row_group % config.spans_per_trace,
            0,
            "ROWS_PER_ROW_GROUP ({}) must be a multiple of SPANS_PER_TRACE ({})",
            config.rows_per_row_group,
            config.spans_per_trace
        );
        assert!(
            config.rows_per_row_group >= 2,
            "ROWS_PER_ROW_GROUP ({}) must be at least 2",
            config.rows_per_row_group
        );
        assert!(
            config.trace_in_list_len <= config.traces_per_row_group(),
            "TRACE_IN_LIST_LEN ({}) must not exceed traces per row group ({}) -- \
             otherwise needle_trace_ids_in would spill into a neighboring row group",
            config.trace_in_list_len,
            config.traces_per_row_group()
        );
        config
    }

    fn traces_per_row_group(&self) -> usize {
        self.rows_per_row_group / self.spans_per_trace
    }

    fn total_rows(&self) -> usize {
        self.row_groups * self.rows_per_row_group
    }

    fn is_noisy_row_group(&self, rg: usize) -> bool {
        !rg.is_multiple_of(NORMAL_ROW_GROUP_STRIDE)
    }

    fn noisy_row_group_count(&self) -> usize {
        (0..self.row_groups)
            .filter(|rg| self.is_noisy_row_group(*rg))
            .count()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Cheap, deterministic 64-bit mix (SplitMix64). Used to turn a plain
/// integer index into an evenly-distributed, hex-looking id without
/// needing to carry a stateful RNG across row-group boundaries (dataset
/// generation happens one row group at a time).
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A 32-hex-char trace id, globally unique across distinct
/// `global_trace_index` values -- `global_trace_index` is `rg *
/// traces_per_row_group + t`, so a trace never spans row groups by
/// construction.
fn trace_id_for_index(global_trace_index: u64) -> String {
    let hi = splitmix64(global_trace_index ^ SEED);
    let lo = splitmix64(hi);
    format!("{hi:016x}{lo:016x}")
}

/// A 16-hex-char span id. High cardinality (one per row): the dictionary
/// for this column is expected to overflow into a `PLAIN` fallback -- see
/// the "Out of scope" note in the module doc about a future negative
/// benchmark variant using this column.
fn span_id_for_index(global_row_index: u64) -> String {
    let v = splitmix64(global_row_index ^ SPAN_ID_SALT);
    format!("{v:016x}")
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("attributes", DataType::Utf8, false),
    ]))
}

/// ~200-byte pseudo-JSON attributes payload (`ATTRIBUTE_PAD_LEN`
/// hex-digit pad). Dominates file bytes, so row groups skipped by pruning
/// translate into real skipped I/O.
fn pseudo_json_attributes(rng: &mut SmallRng, pad_len: usize) -> String {
    let pad: String = (0..pad_len)
        .map(|_| {
            let nibble = rng.random_range(0..16u32);
            char::from_digit(nibble, 16).expect("valid hex digit")
        })
        .collect();
    let retry = rng.random_range(0..5);
    format!(
        r#"{{"http.method":"GET","http.status_code":200,"retry":{retry},"pad":"{pad}"}}"#
    )
}

/// Builds one row group's data. Trace ids for the row group are generated
/// then shuffled, so spans belonging to one trace are interleaved with
/// spans from other traces -- matching arrival-order data rather than a
/// sorted-by-trace layout.
fn generate_row_group(rg: usize, config: &BenchConfig) -> RecordBatch {
    let rows = config.rows_per_row_group;
    let traces_per_rg = config.traces_per_row_group();
    let mut rng = SmallRng::seed_from_u64(SEED.wrapping_add(rg as u64));

    let mut trace_ids: Vec<String> = Vec::with_capacity(rows);
    for t in 0..traces_per_rg {
        let global_trace_index = (rg * traces_per_rg + t) as u64;
        let trace_id = trace_id_for_index(global_trace_index);
        for _ in 0..config.spans_per_trace {
            trace_ids.push(trace_id.clone());
        }
    }
    trace_ids.shuffle(&mut rng);

    let noisy = config.is_noisy_row_group(rg);
    let base_ts_ms: i64 = 1_700_000_000_000 + (rg * config.rows_per_row_group) as i64;

    let mut ts = Vec::with_capacity(rows);
    let mut span_id = Vec::with_capacity(rows);
    let mut tenant = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut name = Vec::with_capacity(rows);
    let mut duration_ns = Vec::with_capacity(rows);
    let mut attributes = Vec::with_capacity(rows);

    for i in 0..rows {
        let global_row = (rg * config.rows_per_row_group + i) as u64;
        ts.push(base_ts_ms + i as i64);
        span_id.push(span_id_for_index(global_row));
        tenant.push(if noisy {
            if i % 2 == 0 {
                TENANT_OPS.to_string()
            } else {
                TENANT_SYNTHETIC.to_string()
            }
        } else if i == 0 {
            // Pin both extrema so every normal row group contains the
            // excluded values within, rather than outside, its statistics.
            "tenant-001".to_string()
        } else if i == 1 {
            format!("tenant-{NORMAL_TENANT_COUNT:03}")
        } else {
            format!("tenant-{:03}", rng.random_range(1..=NORMAL_TENANT_COUNT))
        });
        service.push(format!("service-{:02}", rng.random_range(0..SERVICE_COUNT)));
        name.push(format!("op-{:03}", rng.random_range(0..NAME_COUNT)));
        duration_ns.push(rng.random_range(1_000i64..50_000_000i64));
        attributes.push(pseudo_json_attributes(&mut rng, config.attribute_pad_len));
    }

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(trace_ids)),
            Arc::new(StringArray::from(span_id)),
            Arc::new(StringArray::from(tenant)),
            Arc::new(StringArray::from(service)),
            Arc::new(StringArray::from(name)),
            Arc::new(Int64Array::from(duration_ns)),
            Arc::new(StringArray::from(attributes)),
        ],
    )
    .expect("valid record batch")
}

/// The row group used to pick needle trace ids for `trace_lookup` /
/// `trace_lookup_in` -- any row group works, this just fixes one.
fn needle_row_group(config: &BenchConfig) -> usize {
    config.row_groups / 2
}

/// A trace id present in exactly one row group.
fn needle_trace_id(config: &BenchConfig) -> String {
    let rg = needle_row_group(config);
    let traces_per_rg = config.traces_per_row_group();
    trace_id_for_index((rg * traces_per_rg) as u64)
}

/// `TRACE_IN_LIST_LEN` trace ids, all drawn from the same single row group
/// as [`needle_trace_id`].
fn needle_trace_ids_in(config: &BenchConfig) -> Vec<String> {
    let rg = needle_row_group(config);
    let traces_per_rg = config.traces_per_row_group();
    (0..config.trace_in_list_len)
        .map(|t| trace_id_for_index((rg * traces_per_rg + t) as u64))
        .collect()
}

fn trace_lookup_query(needle: &str) -> String {
    format!(
        "SELECT ts, service, name, duration_ns, attributes FROM spans WHERE trace_id = '{needle}'"
    )
}

fn trace_lookup_in_query(ids: &[String]) -> String {
    let list = ids
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT ts, service, name, duration_ns, attributes FROM spans WHERE trace_id IN ({list})"
    )
}

fn tenant_not_in_query() -> String {
    format!(
        "SELECT count(*), sum(octet_length(attributes)) FROM spans \
         WHERE tenant NOT IN ('{TENANT_OPS}', '{TENANT_SYNTHETIC}')"
    )
}

/// Which on-disk index (if any) a [`BenchVariant`] reads for its predicate
/// column, and therefore whether `bytes_scanned` can be split into index
/// bytes vs. data-page bytes. `Mixed` (`bloom_and_dictionary`) keeps that
/// split unrepresentable rather than printing a guessed number: dictionaries
/// are only read for whichever row groups survive the probabilistic bloom
/// filter stage, so which row groups those are isn't recoverable from
/// metadata alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IndexKind {
    None,
    Bloom,
    Dictionary,
    Mixed,
}

/// Sums the on-disk footprint of `kind`'s index for `column_idx`, across
/// every row group in `metadata`. Not just statistics survivors: this
/// dataset asserts statistics prune nothing (checked in `build_dataset`), so
/// `load_bloom_filters` / `load_dictionaries` (`opener/mod.rs`) always visit
/// every row group for the predicate column.
///
/// Returns `None` when the total can't be derived from metadata alone:
/// `None`/`Mixed` kinds have no closed form (see [`IndexKind`]), and `Bloom`
/// returns `None` if any row group is missing `bloom_filter_length()` --
/// arrow-rs then probes with `SBBF_HEADER_SIZE_ESTIMATE` bytes and the true
/// size isn't knowable without an extra read.
fn index_bytes(
    metadata: &parquet::file::metadata::ParquetMetaData,
    column_idx: usize,
    kind: IndexKind,
) -> Option<u64> {
    match kind {
        IndexKind::None | IndexKind::Mixed => None,
        IndexKind::Bloom => (0..metadata.num_row_groups())
            .map(|rg| {
                metadata
                    .row_group(rg)
                    .column(column_idx)
                    .bloom_filter_length()
                    .map(|len| len as u64)
            })
            .sum(),
        IndexKind::Dictionary => Some(
            (0..metadata.num_row_groups())
                .map(|rg| {
                    let col_meta = metadata.row_group(rg).column(column_idx);
                    // Chunks that fell back to PLAIN are skipped entirely by
                    // `load_dictionaries` (opener/mod.rs), so they cost 0
                    // dictionary-read bytes -- not an unknown quantity.
                    if !is_fully_dictionary_encoded(col_meta) {
                        return 0;
                    }
                    let dictionary_page_offset = col_meta
                        .dictionary_page_offset()
                        .expect("is_fully_dictionary_encoded implies a dictionary page");
                    (col_meta.data_page_offset() - dictionary_page_offset) as u64
                })
                .sum(),
        ),
    }
}

#[derive(Clone, Copy)]
enum DatasetFile {
    Spans,
    SpansBloom,
}

struct Dataset {
    _tempdir: TempDir,
    spans_path: PathBuf,
    spans_bloom_path: PathBuf,
    config: BenchConfig,
    /// Precomputed by [`index_bytes`] in `build_dataset`, keyed by predicate
    /// column and index kind. Absent entries mean "not derivable" -- see
    /// [`IndexKind`] and [`index_bytes`].
    index_bytes: HashMap<(&'static str, IndexKind), u64>,
}

impl Dataset {
    fn path_for(&self, file: DatasetFile) -> &Path {
        match file {
            DatasetFile::Spans => &self.spans_path,
            DatasetFile::SpansBloom => &self.spans_bloom_path,
        }
    }
}

static DATASET: LazyLock<Dataset> =
    LazyLock::new(|| build_dataset().expect("failed to build benchmark dataset"));

fn build_dataset() -> Result<Dataset> {
    let config = BenchConfig::from_env();
    let tempdir = TempDir::new()?;
    let spans_path = tempdir.path().join("spans.parquet");
    let spans_bloom_path = tempdir.path().join("spans_bloom.parquet");

    let schema = schema();
    let compression = Compression::ZSTD(Default::default());
    let spans_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(config.rows_per_row_group))
        .set_dictionary_enabled(true)
        .set_compression(compression)
        .build();
    // Bloom filters off by default (matches DataFusion's own default), on
    // for `spans_bloom.parquet` so the `bloom_filter` / `bloom_and_dictionary`
    // variants have something to read.
    let spans_bloom_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(config.rows_per_row_group))
        .set_dictionary_enabled(true)
        .set_compression(compression)
        .set_bloom_filter_enabled(true)
        .build();

    let mut spans_writer = ArrowWriter::try_new(
        std::fs::File::create(&spans_path)?,
        Arc::clone(&schema),
        Some(spans_props),
    )?;
    let mut spans_bloom_writer = ArrowWriter::try_new(
        std::fs::File::create(&spans_bloom_path)?,
        Arc::clone(&schema),
        Some(spans_bloom_props),
    )?;

    for rg in 0..config.row_groups {
        let batch = generate_row_group(rg, &config);
        spans_writer.write(&batch)?;
        spans_bloom_writer.write(&batch)?;
    }
    spans_writer.close()?;
    spans_bloom_writer.close()?;

    let spans_bytes = std::fs::metadata(&spans_path)?.len();
    let spans_bloom_bytes = std::fs::metadata(&spans_bloom_path)?.len();

    // Load-bearing property: neither predicate column's dictionary may have
    // overflowed into a PLAIN fallback. If one had, `is_fully_dictionary_encoded`
    // would (correctly) refuse the chunk and the corresponding `dictionary*`
    // variant would silently degrade to "no pruning" -- which would look
    // like a regression in the numbers rather than a broken dataset. This is
    // also the precondition `index_bytes` relies on below.
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&spans_path)?)?;
    let metadata = reader.metadata();
    assert_eq!(
        metadata.num_row_groups(),
        config.row_groups,
        "unexpected row group count -- the writer flushed at a different \
         boundary than ROWS_PER_ROW_GROUP"
    );
    for column_name in [TRACE_ID_COLUMN, TENANT_COLUMN] {
        let column_idx = schema.index_of(column_name).expect("known column");
        for rg in 0..config.row_groups {
            let col_meta = metadata.row_group(rg).column(column_idx);
            assert!(
                is_fully_dictionary_encoded(col_meta),
                "{column_name} dictionary overflowed to PLAIN in row group {rg} -- \
                 shrink ROWS_PER_ROW_GROUP or SPANS_PER_TRACE"
            );
        }
    }

    // The `bloom_filter` variant's index bytes come from spans_bloom.parquet
    // (the only file bloom filters are written to); the `dictionary`
    // variant's come from spans.parquet, read above. Both predicate columns
    // are precomputed so the report needs no further metadata I/O.
    let spans_bloom_reader = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&spans_bloom_path)?,
    )?;
    let spans_bloom_metadata = spans_bloom_reader.metadata();
    let mut index_bytes_by_column = HashMap::new();
    for column_name in [TRACE_ID_COLUMN, TENANT_COLUMN] {
        let column_idx = schema.index_of(column_name).expect("known column");
        if let Some(bytes) = index_bytes(metadata, column_idx, IndexKind::Dictionary) {
            index_bytes_by_column.insert((column_name, IndexKind::Dictionary), bytes);
        }
        if let Some(bytes) =
            index_bytes(spans_bloom_metadata, column_idx, IndexKind::Bloom)
        {
            index_bytes_by_column.insert((column_name, IndexKind::Bloom), bytes);
        }
    }

    println!(
        "parquet_dictionary_pruning_query: {} row groups x {} rows = {} total rows, \
         {} traces/row group",
        config.row_groups,
        config.rows_per_row_group,
        config.total_rows(),
        config.traces_per_row_group(),
    );
    println!(
        "parquet_dictionary_pruning_query: spans.parquet = {spans_bytes} bytes, \
         spans_bloom.parquet = {spans_bloom_bytes} bytes (bloom filter overhead = {} bytes)",
        spans_bloom_bytes as i64 - spans_bytes as i64,
    );

    Ok(Dataset {
        _tempdir: tempdir,
        spans_path,
        spans_bloom_path,
        config,
        index_bytes: index_bytes_by_column,
    })
}

#[derive(Clone, Copy)]
struct BenchVariant {
    name: &'static str,
    file: DatasetFile,
    dictionary_filter_on_read: bool,
    bloom_filter_on_read: bool,
    /// Which index (if any) this variant reads for the predicate column --
    /// drives the byte report's index/data split. See [`IndexKind`].
    predicate_index_kind: IndexKind,
}

const BENCH_VARIANTS: [BenchVariant; 4] = [
    BenchVariant {
        name: "stats_only",
        file: DatasetFile::Spans,
        dictionary_filter_on_read: false,
        bloom_filter_on_read: false,
        predicate_index_kind: IndexKind::None,
    },
    BenchVariant {
        name: "dictionary",
        file: DatasetFile::Spans,
        dictionary_filter_on_read: true,
        bloom_filter_on_read: false,
        predicate_index_kind: IndexKind::Dictionary,
    },
    BenchVariant {
        name: "bloom_filter",
        file: DatasetFile::SpansBloom,
        dictionary_filter_on_read: false,
        bloom_filter_on_read: true,
        predicate_index_kind: IndexKind::Bloom,
    },
    BenchVariant {
        name: "bloom_and_dictionary",
        file: DatasetFile::SpansBloom,
        dictionary_filter_on_read: true,
        bloom_filter_on_read: true,
        predicate_index_kind: IndexKind::Mixed,
    },
];

fn session_config(variant: &BenchVariant) -> SessionConfig {
    let mut cfg = SessionConfig::new();
    let opts = cfg.options_mut();
    opts.execution.parquet.dictionary_filter_on_read = variant.dictionary_filter_on_read;
    opts.execution.parquet.bloom_filter_on_read = variant.bloom_filter_on_read;
    cfg
}

async fn run_query(variant: &BenchVariant, path: &Path, query: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new_with_config(session_config(variant));
    ctx.register_parquet(
        "spans",
        path.to_str().expect("utf8 path"),
        ParquetReadOptions::default(),
    )
    .await
    .expect("register spans table");
    let df = ctx.sql(query).await.expect("plan query");
    df.collect().await.expect("collect query")
}

struct ScanEvidence {
    datasource_line: String,
    metrics: MetricsSet,
}

/// Recursively collects the typed metrics from every node in `plan`.
fn gather_metrics(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            out.push(Arc::clone(metric));
        }
    }
    for child in plan.children() {
        gather_metrics(child, out);
    }
}

/// Runs `query` and returns both the typed execution metrics used by the
/// assertions and the formatted `DataSourceExec` line used in the report.
async fn scan_evidence(variant: &BenchVariant, path: &Path, query: &str) -> ScanEvidence {
    let mut cfg = session_config(variant);
    cfg.options_mut().explain.analyze_level = MetricType::Summary;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_parquet(
        "spans",
        path.to_str().expect("utf8 path"),
        ParquetReadOptions::default(),
    )
    .await
    .expect("register spans table");

    let dataframe = ctx
        .sql(query)
        .await
        .expect("plan query for pruning evidence");
    let plan = dataframe
        .create_physical_plan()
        .await
        .expect("create physical plan for pruning evidence");
    collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("execute query for pruning evidence");

    let formatted = DisplayableExecutionPlan::with_metrics(plan.as_ref())
        .set_metric_types(vec![MetricType::Summary])
        .indent(false)
        .to_string();
    let datasource_line = formatted
        .lines()
        .find(|line| line.contains("DataSourceExec"))
        .unwrap_or_else(|| {
            panic!("no DataSourceExec line in plan for {query:?}:\n{formatted}")
        })
        .trim()
        .to_string();

    let mut metrics = MetricsSet::new();
    gather_metrics(&plan, &mut metrics);
    ScanEvidence {
        datasource_line,
        metrics,
    }
}

/// Returns the raw value of `key=...` from a metrics line, up to (but not
/// including) the next top-level comma.
fn metric_value<'a>(line: &'a str, key: &str) -> &'a str {
    let marker = format!("{key}=");
    let start = line
        .find(&marker)
        .unwrap_or_else(|| panic!("metric {key} not found in line: {line}"))
        + marker.len();
    let rest = &line[start..];
    let end = rest.find(',').unwrap_or(rest.len());
    rest[..end].trim_end_matches(']').trim()
}

/// Returns exact pruning counts from the typed metrics API. The formatted
/// display rounds large counts (for example, 1,001 becomes `1.00 K`), so it
/// must not be parsed for correctness assertions.
fn pruning_counts(metrics: &MetricsSet, name: &str) -> (usize, usize, usize) {
    match metrics.sum_by_name(name) {
        Some(MetricValue::PruningMetrics {
            pruning_metrics, ..
        }) => (
            pruning_metrics.pruned() + pruning_metrics.matched(),
            pruning_metrics.matched(),
            pruning_metrics.fully_matched(),
        ),
        Some(other) => panic!("metric {name} is not a pruning metric: {other:?}"),
        None => panic!("pruning metric {name} not found"),
    }
}

/// Total bytes read for the scan (`bytes_scanned`, summed across
/// partitions), from the typed metrics API -- see
/// `ParquetFileMetrics::bytes_scanned`.
fn scan_bytes(metrics: &MetricsSet) -> usize {
    metrics
        .aggregate_by_name()
        .sum_by_name("bytes_scanned")
        .map(|v| v.as_usize())
        .expect("parquet scan should report a bytes_scanned metric")
}

/// `(query name, SQL, predicate column)` for each query shape -- the
/// predicate column drives which precomputed [`IndexKind`] bytes the byte
/// report attributes to a given variant.
fn queries(config: &BenchConfig) -> Vec<(&'static str, String, &'static str)> {
    vec![
        (
            "trace_lookup",
            trace_lookup_query(&needle_trace_id(config)),
            TRACE_ID_COLUMN,
        ),
        (
            "trace_lookup_in",
            trace_lookup_in_query(&needle_trace_ids_in(config)),
            TRACE_ID_COLUMN,
        ),
        ("tenant_not_in", tenant_not_in_query(), TENANT_COLUMN),
    ]
}

/// Formats a byte count as `"<bytes> (<MiB> MiB)"` for the evidence report.
fn format_bytes(bytes: u64) -> String {
    format!("{bytes} ({:.2} MiB)", bytes as f64 / (1024.0 * 1024.0))
}

/// Runs every (query, variant) pair once, prints a metrics report directly
/// pasteable into the PR reply, and asserts the pruning counts a reviewer
/// would expect so a regression fails the bench loudly rather than silently
/// producing flat numbers.
async fn print_evidence_table(dataset: &Dataset) {
    let config = &dataset.config;
    println!("\n=== parquet_dictionary_pruning_query: pruning evidence ===");
    println!(
        "(bytes_scanned = data pages + bloom filter reads + dictionary page reads; \
         excludes footer/page-index metadata)"
    );
    for (query_name, sql, predicate_column) in queries(config) {
        let is_trace_lookup =
            query_name == "trace_lookup" || query_name == "trace_lookup_in";
        let mut total_bytes: HashMap<&'static str, u64> = HashMap::new();
        let mut data_page_bytes: HashMap<&'static str, u64> = HashMap::new();
        for variant in &BENCH_VARIANTS {
            let path = dataset.path_for(variant.file);
            let evidence = scan_evidence(variant, path, &sql).await;
            let line = &evidence.datasource_line;
            let stats = metric_value(line, "row_groups_pruned_statistics");
            let bloom = metric_value(line, "row_groups_pruned_bloom_filter");
            let dict = metric_value(line, "row_groups_pruned_dictionary");
            println!("--- {query_name} / {} ---", variant.name);
            println!("  row_groups_pruned_statistics:   {stats}");
            println!("  row_groups_pruned_bloom_filter: {bloom}");
            println!("  row_groups_pruned_dictionary:   {dict}");
            println!("  raw: {line}");

            let bytes_scanned = scan_bytes(&evidence.metrics) as u64;
            println!(
                "  bytes_scanned:                  {}",
                format_bytes(bytes_scanned)
            );
            match variant.predicate_index_kind {
                IndexKind::None => {
                    // No index reads at all for this variant, so the whole
                    // scan is data pages -- a self-check for the attribution
                    // below: it must reproduce this same total by
                    // subtraction for `dictionary` and `bloom_filter`.
                    println!("    index bytes:                  0");
                    println!(
                        "    data pages:                    {}",
                        format_bytes(bytes_scanned)
                    );
                    data_page_bytes.insert(variant.name, bytes_scanned);
                }
                IndexKind::Mixed => {
                    println!(
                        "    index/data split:              n/a (bloom_and_dictionary \
                         mixes bloom survivors with dictionary reads -- not derivable \
                         from metadata)"
                    );
                }
                IndexKind::Bloom | IndexKind::Dictionary => {
                    let kind = variant.predicate_index_kind;
                    let index = *dataset
                        .index_bytes
                        .get(&(predicate_column, kind))
                        .unwrap_or_else(|| {
                            panic!(
                                "no precomputed index bytes for {predicate_column}/{kind:?} \
                                 -- build_dataset should have populated every (column, kind) \
                                 pair BENCH_VARIANTS can request"
                            )
                        });
                    let data = bytes_scanned.checked_sub(index).unwrap_or_else(|| {
                        panic!(
                            "index bytes ({index}) exceed bytes_scanned ({bytes_scanned}) \
                             for {query_name} / {} -- the derived index/data split is wrong",
                            variant.name
                        )
                    });
                    let kind_label = if kind == IndexKind::Bloom {
                        "bloom filter"
                    } else {
                        "dictionary pages"
                    };
                    println!(
                        "    {kind_label} ({predicate_column}):  {}",
                        format_bytes(index)
                    );
                    println!("    data pages:                    {}", format_bytes(data));
                    data_page_bytes.insert(variant.name, data);
                }
            }
            total_bytes.insert(variant.name, bytes_scanned);

            // Random hex trace ids and (per-row-group) near-full-domain
            // tenant sets mean statistics must never prune here -- if they
            // started to, this dataset would stop isolating the
            // dictionary's contribution. Only checked once, off the
            // variant that has both bloom and dictionary pruning disabled,
            // since the statistics stage itself doesn't depend on either
            // flag.
            if variant.name == "stats_only" {
                let (total, matched, fully_matched) =
                    pruning_counts(&evidence.metrics, "row_groups_pruned_statistics");
                assert_eq!(total, config.row_groups);
                assert_eq!(
                    matched, config.row_groups,
                    "statistics unexpectedly pruned a row group for {query_name} -- \
                     the dataset no longer isolates the dictionary's contribution"
                );
                if query_name == "tenant_not_in" {
                    assert_eq!(
                        fully_matched, 0,
                        "statistics unexpectedly proved a row group fully matched for \
                         tenant_not_in -- the excluded values must remain within every \
                         normal row group's min/max range"
                    );
                }
            }

            if variant.bloom_filter_on_read {
                let (total, matched, _) =
                    pruning_counts(&evidence.metrics, "row_groups_pruned_bloom_filter");
                assert_eq!(total, config.row_groups);
                if query_name == "trace_lookup" {
                    // Bloom filters can retain false positives, so do not
                    // require the exact one-row-group result dictionaries
                    // provide. They must retain the needle's row group and
                    // prune at least one row group to be a useful baseline.
                    assert!(
                        matched > 0 && matched < total,
                        "expected bloom filters to retain the needle and prune at least \
                         one row group for {query_name} ({})",
                        variant.name
                    );
                } else if query_name == "trace_lookup_in" {
                    // At N = TRACE_IN_LIST_LEN literals, a non-matching row
                    // group's false-positive probability compounds to
                    // 1 - (1 - fpp)^N, so bloom filters may retain most or
                    // even all row groups here. Only require that the
                    // needle's row group survives (bloom filters never
                    // produce false negatives) -- the retained count above
                    // is a printed observation, not an assertion.
                    assert!(
                        matched > 0,
                        "expected bloom filters to retain the needle's row group for \
                         {query_name} ({})",
                        variant.name
                    );
                } else if query_name == "tenant_not_in" {
                    assert_eq!(
                        matched, total,
                        "bloom filters must not prune row groups for tenant_not_in ({})",
                        variant.name
                    );
                }
            }

            if variant.dictionary_filter_on_read && is_trace_lookup {
                let (_, matched, _) =
                    pruning_counts(&evidence.metrics, "row_groups_pruned_dictionary");
                assert_eq!(
                    matched, 1,
                    "expected dictionary pruning to narrow {query_name} down to \
                     exactly the one row group containing the needle trace id \
                     ({})",
                    variant.name
                );
            }

            if variant.dictionary_filter_on_read && query_name == "tenant_not_in" {
                let (_, matched, _) =
                    pruning_counts(&evidence.metrics, "row_groups_pruned_dictionary");
                let expected = config.row_groups - config.noisy_row_group_count();
                assert_eq!(
                    matched, expected,
                    "expected dictionary pruning to remove exactly the \
                     tenant-noisy-only row groups for tenant_not_in ({})",
                    variant.name
                );
            }
        }

        // Cross-variant byte assertions -- deterministic given the dataset
        // invariants asserted above, so a regression in the *bytes read*
        // rather than the *row groups pruned* still fails loudly.
        let stats_only_total = *total_bytes
            .get("stats_only")
            .expect("stats_only variant ran above");
        let dictionary_total = *total_bytes
            .get("dictionary")
            .expect("dictionary variant ran above");
        let bloom_total = *total_bytes
            .get("bloom_filter")
            .expect("bloom_filter variant ran above");
        assert!(
            dictionary_total < stats_only_total,
            "expected dictionary pruning to read fewer total bytes than stats_only for \
             {query_name} ({dictionary_total} >= {stats_only_total}) -- it keeps \
             strictly fewer row groups at no extra index cost"
        );

        let dictionary_data = *data_page_bytes
            .get("dictionary")
            .expect("dictionary data-page bytes computed above");
        let bloom_data = *data_page_bytes
            .get("bloom_filter")
            .expect("bloom_filter data-page bytes computed above");
        match query_name {
            "trace_lookup" => {
                // Not a strict `<`: at one literal, P(bloom filters also
                // retain exactly the needle's row group and nothing else)
                // = 0.95^63 ~= 4%, so a strict assertion would flake. `<=`
                // still catches a real regression (dictionary reading more
                // data than bloom).
                assert!(
                    dictionary_data <= bloom_data,
                    "expected dictionary pruning to read no more data-page bytes than \
                     bloom filters for {query_name} ({dictionary_data} > {bloom_data})"
                );
            }
            "trace_lookup_in" => {
                // At TRACE_IN_LIST_LEN=32 literals, a non-matching row
                // group's false-positive probability compounds to
                // 1 - 0.95^32 ~= 81%, so bloom filters retain most row
                // groups here while the dictionary stays exact at 1 -- safe
                // to assert strictly.
                assert!(
                    dictionary_data < bloom_data,
                    "expected dictionary pruning to read fewer data-page bytes than \
                     bloom filters for {query_name} ({dictionary_data} >= {bloom_data})"
                );
            }
            "tenant_not_in" => {
                assert!(
                    dictionary_data < bloom_data,
                    "expected dictionary pruning to read fewer data-page bytes than \
                     bloom filters for {query_name} ({dictionary_data} >= {bloom_data}) \
                     -- bloom filters cannot prune a NOT IN at all here"
                );
                assert!(
                    bloom_total > stats_only_total,
                    "expected bloom filters to cost more total bytes than stats_only for \
                     {query_name} ({bloom_total} <= {stats_only_total}) -- bloom filters \
                     pay index bytes but prune nothing for a NOT IN"
                );
            }
            other => unreachable!("unknown query shape {other}"),
        }
    }
    println!("=== end pruning evidence ===\n");
}

fn bench_query(
    c: &mut Criterion,
    rt: &Runtime,
    dataset: &Dataset,
    group_name: &str,
    sql: &str,
) {
    let mut group = c.benchmark_group(group_name);
    for variant in &BENCH_VARIANTS {
        let path = dataset.path_for(variant.file).to_path_buf();
        let query = sql.to_string();
        let variant = *variant;
        group.bench_function(variant.name, |b| {
            b.to_async(rt).iter(|| {
                let path = path.clone();
                let query = query.clone();
                async move { black_box(run_query(&variant, &path, &query).await) }
            })
        });
    }
    group.finish();
}

fn parquet_dictionary_pruning_query(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let dataset = &*DATASET;
    rt.block_on(print_evidence_table(dataset));

    let config = &dataset.config;
    let trace_lookup_sql = trace_lookup_query(&needle_trace_id(config));
    let trace_lookup_in_sql = trace_lookup_in_query(&needle_trace_ids_in(config));
    let tenant_not_in_sql = tenant_not_in_query();

    bench_query(c, &rt, dataset, "trace_lookup", &trace_lookup_sql);
    bench_query(c, &rt, dataset, "trace_lookup_in", &trace_lookup_in_sql);
    bench_query(c, &rt, dataset, "tenant_not_in", &tenant_not_in_sql);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(20))
        .sample_size(10);
    targets = parquet_dictionary_pruning_query
}
criterion_main!(benches);
