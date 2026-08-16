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

//! Benchmarks the **cost** of row-group pruning by Parquet dictionaries --
//! i.e. how expensive the pruning decision itself is, and (in the
//! `prune_and_scan` group) what that decision actually saves. See
//! `datafusion/core/benches/parquet_dictionary_pruning_query.rs` for the
//! same win measured end to end (full SQL queries, real file I/O, and
//! measured `bytes_scanned` rather than the metadata-derived estimate
//! `report_byte_accounting` below prints) against a second, span/log-shaped
//! dataset; this file isolates the pruning stage itself using a single
//! synthetic file with three query shapes.
//!
//! Compares three levels of row-group pruning:
//!
//! - `statistics_only`: min/max statistics alone (the baseline every reader
//!   already gets, dictionary or bloom filter disabled).
//! - `bloom_filter`: adds Parquet Split Block Bloom Filters
//!   (`bloom_filter_on_read`).
//! - `dictionary`: adds exact Parquet dictionary-page pruning
//!   (`dictionary_filter_on_read`), this crate's new row-group index.
//!
//! against three query shapes, all against the same file:
//!
//! - `point_lookup`: `s = <needle>`, present in exactly one row group.
//! - `in_list`: `s IN (v0 .. v31)`, 32 literals all drawn from one row
//!   group. `Guarantee::In` prunes a row group only when *every* literal is
//!   absent, so a bloom filter's false-positive rate compounds per literal:
//!   a non-matching row group survives with probability
//!   `1 - (1 - fpp)^N`. At the default `fpp = 0.05` and `N = 32` that's
//!   ~81% -- bloom filters retain most non-matching row groups here, while
//!   dictionaries stay exact at any `N`.
//! - `not_in`: `c NOT IN (v0, v1)` on a second, low-cardinality column.
//!   Bloom filters cannot answer "does this row group contain anything
//!   *other than* these values" -- they return `None` and prune nothing;
//!   dictionaries prune every row group whose value set is a subset of the
//!   excluded literals.
//!
//! `s` has `TOTAL_ROW_GROUPS * DISTINCT_VALUES_PER_ROW_GROUP` distinct
//! values overall, each repeated `ROWS_PER_DISTINCT_VALUE` times within its
//! row group -- so a single row group's dictionary is small and never falls
//! back to `PLAIN`, while the column stays high-cardinality overall. `c`
//! mirrors the query bench's `tenant` construction: normal row groups draw
//! from a small set with the extrema pinned so statistics can't prune them,
//! and 3 of every 4 row groups are "noisy" -- drawn only from the two
//! excluded values -- so only dictionary pruning can remove them.
//!
//! Two criterion groups exercise the three strategies over the three query
//! shapes (`{statistics_only,bloom_filter,dictionary}` x
//! `{point_lookup,in_list,not_in}`):
//!
//! - `prune_only` times only the pruning decision itself -- real work
//!   (bloom filters and dictionaries must be read and decoded for every
//!   surviving row group), but not the data-page reads it saves.
//! - `prune_and_scan` prunes, then actually reads the surviving row groups'
//!   data pages via `ParquetRecordBatchReaderBuilder::with_row_groups`.
//!   This is the total a user actually pays, and it's where exactness shows
//!   up: `dictionary` should win here even where it loses in `prune_only`.
//!
//! Run with `cargo bench -p datafusion-datasource-parquet --bench parquet_dictionary_pruning`.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_datasource_parquet::{
    BloomFilterStatistics, DictionaryStatistics, ParquetAccessPlan, ParquetFileMetrics,
    RowGroupAccessPlanFilter, is_fully_dictionary_encoded,
};
use datafusion_expr::{Expr, col, lit};
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_pruning::PruningPredicate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, parquet_column};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::SchemaDescriptor;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

/// Fixed seed so the generated `c` column (and its noisy/normal row group
/// assignment) is identical from run to run.
const SEED: u64 = 0xC0FF_EE00_D1C7_5EED;

const TOTAL_ROW_GROUPS: usize = 200;
const DISTINCT_VALUES_PER_ROW_GROUP: usize = 1_000;
/// Each distinct `s` value is repeated this many times within its row
/// group, so the dictionary indexes real repeated data rather than being
/// the entire column chunk (the pathological, maximally-expensive case).
const ROWS_PER_DISTINCT_VALUE: usize = 8;
const ROWS_PER_ROW_GROUP: usize = DISTINCT_VALUES_PER_ROW_GROUP * ROWS_PER_DISTINCT_VALUE;
const TOTAL_VALUES: usize = TOTAL_ROW_GROUPS * DISTINCT_VALUES_PER_ROW_GROUP;
const COLUMN_NAME: &str = "s";

const IN_LIST_LEN: usize = 32;

const CAT_COLUMN_NAME: &str = "c";
const NORMAL_CAT_COUNT: u32 = 20;
// These sort between `cat-09` and `cat-10`, keeping them inside every
// normal row group's `[min, max]` range so statistics cannot prune a row
// group for `c NOT IN (...)`, only the dictionary can.
const NOT_IN_VALUE_A: &str = "cat-09-a";
const NOT_IN_VALUE_B: &str = "cat-09-b";
/// Every `NORMAL_ROW_GROUP_STRIDE`-th row group draws `c` from the full
/// normal domain; the rest are "noisy" row groups whose `c` column is drawn
/// only from `{NOT_IN_VALUE_A, NOT_IN_VALUE_B}`, so their dictionary is a
/// subset of the excluded values while `min != max` (statistics can't prune
/// them, only the dictionary can). Mirrors the query bench's `tenant`
/// construction: 3 of every 4 row groups here are noisy.
const NORMAL_ROW_GROUP_STRIDE: usize = 4;

fn is_noisy_row_group(rg: usize) -> bool {
    !rg.is_multiple_of(NORMAL_ROW_GROUP_STRIDE)
}

fn normal_row_group_count() -> usize {
    (0..TOTAL_ROW_GROUPS)
        .filter(|rg| !is_noisy_row_group(*rg))
        .count()
}

/// Interleaves (round-robins) values across row groups: row group `rg` gets
/// the values at global positions `rg, rg + TOTAL_ROW_GROUPS, rg + 2 *
/// TOTAL_ROW_GROUPS, ...`. Every row group's `[min, max]` therefore spans
/// almost the entire value domain (min is close to 0, max close to
/// `TOTAL_VALUES`), so plain min/max statistics can't prune any of them --
/// only the *set* of values actually present (from a bloom filter or exact
/// dictionary) can distinguish row groups. This mirrors data that arrives
/// already shuffled with respect to a low/no-correlation column, e.g. trace
/// IDs or session IDs sharded across row groups by arrival time.
fn value_at(rg: usize, k: usize) -> String {
    format!("val-{:06}", rg + k * TOTAL_ROW_GROUPS)
}

/// The row group and local (per-row-group) index of the needle value used
/// by `point_lookup` and `in_list`, chosen near the middle of the value
/// domain so every row group's `[min, max]` range contains it, but only one
/// row group's dictionary actually does.
fn needle_row_group() -> usize {
    (TOTAL_VALUES / 2) % TOTAL_ROW_GROUPS
}

fn needle_local_index() -> usize {
    (TOTAL_VALUES / 2) / TOTAL_ROW_GROUPS
}

/// Present in exactly one row group.
fn needle() -> String {
    value_at(needle_row_group(), needle_local_index())
}

/// `IN_LIST_LEN` values, all drawn from the same single row group as
/// [`needle`].
fn needle_in_list() -> Vec<String> {
    let rg = needle_row_group();
    let k0 = needle_local_index();
    assert!(
        k0 + IN_LIST_LEN <= DISTINCT_VALUES_PER_ROW_GROUP,
        "IN_LIST_LEN ({IN_LIST_LEN}) overruns the needle row group's distinct values"
    );
    (0..IN_LIST_LEN).map(|k| value_at(rg, k0 + k)).collect()
}

struct BenchmarkDataset {
    _tempdir: TempDir,
    file_path: PathBuf,
}

impl BenchmarkDataset {
    fn path(&self) -> &Path {
        &self.file_path
    }
}

static DATASET: LazyLock<BenchmarkDataset> = LazyLock::new(|| {
    create_dataset().expect("failed to prepare parquet benchmark dataset")
});

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COLUMN_NAME, DataType::Utf8, false),
        Field::new(CAT_COLUMN_NAME, DataType::Utf8, false),
    ]))
}

/// Builds one row group's data for both `s` (high-cardinality overall, but
/// small and fully dictionary-encoded per row group) and `c` (low
/// cardinality, normal vs. noisy per [`is_noisy_row_group`]).
fn generate_row_group(rg: usize) -> RecordBatch {
    let mut rng = SmallRng::seed_from_u64(SEED.wrapping_add(rg as u64));
    let noisy = is_noisy_row_group(rg);

    let mut s_values: Vec<String> = Vec::with_capacity(ROWS_PER_ROW_GROUP);
    for k in 0..DISTINCT_VALUES_PER_ROW_GROUP {
        let v = value_at(rg, k);
        for _ in 0..ROWS_PER_DISTINCT_VALUE {
            s_values.push(v.clone());
        }
    }

    let mut c_values: Vec<String> = Vec::with_capacity(ROWS_PER_ROW_GROUP);
    for i in 0..ROWS_PER_ROW_GROUP {
        c_values.push(if noisy {
            if i % 2 == 0 {
                NOT_IN_VALUE_A.to_string()
            } else {
                NOT_IN_VALUE_B.to_string()
            }
        } else if i == 0 {
            // Pin both extrema so every normal row group contains the
            // excluded values within, rather than outside, its statistics.
            "cat-00".to_string()
        } else if i == 1 {
            format!("cat-{:02}", NORMAL_CAT_COUNT - 1)
        } else {
            format!("cat-{:02}", rng.random_range(0..NORMAL_CAT_COUNT))
        });
    }

    let s_array: ArrayRef = Arc::new(StringArray::from_iter_values(
        s_values.iter().map(|v| v.as_str()),
    ));
    let c_array: ArrayRef = Arc::new(StringArray::from_iter_values(
        c_values.iter().map(|v| v.as_str()),
    ));

    RecordBatch::try_new(schema(), vec![s_array, c_array]).expect("valid record batch")
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("dictionary_pruning.parquet");

    let schema = schema();
    // Dictionary and bloom filter both enabled, so the same file drives all
    // three benchmark strategies below.
    let writer_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
        .set_dictionary_enabled(true)
        .set_bloom_filter_enabled(true)
        .build();

    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file_path)?,
        Arc::clone(&schema),
        Some(writer_props),
    )?;

    for rg in 0..TOTAL_ROW_GROUPS {
        let batch = generate_row_group(rg);
        writer.write(&batch)?;
    }
    writer.close()?;

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file_path)?)?;
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), TOTAL_ROW_GROUPS);

    // Load-bearing property: neither column's dictionary may have
    // overflowed into a PLAIN fallback. If one had, `is_fully_dictionary_encoded`
    // would (correctly) refuse the chunk and the corresponding `dictionary`
    // benchmark scenario would silently degrade to "no pruning" -- which
    // would look like a regression in the numbers rather than a broken
    // dataset.
    let s_idx = schema.index_of(COLUMN_NAME).expect("s column");
    let c_idx = schema.index_of(CAT_COLUMN_NAME).expect("c column");
    for rg in 0..TOTAL_ROW_GROUPS {
        assert!(
            is_fully_dictionary_encoded(metadata.row_group(rg).column(s_idx)),
            "s dictionary overflowed to PLAIN in row group {rg}"
        );
        assert!(
            is_fully_dictionary_encoded(metadata.row_group(rg).column(c_idx)),
            "c dictionary overflowed to PLAIN in row group {rg}"
        );
    }

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
    })
}

/// One of the three query shapes benchmarked below: a name, the column it
/// filters on, and how to build its [`PruningPredicate`] against a given
/// file's schema descriptor.
#[derive(Clone, Copy)]
struct QueryShape {
    name: &'static str,
    column_name: &'static str,
    build_predicate: fn(&SchemaDescriptor) -> (PruningPredicate, usize),
}

fn build_predicate(
    parquet_schema: &SchemaDescriptor,
    column_name: &str,
    expr: &Expr,
) -> (PruningPredicate, usize) {
    let schema = schema();
    let physical_expr = logical2physical(expr, &schema);
    let predicate =
        PruningPredicate::try_new(physical_expr, schema).expect("valid predicate");
    let (column_idx, _) = parquet_column(parquet_schema, predicate.schema(), column_name)
        .expect("column present");
    (predicate, column_idx)
}

fn point_lookup_predicate(
    parquet_schema: &SchemaDescriptor,
) -> (PruningPredicate, usize) {
    build_predicate(
        parquet_schema,
        COLUMN_NAME,
        &col(COLUMN_NAME).eq(lit(needle())),
    )
}

fn in_list_predicate(parquet_schema: &SchemaDescriptor) -> (PruningPredicate, usize) {
    let list = needle_in_list().into_iter().map(lit).collect();
    build_predicate(
        parquet_schema,
        COLUMN_NAME,
        &col(COLUMN_NAME).in_list(list, false),
    )
}

fn not_in_predicate(parquet_schema: &SchemaDescriptor) -> (PruningPredicate, usize) {
    let list = vec![lit(NOT_IN_VALUE_A), lit(NOT_IN_VALUE_B)];
    build_predicate(
        parquet_schema,
        CAT_COLUMN_NAME,
        &col(CAT_COLUMN_NAME).in_list(list, true),
    )
}

const QUERY_SHAPES: [QueryShape; 3] = [
    QueryShape {
        name: "point_lookup",
        column_name: COLUMN_NAME,
        build_predicate: point_lookup_predicate,
    },
    QueryShape {
        name: "in_list",
        column_name: COLUMN_NAME,
        build_predicate: in_list_predicate,
    },
    QueryShape {
        name: "not_in",
        column_name: CAT_COLUMN_NAME,
        build_predicate: not_in_predicate,
    },
];

/// Total compressed bytes of the queried column across `indexes`' row
/// groups -- i.e. the data-page bytes an actual scan would have to read for
/// the row groups that survive pruning.
fn column_bytes(
    metadata: &parquet::file::metadata::ParquetMetaData,
    column_idx: usize,
    indexes: impl Iterator<Item = usize>,
) -> i64 {
    indexes
        .map(|idx| metadata.row_group(idx).column(column_idx).compressed_size())
        .sum()
}

fn prune_by_statistics_only(path: &Path, shape: &QueryShape) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let reader = SerializedFileReader::new(file).expect("open reader");
    let metadata = reader.metadata();
    let (predicate, _) = (shape.build_predicate)(metadata.file_metadata().schema_descr());

    let mut access_plan = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(
        metadata.num_row_groups(),
    ));
    let metrics_set = ExecutionPlanMetricsSet::new();
    let metrics = ParquetFileMetrics::new(0, &path.display().to_string(), &metrics_set);
    access_plan.prune_by_statistics(
        predicate.schema(),
        metadata.file_metadata().schema_descr(),
        metadata.row_groups(),
        &predicate,
        &metrics,
    );
    access_plan.row_group_indexes().collect()
}

fn prune_by_bloom_filter(path: &Path, shape: &QueryShape) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("open reader");
    let metadata = builder.metadata().clone();
    let (predicate, column_idx) =
        (shape.build_predicate)(metadata.file_metadata().schema_descr());
    let physical_type = metadata
        .file_metadata()
        .schema_descr()
        .column(column_idx)
        .physical_type();
    let type_length = metadata
        .file_metadata()
        .schema_descr()
        .column(column_idx)
        .type_length();

    let mut access_plan = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(
        metadata.num_row_groups(),
    ));
    let metrics_set = ExecutionPlanMetricsSet::new();
    let metrics = ParquetFileMetrics::new(0, &path.display().to_string(), &metrics_set);
    access_plan.prune_by_statistics(
        predicate.schema(),
        metadata.file_metadata().schema_descr(),
        metadata.row_groups(),
        &predicate,
        &metrics,
    );

    let mut row_group_bloom_filters =
        vec![BloomFilterStatistics::new(); metadata.num_row_groups()];
    for idx in access_plan.row_group_indexes() {
        let mut stats = BloomFilterStatistics::with_capacity(1);
        if let Ok(Some(bf)) = builder.get_row_group_column_bloom_filter(idx, column_idx) {
            stats.insert(shape.column_name, bf, physical_type, type_length);
        }
        row_group_bloom_filters[idx] = stats;
    }
    access_plan.prune_by_bloom_filters(&predicate, &metrics, &row_group_bloom_filters);
    access_plan.row_group_indexes().collect()
}

fn prune_by_dictionary(path: &Path, shape: &QueryShape) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let reader = SerializedFileReader::new(file).expect("open reader");
    let metadata = reader.metadata();
    let (predicate, column_idx) =
        (shape.build_predicate)(metadata.file_metadata().schema_descr());

    let mut access_plan = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(
        metadata.num_row_groups(),
    ));
    let metrics_set = ExecutionPlanMetricsSet::new();
    let metrics = ParquetFileMetrics::new(0, &path.display().to_string(), &metrics_set);
    access_plan.prune_by_statistics(
        predicate.schema(),
        metadata.file_metadata().schema_descr(),
        metadata.row_groups(),
        &predicate,
        &metrics,
    );

    let file = std::fs::File::open(path).expect("open file");
    let mut row_group_dictionaries =
        vec![DictionaryStatistics::new(); metadata.num_row_groups()];
    for idx in access_plan.row_group_indexes() {
        let col_meta = metadata.row_group(idx).column(column_idx);
        if !is_fully_dictionary_encoded(col_meta) {
            continue;
        }
        let mut stats = DictionaryStatistics::with_capacity(1);
        if let Ok(Some(dict)) = ParquetMetaDataReader::read_column_dictionary(
            &file, metadata, idx, column_idx,
        ) {
            stats
                .insert(shape.column_name, &dict)
                .expect("decode dictionary");
        }
        row_group_dictionaries[idx] = stats;
    }
    access_plan.prune_by_dictionary(&predicate, &metrics, &row_group_dictionaries);
    access_plan.row_group_indexes().collect()
}

/// Prunes are cheap relative to a scan; this reads and decodes the data
/// pages of exactly the row groups pruning left standing, which is the
/// total cost a real query actually pays. Returns the row count so the
/// result can't be optimized away.
fn scan_survivors(path: &Path, indexes: &[usize]) -> usize {
    let file = std::fs::File::open(path).expect("open file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("open reader");
    let reader = builder
        .with_row_groups(indexes.to_vec())
        .build()
        .expect("build reader");
    reader
        .map(|batch| batch.expect("read batch").num_rows())
        .sum()
}

/// Sanity-checks each strategy's pruning result against the shape it should
/// (or structurally cannot) exploit, and reports the column bytes an actual
/// scan would read for each -- the real payoff `prune_only` doesn't time.
fn report_byte_accounting(dataset_path: &Path) {
    let file = std::fs::File::open(dataset_path).expect("open file");
    let metadata = SerializedFileReader::new(file)
        .expect("open reader")
        .metadata()
        .clone();

    for shape in &QUERY_SHAPES {
        let (_, column_idx) =
            (shape.build_predicate)(metadata.file_metadata().schema_descr());

        // Every row group's [min, max] range spans the needle(s) (`s` is
        // interleaved across row groups, `c`'s excluded values sort inside
        // every normal row group's range), so statistics alone can't prune
        // any of them for any of the three shapes -- this is the baseline
        // the other two strategies are compared against.
        let statistics_only = prune_by_statistics_only(dataset_path, shape);
        assert_eq!(
            statistics_only.len(),
            TOTAL_ROW_GROUPS,
            "statistics unexpectedly pruned a row group for {} -- the dataset no \
             longer isolates the dictionary's contribution",
            shape.name
        );

        let bloom_filter = prune_by_bloom_filter(dataset_path, shape);
        let dictionary = prune_by_dictionary(dataset_path, shape);

        match shape.name {
            "point_lookup" => {
                // Bloom filters are probabilistic but should reliably prune
                // this exact, absent-from-most-groups scenario down to
                // (approximately) one row group.
                assert!(bloom_filter.len() <= TOTAL_ROW_GROUPS);
                assert_eq!(
                    dictionary.len(),
                    1,
                    "expected dictionary pruning to narrow point_lookup down to \
                     exactly the one row group containing the needle"
                );
            }
            "in_list" => {
                // The needle's row group must always survive (bloom filters
                // never produce false negatives); how many others also
                // survive is the compounding-false-positive-rate
                // observation from the module doc, not an assertion.
                assert!(
                    !bloom_filter.is_empty(),
                    "expected bloom filters to retain the needle's row group for in_list"
                );
                assert_eq!(
                    dictionary.len(),
                    1,
                    "expected dictionary pruning to narrow in_list down to exactly \
                     the one row group containing all the needles"
                );
            }
            "not_in" => {
                assert_eq!(
                    bloom_filter.len(),
                    TOTAL_ROW_GROUPS,
                    "bloom filters must not prune any row group for not_in -- they \
                     cannot prove a row group contains nothing but excluded values"
                );
                assert_eq!(
                    dictionary.len(),
                    normal_row_group_count(),
                    "expected dictionary pruning to remove exactly the noisy row \
                     groups for not_in"
                );
            }
            other => unreachable!("unknown query shape {other}"),
        }

        let statistics_only_bytes =
            column_bytes(&metadata, column_idx, statistics_only.into_iter());
        let bloom_filter_bytes =
            column_bytes(&metadata, column_idx, bloom_filter.into_iter());
        let dictionary_bytes =
            column_bytes(&metadata, column_idx, dictionary.into_iter());
        eprintln!(
            "parquet_dictionary_pruning: {} column data bytes an actual scan would \
             read -- statistics_only: {statistics_only_bytes}, bloom_filter: \
             {bloom_filter_bytes}, dictionary: {dictionary_bytes}",
            shape.name
        );
    }
}

fn parquet_dictionary_pruning(c: &mut Criterion) {
    let dataset_path = DATASET.path().to_owned();

    report_byte_accounting(&dataset_path);

    let mut prune_only = c.benchmark_group("prune_only");
    prune_only.throughput(Throughput::Elements(TOTAL_ROW_GROUPS as u64));
    for shape in &QUERY_SHAPES {
        let path = dataset_path.clone();
        prune_only.bench_function(BenchmarkId::new("statistics_only", shape.name), |b| {
            b.iter(|| black_box(prune_by_statistics_only(&path, shape)));
        });
        prune_only.bench_function(BenchmarkId::new("bloom_filter", shape.name), |b| {
            b.iter(|| black_box(prune_by_bloom_filter(&path, shape)));
        });
        prune_only.bench_function(BenchmarkId::new("dictionary", shape.name), |b| {
            b.iter(|| black_box(prune_by_dictionary(&path, shape)));
        });
    }
    prune_only.finish();

    let mut prune_and_scan = c.benchmark_group("prune_and_scan");
    prune_and_scan.throughput(Throughput::Elements(TOTAL_ROW_GROUPS as u64));
    for shape in &QUERY_SHAPES {
        let path = dataset_path.clone();
        prune_and_scan.bench_function(
            BenchmarkId::new("statistics_only", shape.name),
            |b| {
                b.iter(|| {
                    let indexes = prune_by_statistics_only(&path, shape);
                    black_box(scan_survivors(&path, &indexes))
                });
            },
        );
        prune_and_scan.bench_function(
            BenchmarkId::new("bloom_filter", shape.name),
            |b| {
                b.iter(|| {
                    let indexes = prune_by_bloom_filter(&path, shape);
                    black_box(scan_survivors(&path, &indexes))
                });
            },
        );
        prune_and_scan.bench_function(BenchmarkId::new("dictionary", shape.name), |b| {
            b.iter(|| {
                let indexes = prune_by_dictionary(&path, shape);
                black_box(scan_survivors(&path, &indexes))
            });
        });
    }
    prune_and_scan.finish();
}

criterion_group!(benches, parquet_dictionary_pruning);
criterion_main!(benches);
