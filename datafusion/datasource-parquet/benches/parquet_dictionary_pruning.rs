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

//! Benchmarks for row-group pruning by Parquet dictionaries.
//!
//! Compares three levels of row-group pruning for a query that looks for a
//! single, sparse value in a high-cardinality `Utf8` column:
//!
//! - `statistics_only`: min/max statistics alone (the baseline every reader
//!   already gets, dictionary or bloom filter disabled).
//! - `bloom_filter`: adds Parquet Split Block Bloom Filters
//!   (`bloom_filter_on_read`).
//! - `dictionary`: adds exact Parquet dictionary-page pruning
//!   (`dictionary_filter_on_read`), this crate's new row-group index.
//!
//! The dataset has `TOTAL_ROW_GROUPS` row groups, each with
//! `DISTINCT_VALUES_PER_ROW_GROUP` distinct values unique to that row group
//! (so the column is high-cardinality overall, but any single row group's
//! dictionary is small and never falls back to `PLAIN`). The query looks for
//! a value that exists in exactly one row group, so a fully effective
//! pruning strategy skips reading data pages for all the others.
//!
//! Run with `cargo bench -p datafusion-datasource-parquet --bench parquet_dictionary_pruning`.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_datasource_parquet::{
    BloomFilterStatistics, DictionaryStatistics, ParquetAccessPlan, ParquetFileMetrics,
    RowGroupAccessPlanFilter, is_fully_dictionary_encoded,
};
use datafusion_expr::{col, lit};
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_pruning::PruningPredicate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, parquet_column};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::SchemaDescriptor;
use tempfile::TempDir;

const TOTAL_ROW_GROUPS: usize = 200;
const DISTINCT_VALUES_PER_ROW_GROUP: usize = 1_000;
const ROWS_PER_ROW_GROUP: usize = DISTINCT_VALUES_PER_ROW_GROUP;
const TOTAL_VALUES: usize = TOTAL_ROW_GROUPS * DISTINCT_VALUES_PER_ROW_GROUP;
const COLUMN_NAME: &str = "s";

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

/// Present in exactly one row group. Chosen near the middle of the value
/// domain so every row group's `[min, max]` range contains it, but only one
/// row group's dictionary actually does.
fn needle() -> String {
    let global_index = TOTAL_VALUES / 2;
    value_at(
        global_index % TOTAL_ROW_GROUPS,
        global_index / TOTAL_ROW_GROUPS,
    )
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
    Arc::new(Schema::new(vec![Field::new(
        COLUMN_NAME,
        DataType::Utf8,
        false,
    )]))
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("dictionary_pruning.parquet");

    let schema = schema();
    // Dictionary and bloom filter both enabled, so the same file drives all
    // three benchmark scenarios below.
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
        let values: Vec<String> = (0..DISTINCT_VALUES_PER_ROW_GROUP)
            .map(|k| value_at(rg, k))
            .collect();
        let array: ArrayRef = Arc::new(StringArray::from_iter_values(
            values.iter().map(|v| v.as_str()),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        writer.write(&batch)?;
    }
    writer.close()?;

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file_path)?)?;
    assert_eq!(reader.metadata().row_groups().len(), TOTAL_ROW_GROUPS);

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
    })
}

/// `s = needle()` as a [`PruningPredicate`], plus the leaf column index it
/// resolves to in `parquet_schema`.
fn needle_predicate(parquet_schema: &SchemaDescriptor) -> (PruningPredicate, usize) {
    let schema = schema();
    let expr = logical2physical(&col(COLUMN_NAME).eq(lit(needle())), &schema);
    let predicate = PruningPredicate::try_new(expr, schema).expect("valid predicate");
    let (column_idx, _) = parquet_column(parquet_schema, predicate.schema(), COLUMN_NAME)
        .expect("column present");
    (predicate, column_idx)
}

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

fn prune_by_statistics_only(path: &Path) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let reader = SerializedFileReader::new(file).expect("open reader");
    let metadata = reader.metadata();
    let (predicate, _) = needle_predicate(metadata.file_metadata().schema_descr());

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

fn prune_by_bloom_filter(path: &Path) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("open reader");
    let metadata = builder.metadata().clone();
    let (predicate, column_idx) =
        needle_predicate(metadata.file_metadata().schema_descr());
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
            stats.insert(COLUMN_NAME, bf, physical_type, type_length);
        }
        row_group_bloom_filters[idx] = stats;
    }
    access_plan.prune_by_bloom_filters(&predicate, &metrics, &row_group_bloom_filters);
    access_plan.row_group_indexes().collect()
}

fn prune_by_dictionary(path: &Path) -> Vec<usize> {
    let file = std::fs::File::open(path).expect("open file");
    let reader = SerializedFileReader::new(file).expect("open reader");
    let metadata = reader.metadata();
    let (predicate, column_idx) =
        needle_predicate(metadata.file_metadata().schema_descr());

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
            stats.insert(COLUMN_NAME, &dict).expect("decode dictionary");
        }
        row_group_dictionaries[idx] = stats;
    }
    access_plan.prune_by_dictionary(&predicate, &metrics, &row_group_dictionaries);
    access_plan.row_group_indexes().collect()
}

fn parquet_dictionary_pruning(c: &mut Criterion) {
    let dataset_path = DATASET.path().to_owned();
    let mut group = c.benchmark_group("parquet_dictionary_pruning");
    group.throughput(Throughput::Elements(TOTAL_ROW_GROUPS as u64));

    // Sanity + a one-time report of what each strategy actually buys in data
    // scanned, since that's the real payoff -- the pruning phase itself
    // benchmarked below reads a bloom filter or a dictionary from every
    // surviving row group, so it is not free, and a bigger, denser
    // dictionary can cost more to decode than a compact bloom filter would.
    // The win is in how much *data-page* reading gets skipped afterward.
    {
        let file = std::fs::File::open(&dataset_path).expect("open file");
        let metadata = SerializedFileReader::new(file)
            .expect("open reader")
            .metadata()
            .clone();
        let (_, column_idx) = needle_predicate(metadata.file_metadata().schema_descr());

        // Every row group's [min, max] range spans the needle (values are
        // interleaved across row groups, see `value_at`), so statistics
        // alone can't prune any of them -- this is the baseline the other
        // two scenarios are compared against.
        let statistics_only = prune_by_statistics_only(&dataset_path);
        assert_eq!(statistics_only.len(), TOTAL_ROW_GROUPS);
        // Bloom filters are probabilistic but should reliably prune this
        // exact, absent-from-most-groups scenario down to (approximately)
        // one row group.
        let bloom_filter = prune_by_bloom_filter(&dataset_path);
        assert!(bloom_filter.len() <= TOTAL_ROW_GROUPS);
        // Dictionaries are exact: exactly the one row group containing the
        // needle survives.
        let dictionary = prune_by_dictionary(&dataset_path);
        assert_eq!(dictionary.len(), 1);

        let statistics_only_bytes =
            column_bytes(&metadata, column_idx, statistics_only.into_iter());
        let bloom_filter_bytes =
            column_bytes(&metadata, column_idx, bloom_filter.into_iter());
        let dictionary_bytes =
            column_bytes(&metadata, column_idx, dictionary.into_iter());
        eprintln!(
            "parquet_dictionary_pruning: column data bytes an actual scan would \
             read -- statistics_only: {statistics_only_bytes}, bloom_filter: \
             {bloom_filter_bytes}, dictionary: {dictionary_bytes}"
        );
    }

    group.bench_function("statistics_only", |b| {
        b.iter(|| black_box(prune_by_statistics_only(&dataset_path)));
    });

    group.bench_function("bloom_filter", |b| {
        b.iter(|| black_box(prune_by_bloom_filter(&dataset_path)));
    });

    group.bench_function("dictionary", |b| {
        b.iter(|| black_box(prune_by_dictionary(&dataset_path)));
    });

    group.finish();
}

criterion_group!(benches, parquet_dictionary_pruning);
criterion_main!(benches);
