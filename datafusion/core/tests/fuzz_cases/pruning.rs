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

use std::sync::{Arc, LazyLock};

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::{BufMut, Bytes, BytesMut};
use datafusion::{
    datasource::{listing::PartitionedFile, physical_plan::ParquetSource},
    prelude::*,
};
use datafusion_common::DFSchema;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{collect, filter::FilterExec, ExecutionPlan};
use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use rand::seq::SliceRandom;
use tokio::sync::Mutex;
use url::Url;

#[tokio::test]
async fn test_utf8_eq() {
    Utf8Test::new(|value| col("a").eq(lit(value))).run().await;
}

#[tokio::test]
async fn test_utf8_not_eq() {
    Utf8Test::new(|value| col("a").not_eq(lit(value)))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_lt() {
    Utf8Test::new(|value| col("a").lt(lit(value))).run().await;
}

#[tokio::test]
async fn test_utf8_lt_eq() {
    Utf8Test::new(|value| col("a").lt_eq(lit(value)))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_gt() {
    Utf8Test::new(|value| col("a").gt(lit(value))).run().await;
}

#[tokio::test]
async fn test_utf8_gt_eq() {
    Utf8Test::new(|value| col("a").gt_eq(lit(value)))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_like() {
    Utf8Test::new(|value| col("a").like(lit(value))).run().await;
}

#[tokio::test]
async fn test_utf8_not_like() {
    Utf8Test::new(|value| col("a").not_like(lit(value)))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_like_prefix() {
    Utf8Test::new(|value| col("a").like(lit(format!("%{}", value))))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_like_suffix() {
    Utf8Test::new(|value| col("a").like(lit(format!("{}%", value))))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_not_like_prefix() {
    Utf8Test::new(|value| col("a").not_like(lit(format!("%{}", value))))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_not_like_ecsape() {
    Utf8Test::new(|value| col("a").not_like(lit(format!("\\%{}%", value))))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_not_like_suffix() {
    Utf8Test::new(|value| col("a").not_like(lit(format!("{}%", value))))
        .run()
        .await;
}

#[tokio::test]
async fn test_utf8_not_like_suffix_one() {
    Utf8Test::new(|value| col("a").not_like(lit(format!("{}_", value))))
        .run()
        .await;
}

/// Fuzz testing for UTF8 predicate pruning
/// The basic idea is that query results should always be the same with or without stats/pruning
/// If we get this right we at least guarantee that there are no incorrect results
/// There may still be suboptimal pruning or stats but that's something we can try to catch
/// with more targeted tests.
//
/// Since we know where the edge cases might be we don't do random black box fuzzing.
/// Instead we fuzz on specific pre-defined axis:
///
/// - Which characters are in each value. We want to make sure to include characters that when
///   incremented, truncated or otherwise manipulated might cause issues.
/// - The values in each row group. This impacts which min/max stats are generated for each rg.
///   We'll generate combinations of the characters with lengths ranging from 1 to 4.
/// - Truncation of statistics to 1, 2 or 3 characters as well as no truncation.
struct Utf8Test {
    /// Test queries the parquet files with this predicate both with and without
    /// pruning enabled
    predicate_generator: Box<dyn Fn(&str) -> Expr + 'static>,
}

impl Utf8Test {
    /// Create a new test with the given predicate generator
    fn new<F: Fn(&str) -> Expr + 'static>(f: F) -> Self {
        Self {
            predicate_generator: Box::new(f),
        }
    }

    /// Run the test by evaluating the predicate on the test files with and
    /// without pruning enable
    async fn run(&self) {
        let ctx = SessionContext::new();

        let mut predicates = vec![];
        for value in Self::values() {
            predicates.push((self.predicate_generator)(value));
        }

        let store = Self::memory_store();
        ctx.register_object_store(&Url::parse("memory://").unwrap(), Arc::clone(store));

        let files = Self::test_files().await;
        let schema = Self::schema();
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();

        println!("Testing {} predicates", predicates.len());
        for predicate in predicates {
            // println!("Testing predicate {:?}", predicate);
            let phys_expr_predicate = ctx
                .create_physical_expr(predicate.clone(), &df_schema)
                .unwrap();
            let expected = execute_with_predicate(
                &files,
                Arc::clone(&phys_expr_predicate),
                false,
                schema.clone(),
                &ctx,
            )
            .await;
            let with_pruning = execute_with_predicate(
                &files,
                phys_expr_predicate,
                true,
                schema.clone(),
                &ctx,
            )
            .await;
            assert_eq!(expected, with_pruning);
        }
    }

    ///  all combinations of interesting charactes  with lengths ranging from 1 to 4
    fn values() -> &'static [String] {
        &VALUES
    }

    /// return the in memory object store
    fn memory_store() -> &'static Arc<dyn ObjectStore> {
        &MEMORY_STORE
    }

    /// return the schema of the created test files
    fn schema() -> Arc<Schema> {
        let schema = &SCHEMA;
        Arc::clone(schema)
    }

    /// Return a list of test files with UTF8 data and combinations of
    /// [`Self::values`]
    async fn test_files() -> Vec<TestFile> {
        let files_mutex = &TESTFILES;
        let mut files = files_mutex.lock().await;
        if !files.is_empty() {
            return (*files).clone();
        }

        let mut rng = rand::thread_rng();
        let values = Self::values();

        let mut row_groups = vec![];
        // generate all combinations of values for row groups (1 or 2 values per rg, more is unnecessary since we only get min/max stats out)
        for rg_length in [1, 2] {
            row_groups.extend(values.iter().cloned().combinations(rg_length));
        }

        println!("Generated {} row groups", row_groups.len());

        // Randomly pick 100 row groups (combinations of said values)
        row_groups.shuffle(&mut rng);
        row_groups.truncate(100);

        let schema = Self::schema();

        let store = Self::memory_store();
        for (idx, truncation_length) in [Some(1), Some(2), None].iter().enumerate() {
            // parquet files only support 32767 row groups per file, so chunk up into multiple files so we don't error if running on a large number of row groups
            for (rg_idx, row_groups) in row_groups.chunks(32766).enumerate() {
                let buf = write_parquet_file(
                    *truncation_length,
                    Arc::clone(&schema),
                    row_groups.to_vec(),
                )
                .await;
                let filename = format!("test_fuzz_utf8_{idx}_{rg_idx}.parquet");
                let size = buf.len();
                let path = Path::from(filename);
                let payload = PutPayload::from(buf);
                store.put(&path, payload).await.unwrap();

                files.push(TestFile { path, size });
            }
        }

        println!("Generated {} parquet files", files.len());
        files.clone()
    }
}

async fn execute_with_predicate(
    files: &[TestFile],
    predicate: Arc<dyn PhysicalExpr>,
    prune_stats: bool,
    schema: Arc<Schema>,
    ctx: &SessionContext,
) -> Vec<String> {
    let parquet_source = if prune_stats {
        ParquetSource::default().with_predicate(Arc::clone(&schema), predicate.clone())
    } else {
        ParquetSource::default()
    };
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("memory://").unwrap(),
        schema.clone(),
        Arc::new(parquet_source),
    )
    .with_file_group(
        files
            .iter()
            .map(|test_file| {
                PartitionedFile::new(test_file.path.clone(), test_file.size as u64)
            })
            .collect(),
    )
    .build();
    let exec = DataSourceExec::from_data_source(config);
    let exec =
        Arc::new(FilterExec::try_new(predicate, exec).unwrap()) as Arc<dyn ExecutionPlan>;

    let batches = collect(exec, ctx.task_ctx()).await.unwrap();
    let mut values = vec![];
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..column.len() {
            values.push(column.value(i).to_string());
        }
    }
    values
}

async fn write_parquet_file(
    truncation_length: Option<usize>,
    schema: Arc<Schema>,
    row_groups: Vec<Vec<String>>,
) -> Bytes {
    let mut buf = BytesMut::new().writer();
    let mut props = WriterProperties::builder();
    if let Some(truncation_length) = truncation_length {
        props = {
            #[allow(deprecated)]
            props.set_max_statistics_size(truncation_length)
        }
    }
    props = props.set_statistics_enabled(EnabledStatistics::Chunk); // row group level
    let props = props.build();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        for rg_values in row_groups.iter() {
            let arr = StringArray::from_iter_values(rg_values.iter());
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap(); // finishes the current row group and starts a new one
        }
        writer.finish().unwrap();
    }
    buf.into_inner().freeze()
}

/// The string values for [Utf8Test::values]
static VALUES: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut rng = rand::thread_rng();

    let characters = [
        "z",
        "0",
        "~",
        "ß",
        "℣",
        "%", // this one is useful for like/not like tests since it will result in randomly inserted wildcards
        "_", // this one is useful for like/not like tests since it will result in randomly inserted wildcards
        "\u{7F}",
        "\u{7FF}",
        "\u{FF}",
        "\u{10FFFF}",
        "\u{D7FF}",
        "\u{FDCF}",
        // null character
        "\u{0}",
    ];
    let value_lengths = [1, 2, 3];
    let mut values = vec![];
    for length in &value_lengths {
        values.extend(
            characters
                .iter()
                .cloned()
                .combinations(*length)
                // now get all permutations of each combination
                .flat_map(|c| c.into_iter().permutations(*length))
                // and join them into strings
                .map(|c| c.join("")),
        );
    }
    println!("Generated {} values", values.len());
    // randomly pick 100 values
    values.shuffle(&mut rng);
    values.truncate(100);
    values
});
/// The schema for the [Utf8Test::schema]
static SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)])));

/// The InMemory object store
static MEMORY_STORE: LazyLock<Arc<dyn ObjectStore>> =
    LazyLock::new(|| Arc::new(InMemory::new()));

/// List of in memory parquet files with UTF8 data
// Use a mutex rather than LazyLock to allow for async initialization
static TESTFILES: LazyLock<Mutex<Vec<TestFile>>> = LazyLock::new(|| Mutex::new(vec![]));

/// Holds a temporary parquet file path and its size
#[derive(Debug, Clone)]
struct TestFile {
    path: Path,
    size: usize,
}
