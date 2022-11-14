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

use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Statistics;
use datafusion_expr::{col, lit, Expr};
use object_store::path::Path;
use object_store::ObjectMeta;
use tokio_stream::StreamExt;

async fn get_parquet_exec(filter: Expr, session_ctx: SessionContext) -> ParquetExec {
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let store = session_ctx
        .runtime_env()
        .object_store(&object_store_url)
        .unwrap();

    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/alltypes_tiny_pages.parquet", testdata);

    let location = Path::from_filesystem_path(filename.as_str()).unwrap();
    let metadata = std::fs::metadata(filename).expect("Local file metadata");
    let meta = ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len() as usize,
    };

    let schema = ParquetFormat::default()
        .infer_schema(&store, &[meta.clone()])
        .await
        .unwrap();

    let partitioned_file = PartitionedFile {
        object_meta: meta,
        partition_values: vec![],
        range: None,
        extensions: None,
    };

    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url,
            file_groups: vec![vec![partitioned_file]],
            file_schema: schema,
            statistics: Statistics::default(),
            // file has 10 cols so index 12 should be month
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            config_options: ConfigOptions::new().into_shareable(),
            output_ordering: None,
        },
        Some(filter),
        None,
    );
    parquet_exec.with_enable_page_index(true)
}

#[tokio::test]
async fn page_index_filter_one_col() {
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    // 1.create filter month == 1;
    let filter = col("month").eq(lit(1_i32));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // `month = 1` from the page index should create below RowSelection
    //  vec.push(RowSelector::select(312));
    //  vec.push(RowSelector::skip(3330));
    //  vec.push(RowSelector::select(333));
    //  vec.push(RowSelector::skip(3330));
    // total 645 row
    assert_eq!(batch.num_rows(), 645);

    // 2. create filter month == 1 or month == 2;
    let filter = col("month").eq(lit(1_i32)).or(col("month").eq(lit(2_i32)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // `month = 12` from the page index should create below RowSelection
    //  vec.push(RowSelector::skip(894));
    //  vec.push(RowSelector::select(339));
    //  vec.push(RowSelector::skip(3330));
    //  vec.push(RowSelector::select(312));
    //  vec.push(RowSelector::skip(2430));
    //  combine with before filter total 1275 row
    assert_eq!(batch.num_rows(), 1275);

    // 3. create filter month == 1 and month == 12;
    let filter = col("month")
        .eq(lit(1_i32))
        .and(col("month").eq(lit(12_i32)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await;

    assert!(batch.is_none());

    // 4.create filter 0 < month < 2 ;
    let filter = col("month").gt(lit(0_i32)).and(col("month").lt(lit(2_i32)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // should same with `month = 1`
    assert_eq!(batch.num_rows(), 645);
}

#[tokio::test]
async fn page_index_filter_multi_col() {
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    // create filter month == 1 and year = 2009;
    let filter = col("month").eq(lit(1_i32)).and(col("year").eq(lit(2009)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    //  `year = 2009` from the page index should create below RowSelection
    //  vec.push(RowSelector::select(3663));
    //  vec.push(RowSelector::skip(3642));
    //  combine with `month = 1` total 333 row
    assert_eq!(batch.num_rows(), 333);

    // create filter (year = 2009 or id = 1) and month = 1;
    // this should only use `month = 1` to evaluate the page index.
    let filter = col("month")
        .eq(lit(1_i32))
        .and(col("year").eq(lit(2009)).or(col("id").eq(lit(1))));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 645);

    // create filter (year = 2009 or id = 1)
    // this filter use two columns will not push down
    let filter = col("year").eq(lit(2009)).or(col("id").eq(lit(1)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 7300);

    // create filter (year = 2009 and id = 1) or (year = 2010)
    // this filter use two columns will not push down
    // todo but after use CNF rewrite it could rewrite to (year = 2009 or  year = 2010) and (id = 1 or year = 2010)
    // which could push (year = 2009 or year = 2010) down.
    let filter = col("year")
        .eq(lit(2009))
        .and(col("id").eq(lit(1)))
        .or(col("year").eq(lit(2010)));

    let parquet_exec = get_parquet_exec(filter, session_ctx.clone()).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 7300);
}
