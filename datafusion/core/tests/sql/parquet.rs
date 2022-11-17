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

use std::{fs, path::Path};

use ::parquet::arrow::ArrowWriter;
use datafusion::datasource::listing::ListingOptions;
use datafusion_common::cast::as_string_array;
use tempfile::TempDir;

use super::*;

#[tokio::test]
async fn parquet_query() {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------------------------+",
        "| id | alltypes_plain.string_col |",
        "+----+---------------------------+",
        "| 4  | 0                         |",
        "| 5  | 1                         |",
        "| 6  | 0                         |",
        "| 7  | 1                         |",
        "| 2  | 0                         |",
        "| 3  | 1                         |",
        "| 0  | 0                         |",
        "| 1  | 1                         |",
        "+----+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
/// Test that if sort order is specified in ListingOptions, the sort
/// expressions make it all the way down to the ParquetExec
async fn parquet_with_sort_order_specified() {
    let parquet_read_options = ParquetReadOptions::default();
    let target_partitions = 2;

    // The sort order is not specified
    let options_no_sort = parquet_read_options
        .to_listing_options(target_partitions)
        .with_file_sort_order(None);

    // The sort order is specified (not actually correct in this case)
    let file_sort_order = [col("string_col"), col("int_col")]
        .into_iter()
        .map(|e| {
            let ascending = true;
            let nulls_first = false;
            e.sort(ascending, nulls_first)
        })
        .collect::<Vec<_>>();

    let options_sort = parquet_read_options
        .to_listing_options(target_partitions)
        .with_file_sort_order(Some(file_sort_order));

    // This string appears in ParquetExec if the output ordering is
    // specified
    let expected_output_ordering =
        "output_ordering=[string_col@9 ASC NULLS LAST, int_col@4 ASC NULLS LAST]";

    // when sort not specified, should not appear in the explain plan
    let num_files = 1;
    assert_not_contains!(
        run_query_with_options(options_no_sort, num_files).await,
        expected_output_ordering
    );

    // when sort IS specified, SHOULD appear in the explain plan
    let num_files = 1;
    assert_contains!(
        run_query_with_options(options_sort.clone(), num_files).await,
        expected_output_ordering
    );

    // when sort IS specified, but there are too many files (greater
    // than the number of partitions) sort should not appear
    let num_files = 3;
    assert_not_contains!(
        run_query_with_options(options_sort, num_files).await,
        expected_output_ordering
    );
}

/// Runs a limit query against a parquet file that was registered from
/// options on num_files copies of all_types_plain.parquet
async fn run_query_with_options(options: ListingOptions, num_files: usize) -> String {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();
    let file_path = format!("{}/alltypes_plain.parquet", testdata);

    // Create a directory of parquet files with names
    // 0.parquet
    // 1.parquet
    let tmpdir = TempDir::new().unwrap();
    for i in 0..num_files {
        let target_file = tmpdir.path().join(format!("{i}.parquet"));
        println!("Copying {file_path} to {target_file:?}");
        std::fs::copy(&file_path, target_file).unwrap();
    }

    let provided_schema = None;
    let sql_definition = None;
    ctx.register_listing_table(
        "t",
        tmpdir.path().to_string_lossy(),
        options.clone(),
        provided_schema,
        sql_definition,
    )
    .await
    .unwrap();

    let batches = ctx.sql("explain select int_col, string_col from t order by string_col, int_col limit 10")
        .await
        .expect("planing worked")
        .collect()
        .await
        .expect("execution worked");

    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn fixed_size_binary_columns() {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "t0",
        "tests/parquet/data/test_binary.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let sql = "SELECT ids FROM t0 ORDER BY ids";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();
    for batch in results {
        assert_eq!(466, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
async fn parquet_single_nan_schema() {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "single_nan",
        &format!("{}/single_nan.parquet", testdata),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
#[ignore = "Test ignored, will be enabled as part of the nested Parquet reader"]
async fn parquet_list_columns() {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "list_columns",
        &format!("{}/list_columns.parquet", testdata),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "int64_list",
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "utf8_list",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let sql = "SELECT int64_list, utf8_list FROM list_columns";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();

    //   int64_list              utf8_list
    // 0  [1, 2, 3]        [abc, efg, hij]
    // 1  [None, 1]                   None
    // 2        [4]  [efg, None, hij, xyz]

    assert_eq!(1, results.len());
    let batch = &results[0];
    assert_eq!(3, batch.num_rows());
    assert_eq!(2, batch.num_columns());
    assert_eq!(schema, batch.schema());

    let int_list_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let utf8_list_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(
        int_list_array
            .value(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![Some(1), Some(2), Some(3),])
    );

    assert_eq!(
        as_string_array(&utf8_list_array.value(0)).unwrap(),
        &StringArray::try_from(vec![Some("abc"), Some("efg"), Some("hij"),]).unwrap()
    );

    assert_eq!(
        int_list_array
            .value(1)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![None, Some(1),])
    );

    assert!(utf8_list_array.is_null(1));

    assert_eq!(
        int_list_array
            .value(2)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![Some(4),])
    );

    let result = utf8_list_array.value(2);
    let result = as_string_array(&result).unwrap();

    assert_eq!(result.value(0), "efg");
    assert!(result.is_null(1));
    assert_eq!(result.value(2), "hij");
    assert_eq!(result.value(3), "xyz");
}

#[tokio::test]
async fn parquet_query_with_max_min() {
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");
    let table_path = Path::new(&table_dir);

    let fields = vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("c3", DataType::Int64, true),
        Field::new("c4", DataType::Date32, true),
    ];

    let schema = Arc::new(Schema::new(fields.clone()));

    if let Ok(()) = fs::create_dir(table_path) {
        let filename = "foo.parquet";
        let path = table_path.join(filename);
        let file = fs::File::create(path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None)
                .unwrap();

        // create mock record batch
        let c1s = Arc::new(Int32Array::from_slice([1, 2, 3]));
        let c2s = Arc::new(StringArray::from_slice(["aaa", "bbb", "ccc"]));
        let c3s = Arc::new(Int64Array::from_slice([100, 200, 300]));
        let c4s = Arc::new(Date32Array::from(vec![Some(1), Some(2), Some(3)]));
        let rec_batch =
            RecordBatch::try_new(schema.clone(), vec![c1s, c2s, c3s, c4s]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();
    }

    // query parquet
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "foo",
        &format!("{}/foo.parquet", table_dir.to_str().unwrap()),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let sql = "SELECT max(c1) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------+",
        "| MAX(foo.c1) |",
        "+-------------+",
        "| 3           |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT min(c2) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------+",
        "| MIN(foo.c2) |",
        "+-------------+",
        "| aaa         |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT max(c3) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------+",
        "| MAX(foo.c3) |",
        "+-------------+",
        "| 300         |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT min(c4) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------+",
        "| MIN(foo.c4) |",
        "+-------------+",
        "| 1970-01-02  |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);
}
