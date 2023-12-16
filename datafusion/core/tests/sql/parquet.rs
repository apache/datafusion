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
use datafusion_common::cast::{as_list_array, as_primitive_array, as_string_array};
use tempfile::TempDir;

use super::*;

#[tokio::test]
async fn window_fn_timestamp_tz() {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "t0",
        "tests/data/timestamp_with_tz.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let sql = "SELECT count, LAG(timestamp, 1) OVER (ORDER BY timestamp) FROM t0";
    let dataframe = ctx.sql(sql).await.unwrap();
    let results = dataframe.collect().await.unwrap();

    let mut num_rows = 0;
    for batch in results {
        num_rows += batch.num_rows();
        assert_eq!(2, batch.num_columns());

        let ty = batch.column(0).data_type().clone();
        assert_eq!(DataType::Int64, ty);

        let ty = batch.column(1).data_type().clone();
        assert_eq!(
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ty
        );
    }

    assert_eq!(131072, num_rows);
}

#[tokio::test]
async fn parquet_single_nan_schema() {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "single_nan",
        &format!("{testdata}/single_nan.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let dataframe = ctx.sql(sql).await.unwrap();
    let results = dataframe.collect().await.unwrap();
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
        &format!("{testdata}/list_columns.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new_list(
            "int64_list",
            Field::new("item", DataType::Int64, true),
            true,
        ),
        Field::new_list("utf8_list", Field::new("item", DataType::Utf8, true), true),
    ]));

    let sql = "SELECT int64_list, utf8_list FROM list_columns";
    let dataframe = ctx.sql(sql).await.unwrap();
    let results = dataframe.collect().await.unwrap();

    //   int64_list              utf8_list
    // 0  [1, 2, 3]        [abc, efg, hij]
    // 1  [None, 1]                   None
    // 2        [4]  [efg, None, hij, xyz]

    assert_eq!(1, results.len());
    let batch = &results[0];
    assert_eq!(3, batch.num_rows());
    assert_eq!(2, batch.num_columns());
    assert_eq!(schema, batch.schema());

    let int_list_array = as_list_array(batch.column(0)).unwrap();
    let utf8_list_array = as_list_array(batch.column(1)).unwrap();

    assert_eq!(
        as_primitive_array::<Int64Type>(&int_list_array.value(0)).unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![Some(1), Some(2), Some(3),])
    );

    assert_eq!(
        as_string_array(&utf8_list_array.value(0)).unwrap(),
        &StringArray::from(vec![Some("abc"), Some("efg"), Some("hij"),])
    );

    assert_eq!(
        as_primitive_array::<Int64Type>(&int_list_array.value(1)).unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![None, Some(1),])
    );

    assert!(utf8_list_array.is_null(1));

    assert_eq!(
        as_primitive_array::<Int64Type>(&int_list_array.value(2)).unwrap(),
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
        let c1s = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let c2s = Arc::new(StringArray::from(vec!["aaa", "bbb", "ccc"]));
        let c3s = Arc::new(Int64Array::from(vec![100, 200, 300]));
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
    let expected = [
        "+-------------+",
        "| MAX(foo.c1) |",
        "+-------------+",
        "| 3           |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT min(c2) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-------------+",
        "| MIN(foo.c2) |",
        "+-------------+",
        "| aaa         |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT max(c3) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-------------+",
        "| MAX(foo.c3) |",
        "+-------------+",
        "| 300         |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "SELECT min(c4) FROM foo";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-------------+",
        "| MIN(foo.c4) |",
        "+-------------+",
        "| 1970-01-02  |",
        "+-------------+",
    ];

    assert_batches_eq!(expected, &actual);
}
