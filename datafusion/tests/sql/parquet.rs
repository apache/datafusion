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

use super::*;

#[tokio::test]
async fn parquet_query() {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------------------+",
        "| id | CAST(alltypes_plain.string_col AS Utf8) |",
        "+----+-----------------------------------------+",
        "| 4  | 0                                       |",
        "| 5  | 1                                       |",
        "| 6  | 0                                       |",
        "| 7  | 1                                       |",
        "| 2  | 0                                       |",
        "| 3  | 1                                       |",
        "| 0  | 0                                       |",
        "| 1  | 1                                       |",
        "+----+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn parquet_single_nan_schema() {
    let mut ctx = ExecutionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet("single_nan", &format!("{}/single_nan.parquet", testdata))
        .await
        .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
#[ignore = "Test ignored, will be enabled as part of the nested Parquet reader"]
async fn parquet_list_columns() {
    let mut ctx = ExecutionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "list_columns",
        &format!("{}/list_columns.parquet", testdata),
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
    let results = collect(plan).await.unwrap();

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
        utf8_list_array
            .value(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
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
    let result = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result.value(0), "efg");
    assert!(result.is_null(1));
    assert_eq!(result.value(2), "hij");
    assert_eq!(result.value(3), "xyz");
}
