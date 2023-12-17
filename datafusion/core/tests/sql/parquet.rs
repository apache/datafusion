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

use datafusion_common::cast::{as_list_array, as_primitive_array, as_string_array};

use super::*;

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
