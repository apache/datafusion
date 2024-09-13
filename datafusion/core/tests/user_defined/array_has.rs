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

use arrow::datatypes::UInt32Type;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::ListArray;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::*;
use datafusion_functions_nested::array_has::ArrayHas;
use std::sync::Arc;

/// can't go next to the `ArrayHas` struct due to circular dependencies, hence this standalone test
#[tokio::test]
async fn array_has_empty_haystack() {
    let ctx = SessionContext::new();
    let udf = ScalarUDF::from(ArrayHas::default());
    ctx.register_udf(udf);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            true,
        )])),
        vec![Arc::new(
            ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
                Some(vec![]),
                Some(vec![]),
                Some(vec![]),
            ]),
        )],
    )
    .unwrap();
    ctx.register_batch("test", batch).unwrap();

    let sql = "SELECT 1 from test where array_has(items, 1)";
    let count = ctx.sql(sql).await.unwrap().count().await.unwrap();
    assert_eq!(count, 0);
}
