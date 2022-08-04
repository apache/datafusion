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
use arrow::array::Float64Array;

#[tokio::test]
async fn test_atan2() -> Result<()> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
    ]));

    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0, 1.0, -1.0, -1.0])),
            Arc::new(Float64Array::from(vec![2.0, -2.0, 2.0, -2.0])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT atan2(y, x) FROM t1";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------------------+",
        "| atan2(t1.y,t1.x)    |",
        "+---------------------+",
        "| 1.1071487177940904  |",
        "| -1.1071487177940904 |",
        "| 2.0344439357957027  |",
        "| -2.0344439357957027 |",
        "+---------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    Ok(())
}
