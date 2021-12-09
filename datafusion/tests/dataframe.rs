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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};

use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{col, Expr};
use datafusion::{datasource::MemTable, prelude::JoinType};

#[tokio::test]
async fn join() -> Result<()> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let mut ctx = ExecutionContext::new();

    let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
    let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;

    ctx.register_table("aa", Arc::new(table1))?;

    let df1 = ctx.table("aa")?;

    ctx.register_table("aaa", Arc::new(table2))?;

    let df2 = ctx.table("aaa")?;

    let a = df1.join(df2, JoinType::Inner, &["a"], &["a"])?;

    let batches = a.collect().await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

    Ok(())
}

#[tokio::test]
async fn sort_on_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
        ],
    )
    .unwrap();

    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t")
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .sort(vec![Expr::Sort {
            expr: Box::new(col("b")),
            asc: false,
            nulls_first: true,
        }])
        .unwrap();
    let results = df.collect().await.unwrap();

    let expected = vec![
        "+-----+", "| a   |", "+-----+", "| 100 |", "| 10  |", "| 10  |", "| 1   |",
        "+-----+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
