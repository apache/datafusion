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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};
use datafusion::from_slice::FromSlice;
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_plan::{col, Expr};
use datafusion::prelude::CsvReadOptions;
use datafusion::{datasource::MemTable, prelude::JoinType};
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::{avg, count, lit, sum};

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
            Arc::new(StringArray::from_slice(&["a", "b", "c", "d"])),
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from_slice(&["a", "b", "c", "d"])),
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
    let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;

    ctx.register_table("aa", Arc::new(table1))?;

    let df1 = ctx.table("aa")?;

    ctx.register_table("aaa", Arc::new(table2))?;

    let df2 = ctx.table("aaa")?;

    let a = df1.join(df2, JoinType::Inner, &["a"], &["a"], None)?;

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
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
            Arc::new(Int32Array::from_slice(&[2, 12, 12, 120])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
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

    #[rustfmt::skip]
    let expected = vec![
        "+-----+",
        "| a   |",
        "+-----+",
        "| 100 |",
        "| 10  |",
        "| 10  |",
        "| 1   |",
        "+-----+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn filter_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[1, 10, 10, 100]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t")
        .unwrap()
        .select(vec![(col("a").eq(lit(10))).alias("a")])
        .unwrap()
        .filter(col("a"))
        .unwrap();
    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
    let expected = vec![
        "+------+",
        "| a    |",
        "+------+",
        "| true |",
        "| true |",
        "+------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[1, 10, 10, 100]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t")
        .unwrap()
        .select(vec![col("a").alias("a")])
        .unwrap()
        .select(vec![(col("a").eq(lit(10))).alias("a")])
        .unwrap()
        .select(vec![col("a")])
        .unwrap();

    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
        let expected = vec![
        "+-------+",
        "| a     |",
        "+-------+",
        "| false |",
        "| true  |",
        "| true  |",
        "| false |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets() -> Result<()> {
    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("a")],
        vec![col("b")],
        vec![col("a"), col("b")],
    ]));

    let df = create_test_table()?
        .aggregate(vec![grouping_set_expr], vec![count(col("a"))])?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("a")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("b")),
                asc: false,
                nulls_first: true,
            },
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+-----------+-----+---------------+",
        "| a         | b   | COUNT(test.a) |",
        "+-----------+-----+---------------+",
        "|           | 100 | 1             |",
        "|           | 10  | 2             |",
        "|           | 1   | 1             |",
        "| abcDEF    |     | 1             |",
        "| abcDEF    | 1   | 1             |",
        "| abc123    |     | 1             |",
        "| abc123    | 10  | 1             |",
        "| CBAdef    |     | 1             |",
        "| CBAdef    | 10  | 1             |",
        "| 123AbcDef |     | 1             |",
        "| 123AbcDef | 100 | 1             |",
        "+-----------+-----+---------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets_count() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(lit(1))])?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("c1")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("c2")),
                asc: false,
                nulls_first: true,
            },
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+----+----+-----------------+",
        "| c1 | c2 | COUNT(Int32(1)) |",
        "+----+----+-----------------+",
        "|    | 5  | 14              |",
        "|    | 4  | 23              |",
        "|    | 3  | 19              |",
        "|    | 2  | 22              |",
        "|    | 1  | 22              |",
        "| e  |    | 21              |",
        "| d  |    | 18              |",
        "| c  |    | 21              |",
        "| b  |    | 19              |",
        "| a  |    | 21              |",
        "+----+----+-----------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_set_array_agg_with_overflow() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
        vec![col("c1"), col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(
            vec![grouping_set_expr],
            vec![
                sum(col("c3")).alias("sum_c3"),
                avg(col("c3")).alias("avg_c3"),
            ],
        )?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("c1")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("c2")),
                asc: false,
                nulls_first: true,
            },
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+----+----+--------+---------------------+",
        "| c1 | c2 | sum_c3 | avg_c3              |",
        "+----+----+--------+---------------------+",
        "|    | 5  | -194   | -13.857142857142858 |",
        "|    | 4  | 29     | 1.2608695652173914  |",
        "|    | 3  | 395    | 20.789473684210527  |",
        "|    | 2  | 184    | 8.363636363636363   |",
        "|    | 1  | 367    | 16.681818181818183  |",
        "| e  |    | 847    | 40.333333333333336  |",
        "| e  | 5  | -22    | -11                 |",
        "| e  | 4  | 261    | 37.285714285714285  |",
        "| e  | 3  | 192    | 48                  |",
        "| e  | 2  | 189    | 37.8                |",
        "| e  | 1  | 227    | 75.66666666666667   |",
        "| d  |    | 458    | 25.444444444444443  |",
        "| d  | 5  | -99    | -49.5               |",
        "| d  | 4  | 162    | 54                  |",
        "| d  | 3  | 124    | 41.333333333333336  |",
        "| d  | 2  | 328    | 109.33333333333333  |",
        "| d  | 1  | -57    | -8.142857142857142  |",
        "| c  |    | -28    | -1.3333333333333333 |",
        "| c  | 5  | 24     | 12                  |",
        "| c  | 4  | -43    | -10.75              |",
        "| c  | 3  | 190    | 47.5                |",
        "| c  | 2  | -389   | -55.57142857142857  |",
        "| c  | 1  | 190    | 47.5                |",
        "| b  |    | -111   | -5.842105263157895  |",
        "| b  | 5  | -1     | -0.2                |",
        "| b  | 4  | -223   | -44.6               |",
        "| b  | 3  | -84    | -42                 |",
        "| b  | 2  | 102    | 25.5                |",
        "| b  | 1  | 95     | 31.666666666666668  |",
        "| a  |    | -385   | -18.333333333333332 |",
        "| a  | 5  | -96    | -32                 |",
        "| a  | 4  | -128   | -32                 |",
        "| a  | 3  | -27    | -4.5                |",
        "| a  | 2  | -46    | -15.333333333333334 |",
        "| a  | 1  | -88    | -17.6               |",
        "+----+----+--------+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

fn create_test_table() -> Result<Arc<DataFrame>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_slice(&[
                "abcDEF",
                "abc123",
                "CBAdef",
                "123AbcDef",
            ])),
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("test", Arc::new(table))?;

    ctx.table("test")
}

async fn aggregates_table(ctx: &SessionContext) -> Result<Arc<DataFrame>> {
    let testdata = datafusion::test_util::arrow_test_data();

    ctx.read_csv(
        format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::default(),
    )
    .await
}
