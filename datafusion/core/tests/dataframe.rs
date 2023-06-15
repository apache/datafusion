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
use arrow::util::pretty::pretty_format_batches;
use arrow::{
    array::{
        ArrayRef, Int32Array, Int32Builder, ListBuilder, StringArray, StringBuilder,
        StructBuilder, UInt32Array, UInt32Builder,
    },
    record_batch::RecordBatch,
};
use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::JoinType;
use datafusion::prelude::*;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use datafusion::test_util::parquet_test_data;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_execution::config::SessionConfig;
use datafusion_expr::expr::{GroupingSet, Sort};
use datafusion_expr::Expr::Wildcard;
use datafusion_expr::{
    avg, col, count, exists, expr, in_subquery, lit, max, out_ref_col, scalar_subquery,
    sum, AggregateFunction, Expr, ExprSchemable, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};
use datafusion_physical_expr::var_provider::{VarProvider, VarType};

#[tokio::test]
async fn test_count_wildcard_on_sort() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select b,count(*) from t1 group by b order by count(*)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![col("b")], vec![count(Wildcard)])?
        .sort(vec![count(Wildcard).sort(true, false)])?
        .explain(false, false)?
        .collect()
        .await?;
    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );
    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_in() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a,b FROM t1 WHERE a in (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/arrow-datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference betwwen sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(in_subquery(
            col("a"),
            Arc::new(
                ctx.table("t2")
                    .await?
                    .aggregate(vec![], vec![count(Expr::Wildcard)])?
                    .select(vec![count(Expr::Wildcard)])?
                    .into_unoptimized_plan(),
                // Usually, into_optimized_plan() should be used here, but due to
                // https://github.com/apache/arrow-datafusion/issues/5771,
                // subqueries in SQL cannot be optimized, resulting in differences in logical_plan. Therefore, into_unoptimized_plan() is temporarily used here.
            ),
        ))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    // make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_exist() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a, b FROM t1 WHERE EXISTS (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(exists(Arc::new(
            ctx.table("t2")
                .await?
                .aggregate(vec![], vec![count(Expr::Wildcard)])?
                .select(vec![count(Expr::Wildcard)])?
                .into_unoptimized_plan(),
            // Usually, into_optimized_plan() should be used here, but due to
            // https://github.com/apache/arrow-datafusion/issues/5771,
            // subqueries in SQL cannot be optimized, resulting in differences in logical_plan. Therefore, into_unoptimized_plan() is temporarily used here.
        )))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_window() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select COUNT(*) OVER(ORDER BY a DESC RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING)  from t1")
        .await?
        .explain(false, false)?
        .collect()
        .await?;
    let df_results = ctx
        .table("t1")
        .await?
        .select(vec![Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Count),
            vec![Expr::Wildcard],
            vec![],
            vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))],
            WindowFrame {
                units: WindowFrameUnits::Range,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt32(Some(6))),
                end_bound: WindowFrameBound::Following(ScalarValue::UInt32(Some(2))),
            },
        ))])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&df_results)?.to_string(),
        pretty_format_batches(&sql_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_aggregate() -> Result<()> {
    let ctx = create_join_context()?;
    register_alltypes_tiny_pages_parquet(&ctx).await?;

    let sql_results = ctx
        .sql("select count(*) from t1")
        .await?
        .select(vec![count(Expr::Wildcard)])?
        .explain(false, false)?
        .collect()
        .await?;

    // add `.select(vec![count(Expr::Wildcard)])?` to make sure we can analyze all node instead of just top node.
    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![], vec![count(Expr::Wildcard)])?
        .select(vec![count(Expr::Wildcard)])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}
#[tokio::test]
async fn test_count_wildcard_on_where_scalar_subquery() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select a,b from t1 where (select count(*) from t2 where t1.a = t2.a)>0;")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/arrow-datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference between sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(
            scalar_subquery(Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(out_ref_col(DataType::UInt32, "t1.a").eq(col("t2.a")))?
                    .aggregate(vec![], vec![count(Wildcard)])?
                    .select(vec![col(count(Wildcard).to_string())])?
                    .into_unoptimized_plan(),
            ))
            .gt(lit(ScalarValue::UInt8(Some(0)))),
        )?
        .select(vec![col("t1.a"), col("t1.b")])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn describe() -> Result<()> {
    let ctx = SessionContext::new();
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let describe_record_batch = ctx
        .table("alltypes_tiny_pages")
        .await?
        .describe()
        .await?
        .collect()
        .await?;

    #[rustfmt::skip]
        let expected = vec![
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
        "| describe   | id                | bool_col | tinyint_col        | smallint_col       | int_col            | bigint_col         | float_col          | double_col         | date_string_col | string_col | timestamp_col           | year               | month             |",
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
        "| count      | 7300.0            | 7300     | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300            | 7300       | 7300                    | 7300.0             | 7300.0            |",
        "| null_count | 7300.0            | 7300     | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300            | 7300       | 7300                    | 7300.0             | 7300.0            |",
        "| mean       | 3649.5            | null     | 4.5                | 4.5                | 4.5                | 45.0               | 4.949999964237213  | 45.45000000000001  | null            | null       | null                    | 2009.5             | 6.526027397260274 |",
        "| std        | 2107.472815166704 | null     | 2.8724780750809518 | 2.8724780750809518 | 2.8724780750809518 | 28.724780750809533 | 3.1597258182544645 | 29.012028558317645 | null            | null       | null                    | 0.5000342500942125 | 3.44808750051728  |",
        "| min        | 0.0               | null     | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 01/01/09        | 0          | 2008-12-31T23:00:00     | 2009.0             | 1.0               |",
        "| max        | 7299.0            | null     | 9.0                | 9.0                | 9.0                | 90.0               | 9.899999618530273  | 90.89999999999999  | 12/31/10        | 9          | 2010-12-31T04:09:13.860 | 2010.0             | 12.0              |",
        "| median     | 3649.0            | null     | 4.0                | 4.0                | 4.0                | 45.0               | 4.949999809265137  | 45.45              | null            | null       | null                    | 2009.0             | 7.0               |",
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
    ];
    assert_batches_eq!(expected, &describe_record_batch);

    //add test case for only boolean boolean/binary column
    let result = ctx
        .sql("select 'a' as a,true as b")
        .await?
        .describe()
        .await?
        .collect()
        .await?;
    #[rustfmt::skip]
        let expected = vec![
        "+------------+------+------+",
        "| describe   | a    | b    |",
        "+------------+------+------+",
        "| count      | 1    | 1    |",
        "| null_count | 1    | 1    |",
        "| mean       | null | null |",
        "| std        | null | null |",
        "| min        | a    | null |",
        "| max        | a    | null |",
        "| median     | null | null |",
        "+------------+------+------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}

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

    let ctx = SessionContext::new();

    ctx.register_batch("aa", batch1)?;

    let df1 = ctx.table("aa").await?;

    ctx.register_batch("aaa", batch2)?;

    let df2 = ctx.table("aaa").await?;

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
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("b")), false, true))])
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
async fn sort_on_distinct_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))])
        .unwrap();
    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
        let expected = vec![
        "+-----+",
        "| a   |",
        "+-----+",
        "| 100 |",
        "| 10  |",
        "| 1   |",
        "+-----+",
    ];
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn sort_on_distinct_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )?;

    // Cannot sort on a column after distinct that would add a new column
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let err = ctx
        .table("t")
        .await?
        .select(vec![col("a")])?
        .distinct()?
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("b")), false, true))])
        .unwrap_err();
    assert_eq!(err.to_string(), "Error during planning: For SELECT DISTINCT, ORDER BY expressions b must appear in select list");
    Ok(())
}

#[tokio::test]
async fn sort_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .sort(vec![col("b").sort(true, true)])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.to_string(), expected);
    Ok(())
}

#[tokio::test]
async fn group_by_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .aggregate(vec![col("b")], vec![max(col("a"))])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.to_string(), expected);
    Ok(())
}

#[tokio::test]
async fn filter_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .filter(col("b").eq(lit(1)))
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.to_string(), expected);
    Ok(())
}

#[tokio::test]
async fn select_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .select(vec![col("b")])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.to_string(), expected);
    Ok(())
}

#[tokio::test]
async fn filter_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
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
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx
        .table("t")
        .await?
        .select(vec![col("a").alias("a")])?
        .select(vec![(col("a").eq(lit(10))).alias("a")])?
        .select(vec![col("a")])?;

    let results = df.collect().await?;

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

    let df = create_test_table("test")
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(col("a"))])?
        .sort(vec![
            Expr::Sort(Sort::new(Box::new(col("a")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("b")), false, true)),
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
            Expr::Sort(Sort::new(Box::new(col("c1")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("c2")), false, true)),
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
            Expr::Sort(Sort::new(Box::new(col("c1")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("c2")), false, true)),
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
        "| e  | 5  | -22    | -11.0               |",
        "| e  | 4  | 261    | 37.285714285714285  |",
        "| e  | 3  | 192    | 48.0                |",
        "| e  | 2  | 189    | 37.8                |",
        "| e  | 1  | 227    | 75.66666666666667   |",
        "| d  |    | 458    | 25.444444444444443  |",
        "| d  | 5  | -99    | -49.5               |",
        "| d  | 4  | 162    | 54.0                |",
        "| d  | 3  | 124    | 41.333333333333336  |",
        "| d  | 2  | 328    | 109.33333333333333  |",
        "| d  | 1  | -57    | -8.142857142857142  |",
        "| c  |    | -28    | -1.3333333333333333 |",
        "| c  | 5  | 24     | 12.0                |",
        "| c  | 4  | -43    | -10.75              |",
        "| c  | 3  | 190    | 47.5                |",
        "| c  | 2  | -389   | -55.57142857142857  |",
        "| c  | 1  | 190    | 47.5                |",
        "| b  |    | -111   | -5.842105263157895  |",
        "| b  | 5  | -1     | -0.2                |",
        "| b  | 4  | -223   | -44.6               |",
        "| b  | 3  | -84    | -42.0               |",
        "| b  | 2  | 102    | 25.5                |",
        "| b  | 1  | 95     | 31.666666666666668  |",
        "| a  |    | -385   | -18.333333333333332 |",
        "| a  | 5  | -96    | -32.0               |",
        "| a  | 4  | -128   | -32.0               |",
        "| a  | 3  | -27    | -4.5                |",
        "| a  | 2  | -46    | -15.333333333333334 |",
        "| a  | 1  | -88    | -17.6               |",
        "+----+----+--------+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn join_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;
    let t1_schema = t1.schema().clone();
    let t2_schema = t2.schema().clone();

    // filter: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32) as t1.a + 1 = t2.a + 2
    let filter = Expr::eq(
        col("t1.a") + lit(3i64).cast_to(&DataType::UInt32, &t1_schema)?,
        col("t2.a") + lit(1i32).cast_to(&DataType::UInt32, &t2_schema)?,
    )
    .alias("t1.b + 1 = t2.a + 2");

    let df = t1
        .join(t2, JoinType::Inner, &[], &[], Some(filter))?
        .select(vec![
            col("t1.a"),
            col("t2.a"),
            col("t1.b"),
            col("t1.c"),
            col("t2.b"),
            col("t2.c"),
        ])?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.a, t2.a, t1.b, t1.c, t2.b, t2.c [a:UInt32, a:UInt32, b:Utf8, c:Int32, b:Utf8, c:Int32]",
        "  Inner Join: t1.a + UInt32(3) = t2.a + UInt32(1) [a:UInt32, b:Utf8, c:Int32, a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t1 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+----+----+---+----+---+---+",
        "| a  | a  | b | c  | b | c |",
        "+----+----+---+----+---+---+",
        "| 11 | 13 | c | 30 | c | 3 |",
        "| 1  | 3  | a | 10 | a | 1 |",
        "+----+----+---+----+---+---+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn right_semi_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightSemi, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;
    let expected = vec![
        "RightSemi Join: t1.a = t2.a [a:UInt32, b:Utf8, c:Int32]",
        "  Projection: t1.a [a:UInt32]",
        "    Filter: t1.c > Int32(1) [a:UInt32, c:Int32]",
        "      TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]",
        "  Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+-----+---+---+",
        "| a   | b | c |",
        "+-----+---+---+",
        "| 10  | b | 2 |",
        "| 100 | d | 4 |",
        "+-----+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn right_anti_filter_push_down() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightAnti, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;
    let expected = vec![
        "RightAnti Join: t1.a = t2.a Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]",
        "  Projection: t1.a [a:UInt32]",
        "    Filter: t1.c > Int32(1) [a:UInt32, c:Int32]",
        "      TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]",
        "  TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+----+---+---+",
        "| a  | b | c |",
        "+----+---+---+",
        "| 13 | c | 3 |",
        "| 3  | a | 1 |",
        "+----+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn unnest_columns() -> Result<()> {
    const NUM_ROWS: usize = 4;
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.collect().await?;
    let expected = vec![
        "+----------+------------------------------------------------+--------------------+",
        "| shape_id | points                                         | tags               |",
        "+----------+------------------------------------------------+--------------------+",
        "| 1        | [{x: -3, y: -4}, {x: -3, y: 6}, {x: 2, y: -2}] | [tag1]             |",
        "| 2        |                                                | [tag1, tag2]       |",
        "| 3        | [{x: -9, y: 2}, {x: -10, y: -4}]               |                    |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | [tag1, tag2, tag3] |",
        "+----------+------------------------------------------------+--------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Unnest tags
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_column("tags")?.collect().await?;
    let expected = vec![
        "+----------+------------------------------------------------+------+",
        "| shape_id | points                                         | tags |",
        "+----------+------------------------------------------------+------+",
        "| 1        | [{x: -3, y: -4}, {x: -3, y: 6}, {x: 2, y: -2}] | tag1 |",
        "| 2        |                                                | tag1 |",
        "| 2        |                                                | tag2 |",
        "| 3        | [{x: -9, y: 2}, {x: -10, y: -4}]               |      |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag1 |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag2 |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag3 |",
        "+----------+------------------------------------------------+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_column("tags")?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest points
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_column("points")?.collect().await?;
    let expected = vec![
        "+----------+-----------------+--------------------+",
        "| shape_id | points          | tags               |",
        "+----------+-----------------+--------------------+",
        "| 1        | {x: -3, y: -4}  | [tag1]             |",
        "| 1        | {x: -3, y: 6}   | [tag1]             |",
        "| 1        | {x: 2, y: -2}   | [tag1]             |",
        "| 2        |                 | [tag1, tag2]       |",
        "| 3        | {x: -10, y: -4} |                    |",
        "| 3        | {x: -9, y: 2}   |                    |",
        "| 4        | {x: -3, y: 5}   | [tag1, tag2, tag3] |",
        "| 4        | {x: 2, y: -1}   | [tag1, tag2, tag3] |",
        "+----------+-----------------+--------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for points.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_column("points")?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest both points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_column("points")?
        .unnest_column("tags")?
        .collect()
        .await?;
    let expected = vec![
        "+----------+-----------------+------+",
        "| shape_id | points          | tags |",
        "+----------+-----------------+------+",
        "| 1        | {x: -3, y: -4}  | tag1 |",
        "| 1        | {x: -3, y: 6}   | tag1 |",
        "| 1        | {x: 2, y: -2}   | tag1 |",
        "| 2        |                 | tag1 |",
        "| 2        |                 | tag2 |",
        "| 3        | {x: -10, y: -4} |      |",
        "| 3        | {x: -9, y: 2}   |      |",
        "| 4        | {x: -3, y: 5}   | tag1 |",
        "| 4        | {x: -3, y: 5}   | tag2 |",
        "| 4        | {x: -3, y: 5}   | tag3 |",
        "| 4        | {x: 2, y: -1}   | tag1 |",
        "| 4        | {x: 2, y: -1}   | tag2 |",
        "| 4        | {x: 2, y: -1}   | tag3 |",
        "+----------+-----------------+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df
        .unnest_column("points")?
        .unnest_column("tags")?
        .count()
        .await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    Ok(())
}

#[tokio::test]
async fn unnest_aggregate_columns() -> Result<()> {
    const NUM_ROWS: usize = 5;

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.select_columns(&["tags"])?.collect().await?;
    let expected = vec![
        r#"+--------------------+"#,
        r#"| tags               |"#,
        r#"+--------------------+"#,
        r#"| [tag1]             |"#,
        r#"| [tag1, tag2]       |"#,
        r#"|                    |"#,
        r#"| [tag1, tag2, tag3] |"#,
        r#"| [tag1, tag2, tag3] |"#,
        r#"+--------------------+"#,
    ];
    assert_batches_sorted_eq!(expected, &results);

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_column("tags")?
        .aggregate(vec![], vec![count(col("tags"))])?
        .collect()
        .await?;
    let expected = vec![
        r#"+--------------------+"#,
        r#"| COUNT(shapes.tags) |"#,
        r#"+--------------------+"#,
        r#"| 9                  |"#,
        r#"+--------------------+"#,
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

async fn create_test_table(name: &str) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "abcDEF",
                "abc123",
                "CBAdef",
                "123AbcDef",
            ])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch(name, batch)?;

    ctx.table(name).await
}

async fn aggregates_table(ctx: &SessionContext) -> Result<DataFrame> {
    let testdata = datafusion::test_util::arrow_test_data();

    ctx.read_csv(
        format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await
}

fn create_join_context() -> Result<SessionContext> {
    let t1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let t2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        t1,
        vec![
            Arc::new(UInt32Array::from(vec![1, 10, 11, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        t2,
        vec![
            Arc::new(UInt32Array::from(vec![3, 10, 13, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;

    Ok(ctx)
}

/// Create a data frame that contains nested types.
///
/// Create a data frame with nested types, each row contains:
/// - shape_id an integer primary key
/// - points A list of points structs {x, y}
/// - A list of tags.
async fn table_with_nested_types(n: usize) -> Result<DataFrame> {
    use rand::prelude::*;

    let mut shape_id_builder = UInt32Builder::new();
    let mut points_builder = ListBuilder::new(StructBuilder::from_fields(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ],
        5,
    ));
    let mut tags_builder = ListBuilder::new(StringBuilder::new());

    let mut rng = StdRng::seed_from_u64(197);

    for idx in 0..n {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        // Add a random number of points
        let num_points: usize = rng.gen_range(0..4);
        if num_points > 0 {
            for _ in 0..num_points.max(2) {
                // Add x value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(0)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                // Add y value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(1)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                points_builder.values().append(true);
            }
        }

        // Append null if num points is 0.
        points_builder.append(num_points > 0);

        // Append tags.
        let num_tags: usize = rng.gen_range(0..5);
        for id in 0..num_tags {
            tags_builder.values().append_value(format!("tag{}", id + 1));
        }
        tags_builder.append(num_tags > 0);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("points", Arc::new(points_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    ctx.table("shapes").await
}

pub async fn register_alltypes_tiny_pages_parquet(ctx: &SessionContext) -> Result<()> {
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(())
}
#[derive(Debug)]
struct HardcodedIntProvider {}

impl VarProvider for HardcodedIntProvider {
    fn get_value(&self, _var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(Some(1234)))
    }

    fn get_type(&self, _: &[String]) -> Option<DataType> {
        Some(DataType::Int64)
    }
}

#[tokio::test]
async fn use_var_provider() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("foo", DataType::Int64, false),
        Field::new("bar", DataType::Int64, false),
    ]));

    let mem_table = Arc::new(MemTable::try_new(schema, vec![])?);

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.optimizer.skip_failed_rules", false);
    let ctx = SessionContext::with_config(config);

    ctx.register_table("csv_table", mem_table)?;
    ctx.register_variable(VarType::UserDefined, Arc::new(HardcodedIntProvider {}));

    let dataframe = ctx
        .sql("SELECT foo FROM csv_table WHERE bar > @var")
        .await?;
    dataframe.collect().await?;
    Ok(())
}

// --------------- Begin dataframe function tests ----------------

/// Excutes an expression on the test dataframe as a select.
/// Compares formatted output of a record batch with an expected
/// vector of strings, using the assert_batch_eq! macro
macro_rules! assert_fn_batches {
    ($EXPR:expr, $EXPECTED: expr) => {
        assert_fn_batches!($EXPR, $EXPECTED, 10)
    };
    ($EXPR:expr, $EXPECTED: expr, $LIMIT: expr) => {
        let df = create_test_table("test").await?;
        let df = df.select(vec![$EXPR])?.limit(0, Some($LIMIT))?;
        let batches = df.collect().await?;

        assert_batches_eq!($EXPECTED, &batches);
    };
}

#[tokio::test]
async fn test_fn_ascii() -> Result<()> {
    let expr = ascii(col("a"));

    let expected = vec![
        "+---------------+",
        "| ascii(test.a) |",
        "+---------------+",
        "| 97            |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_bit_length() -> Result<()> {
    let expr = bit_length(col("a"));

    let expected = vec![
        "+--------------------+",
        "| bit_length(test.a) |",
        "+--------------------+",
        "| 48                 |",
        "| 48                 |",
        "| 48                 |",
        "| 72                 |",
        "+--------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim() -> Result<()> {
    let expr = btrim(vec![lit("      a b c             ")]);

    let expected = vec![
        "+-----------------------------------------+",
        "| btrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim_with_chars() -> Result<()> {
    let expr = btrim(vec![col("a"), lit("ab")]);

    let expected = vec![
        "+--------------------------+",
        "| btrim(test.a,Utf8(\"ab\")) |",
        "+--------------------------+",
        "| cDEF                     |",
        "| c123                     |",
        "| CBAdef                   |",
        "| 123AbcDef                |",
        "+--------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_approx_median() -> Result<()> {
    let expr = approx_median(col("b"));

    let expected = vec![
        "+-----------------------+",
        "| APPROX_MEDIAN(test.b) |",
        "+-----------------------+",
        "| 10                    |",
        "+-----------------------+",
    ];

    let df = create_test_table("test").await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_batches_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_fn_approx_percentile_cont() -> Result<()> {
    let expr = approx_percentile_cont(col("b"), lit(0.5));

    let expected = vec![
        "+---------------------------------------------+",
        "| APPROX_PERCENTILE_CONT(test.b,Float64(0.5)) |",
        "+---------------------------------------------+",
        "| 10                                          |",
        "+---------------------------------------------+",
    ];

    let df = create_test_table("test").await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_batches_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_character_length() -> Result<()> {
    let expr = character_length(col("a"));

    let expected = vec![
        "+--------------------------+",
        "| character_length(test.a) |",
        "+--------------------------+",
        "| 6                        |",
        "| 6                        |",
        "| 6                        |",
        "| 9                        |",
        "+--------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_chr() -> Result<()> {
    let expr = chr(lit(128175));

    let expected = vec![
        "+--------------------+",
        "| chr(Int32(128175)) |",
        "+--------------------+",
        "| 💯                 |",
        "+--------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_initcap() -> Result<()> {
    let expr = initcap(col("a"));

    let expected = vec![
        "+-----------------+",
        "| initcap(test.a) |",
        "+-----------------+",
        "| Abcdef          |",
        "| Abc123          |",
        "| Cbadef          |",
        "| 123abcdef       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_left() -> Result<()> {
    let expr = left(col("a"), lit(3));

    let expected = vec![
        "+-----------------------+",
        "| left(test.a,Int32(3)) |",
        "+-----------------------+",
        "| abc                   |",
        "| abc                   |",
        "| CBA                   |",
        "| 123                   |",
        "+-----------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_lower() -> Result<()> {
    let expr = lower(col("a"));

    let expected = vec![
        "+---------------+",
        "| lower(test.a) |",
        "+---------------+",
        "| abcdef        |",
        "| abc123        |",
        "| cbadef        |",
        "| 123abcdef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10)]);

    let expected = vec![
        "+------------------------+",
        "| lpad(test.a,Int32(10)) |",
        "+------------------------+",
        "|     abcDEF             |",
        "|     abc123             |",
        "|     CBAdef             |",
        "|  123AbcDef             |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad_with_string() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10), lit("*")]);

    let expected = vec![
        "+----------------------------------+",
        "| lpad(test.a,Int32(10),Utf8(\"*\")) |",
        "+----------------------------------+",
        "| ****abcDEF                       |",
        "| ****abc123                       |",
        "| ****CBAdef                       |",
        "| *123AbcDef                       |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim() -> Result<()> {
    let expr = ltrim(lit("      a b c             "));

    let expected = vec![
        "+-----------------------------------------+",
        "| ltrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim_with_columns() -> Result<()> {
    let expr = ltrim(col("a"));

    let expected = vec![
        "+---------------+",
        "| ltrim(test.a) |",
        "+---------------+",
        "| abcDEF        |",
        "| abc123        |",
        "| CBAdef        |",
        "| 123AbcDef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_md5() -> Result<()> {
    let expr = md5(col("a"));

    let expected = vec![
        "+----------------------------------+",
        "| md5(test.a)                      |",
        "+----------------------------------+",
        "| ea2de8bd80f3a1f52c754214fc9b0ed1 |",
        "| e99a18c428cb38d5f260853678922e03 |",
        "| 11ed4a6e9985df40913eead67f022e27 |",
        "| 8f5e60e523c9253e623ae38bb58c399a |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_match() -> Result<()> {
    let expr = regexp_match(vec![col("a"), lit("[a-z]")]);

    let expected = vec![
        "+------------------------------------+",
        "| regexp_match(test.a,Utf8(\"[a-z]\")) |",
        "+------------------------------------+",
        "| [a]                                |",
        "| [a]                                |",
        "| [d]                                |",
        "| [b]                                |",
        "+------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_replace() -> Result<()> {
    let expr = regexp_replace(vec![col("a"), lit("[a-z]"), lit("x"), lit("g")]);

    let expected = vec![
        "+----------------------------------------------------------+",
        "| regexp_replace(test.a,Utf8(\"[a-z]\"),Utf8(\"x\"),Utf8(\"g\")) |",
        "+----------------------------------------------------------+",
        "| xxxDEF                                                   |",
        "| xxx123                                                   |",
        "| CBAxxx                                                   |",
        "| 123AxxDxx                                                |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_replace() -> Result<()> {
    let expr = replace(col("a"), lit("abc"), lit("x"));

    let expected = vec![
        "+---------------------------------------+",
        "| replace(test.a,Utf8(\"abc\"),Utf8(\"x\")) |",
        "+---------------------------------------+",
        "| xDEF                                  |",
        "| x123                                  |",
        "| CBAdef                                |",
        "| 123AbcDef                             |",
        "+---------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_repeat() -> Result<()> {
    let expr = repeat(col("a"), lit(2));

    let expected = vec![
        "+-------------------------+",
        "| repeat(test.a,Int32(2)) |",
        "+-------------------------+",
        "| abcDEFabcDEF            |",
        "| abc123abc123            |",
        "| CBAdefCBAdef            |",
        "| 123AbcDef123AbcDef      |",
        "+-------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_reverse() -> Result<()> {
    let expr = reverse(col("a"));

    let expected = vec![
        "+-----------------+",
        "| reverse(test.a) |",
        "+-----------------+",
        "| FEDcba          |",
        "| 321cba          |",
        "| fedABC          |",
        "| feDcbA321       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_right() -> Result<()> {
    let expr = right(col("a"), lit(3));

    let expected = vec![
        "+------------------------+",
        "| right(test.a,Int32(3)) |",
        "+------------------------+",
        "| DEF                    |",
        "| 123                    |",
        "| def                    |",
        "| Def                    |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11)]);

    let expected = vec![
        "+------------------------+",
        "| rpad(test.a,Int32(11)) |",
        "+------------------------+",
        "| abcDEF                 |",
        "| abc123                 |",
        "| CBAdef                 |",
        "| 123AbcDef              |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad_with_characters() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11), lit("x")]);

    let expected = vec![
        "+----------------------------------+",
        "| rpad(test.a,Int32(11),Utf8(\"x\")) |",
        "+----------------------------------+",
        "| abcDEFxxxxx                      |",
        "| abc123xxxxx                      |",
        "| CBAdefxxxxx                      |",
        "| 123AbcDefxx                      |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_sha224() -> Result<()> {
    let expr = sha224(col("a"));

    let expected = vec![
        "+----------------------------------------------------------+",
        "| sha224(test.a)                                           |",
        "+----------------------------------------------------------+",
        "| 8b9ef961d2b19cfe7ee2a8452e3adeea98c7b22954b4073976bf80ee |",
        "| 5c69bb695cc29b93d655e1a4bb5656cda624080d686f74477ea09349 |",
        "| b3b3783b7470594e7ddb845eca0aec5270746dd6d0bc309bb948ceab |",
        "| fc8a30d59386d78053328440c6670c3b583404a905cbe9bbd491a517 |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_split_part() -> Result<()> {
    let expr = split_part(col("a"), lit("b"), lit(1));

    let expected = vec![
        "+---------------------------------------+",
        "| split_part(test.a,Utf8(\"b\"),Int32(1)) |",
        "+---------------------------------------+",
        "| a                                     |",
        "| a                                     |",
        "| CBAdef                                |",
        "| 123A                                  |",
        "+---------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_starts_with() -> Result<()> {
    let expr = starts_with(col("a"), lit("abc"));

    let expected = vec![
        "+---------------------------------+",
        "| starts_with(test.a,Utf8(\"abc\")) |",
        "+---------------------------------+",
        "| true                            |",
        "| true                            |",
        "| false                           |",
        "| false                           |",
        "+---------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_strpos() -> Result<()> {
    let expr = strpos(col("a"), lit("f"));

    let expected = vec![
        "+--------------------------+",
        "| strpos(test.a,Utf8(\"f\")) |",
        "+--------------------------+",
        "| 0                        |",
        "| 0                        |",
        "| 6                        |",
        "| 9                        |",
        "+--------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_substr() -> Result<()> {
    let expr = substr(col("a"), lit(2));

    let expected = vec![
        "+-------------------------+",
        "| substr(test.a,Int32(2)) |",
        "+-------------------------+",
        "| bcDEF                   |",
        "| bc123                   |",
        "| BAdef                   |",
        "| 23AbcDef                |",
        "+-------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_cast() -> Result<()> {
    let expr = cast(col("b"), DataType::Float64);
    let expected = vec![
        "+--------+",
        "| test.b |",
        "+--------+",
        "| 1.0    |",
        "| 10.0   |",
        "| 10.0   |",
        "| 100.0  |",
        "+--------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_to_hex() -> Result<()> {
    let expr = to_hex(col("b"));

    let expected = vec![
        "+----------------+",
        "| to_hex(test.b) |",
        "+----------------+",
        "| 1              |",
        "| a              |",
        "| a              |",
        "| 64             |",
        "+----------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_translate() -> Result<()> {
    let expr = translate(col("a"), lit("bc"), lit("xx"));

    let expected = vec![
        "+-----------------------------------------+",
        "| translate(test.a,Utf8(\"bc\"),Utf8(\"xx\")) |",
        "+-----------------------------------------+",
        "| axxDEF                                  |",
        "| axx123                                  |",
        "| CBAdef                                  |",
        "| 123AxxDef                               |",
        "+-----------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_upper() -> Result<()> {
    let expr = upper(col("a"));

    let expected = vec![
        "+---------------+",
        "| upper(test.a) |",
        "+---------------+",
        "| ABCDEF        |",
        "| ABC123        |",
        "| CBADEF        |",
        "| 123ABCDEF     |",
        "+---------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}
