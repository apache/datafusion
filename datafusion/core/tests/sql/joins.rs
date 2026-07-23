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

use insta::assert_snapshot;

use datafusion::assert_batches_eq;
use datafusion::catalog::MemTable;
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::physical_plan::joins::AsOfJoinExec;
use datafusion::physical_plan::{Distribution, ExecutionPlanProperties};
use datafusion::test_util::register_unbounded_file_with_ordering;
use datafusion_sql::unparser::plan_to_sql;

use super::*;

#[tokio::test]
async fn join_change_in_planner() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![
        [col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>(),
    ];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_no_order_on_filter() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
        Field::new("a3", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![
        [col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>(),
    ];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT * FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a3 > t2.a3 + 3 AND t1.a3 < t2.a3 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a3@0 AS Int64) > CAST(a3@1 AS Int64) + 3 AND CAST(a3@0 AS Int64) < CAST(a3@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1, maintains_sort_order=true
        StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;

    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema, right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
      RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=1
        StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort_not_allowed() -> Result<()> {
    let config = SessionConfig::new()
        .with_target_partitions(8)
        .with_allow_symmetric_joins_without_pruning(false);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema.clone(), right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let df = ctx.sql("SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
    match df.create_physical_plan().await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(
                e.strip_backtrace(),
                "SanityCheckPlan\ncaused by\nError during planning: Join operation cannot operate on a non-prunable stream without enabling the 'allow_symmetric_joins_without_pruning' configuration flag"
            )
        }
    }
    Ok(())
}

#[tokio::test]
async fn join_using_uppercase_column() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "UPPER",
        DataType::UInt32,
        false,
    )]));
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("uppercase-column.csv");
    let mut file = File::create(file_path.clone())?;
    file.write_all("0".as_bytes())?;
    drop(file);

    let ctx = SessionContext::new();
    ctx.register_csv(
        "test",
        file_path.to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let dataframe = ctx
        .sql(
            r#"
        SELECT test."UPPER" FROM "test"
        INNER JOIN (
            SELECT test."UPPER" FROM "test"
        ) AS selection USING ("UPPER")
        ;
        "#,
        )
        .await?;

    assert_batches_eq!(
        [
            "+-------+",
            "| UPPER |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ],
        &dataframe.collect().await?
    );

    Ok(())
}

// Issue #17359: https://github.com/apache/datafusion/issues/17359
#[tokio::test]
async fn unparse_cross_join() -> Result<()> {
    let ctx = SessionContext::new();

    let j1_schema = Arc::new(Schema::new(vec![
        Field::new("j1_id", DataType::Int32, true),
        Field::new("j1_string", DataType::Utf8, true),
    ]));
    let j2_schema = Arc::new(Schema::new(vec![
        Field::new("j2_id", DataType::Int32, true),
        Field::new("j2_string", DataType::Utf8, true),
    ]));

    ctx.register_table("j1", Arc::new(MemTable::try_new(j1_schema, vec![vec![]])?))?;
    ctx.register_table("j2", Arc::new(MemTable::try_new(j2_schema, vec![vec![]])?))?;

    let df = ctx
        .sql(
            r#"
            select j1.j1_id, j2.j2_string
            from j1, j2
            where j2.j2_id = 0
            "#,
        )
        .await?;

    let unopt_sql = plan_to_sql(df.logical_plan())?;
    assert_snapshot!(unopt_sql, @"SELECT j1.j1_id, j2.j2_string FROM j1 CROSS JOIN j2 WHERE (j2.j2_id = 0)");

    let optimized_plan = df.into_optimized_plan()?;

    let opt_sql = plan_to_sql(&optimized_plan)?;
    assert_snapshot!(opt_sql, @"SELECT j1.j1_id, j2.j2_string FROM j1 CROSS JOIN j2 WHERE (j2.j2_id = 0)");

    Ok(())
}

async fn register_asof_test_tables(ctx: &SessionContext) -> Result<()> {
    let trades_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("trade_id", DataType::Int32, false),
    ]));
    let trades = vec![
        RecordBatch::try_new(
            Arc::clone(&trades_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), None])),
                Arc::new(Int64Array::from(vec![Some(7), Some(2), Some(3)])),
                Arc::new(Int32Array::from(vec![3, 4, 6])),
            ],
        )?,
        RecordBatch::try_new(
            Arc::clone(&trades_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("A"), Some("B")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(4), Some(8)])),
                Arc::new(Int32Array::from(vec![1, 2, 5])),
            ],
        )?,
    ];
    ctx.register_table(
        "trades",
        Arc::new(MemTable::try_new(trades_schema, vec![trades])?),
    )?;

    let prices_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("price", DataType::Int32, false),
    ]));
    let prices = vec![
        RecordBatch::try_new(
            Arc::clone(&prices_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), None])),
                Arc::new(Int64Array::from(vec![Some(6), Some(1), Some(2)])),
                Arc::new(Int32Array::from(vec![60, 101, 999])),
            ],
        )?,
        RecordBatch::try_new(
            Arc::clone(&prices_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("A"), Some("B")])),
                Arc::new(Int64Array::from(vec![Some(2), Some(4), Some(6)])),
                Arc::new(Int32Array::from(vec![20, 40, 106])),
            ],
        )?,
    ];
    ctx.register_table(
        "prices",
        Arc::new(MemTable::try_new(prices_schema, vec![prices])?),
    )?;
    Ok(())
}

fn find_asof_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<AsOfJoinExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_asof_exec)
}

#[tokio::test]
async fn asof_join_all_match_directions_across_batches() -> Result<()> {
    let config = SessionConfig::new()
        .with_batch_size(2)
        .with_target_partitions(2);
    let ctx = SessionContext::new_with_config(config);
    register_asof_test_tables(&ctx).await?;

    for (op, expected) in [
        (
            ">=",
            [
                "+----------+-------+",
                "| trade_id | price |",
                "+----------+-------+",
                "| 1        |       |",
                "| 2        | 40    |",
                "| 3        | 60    |",
                "| 4        | 101   |",
                "| 5        | 106   |",
                "| 6        |       |",
                "+----------+-------+",
            ],
        ),
        (
            ">",
            [
                "+----------+-------+",
                "| trade_id | price |",
                "+----------+-------+",
                "| 1        |       |",
                "| 2        | 20    |",
                "| 3        | 60    |",
                "| 4        | 101   |",
                "| 5        | 106   |",
                "| 6        |       |",
                "+----------+-------+",
            ],
        ),
        (
            "<=",
            [
                "+----------+-------+",
                "| trade_id | price |",
                "+----------+-------+",
                "| 1        | 20    |",
                "| 2        | 40    |",
                "| 3        |       |",
                "| 4        | 106   |",
                "| 5        |       |",
                "| 6        |       |",
                "+----------+-------+",
            ],
        ),
        (
            "<",
            [
                "+----------+-------+",
                "| trade_id | price |",
                "+----------+-------+",
                "| 1        | 20    |",
                "| 2        | 60    |",
                "| 3        |       |",
                "| 4        | 106   |",
                "| 5        |       |",
                "| 6        |       |",
                "+----------+-------+",
            ],
        ),
    ] {
        let batches = ctx
            .sql(&format!(
                "SELECT t.trade_id, p.price FROM trades t \
                 ASOF JOIN prices p MATCH_CONDITION (t.ts {op} p.ts) \
                 ON t.symbol = p.symbol ORDER BY t.trade_id"
            ))
            .await?
            .collect()
            .await?;
        assert_batches_eq!(expected, &batches);
    }
    Ok(())
}

#[tokio::test]
async fn asof_join_coerces_equality_and_match_types() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = ctx
        .sql(
            "SELECT t.id, p.price \
             FROM (VALUES (CAST(1 AS INT), CAST(4 AS INT), 7)) t(k, ts, id) \
             ASOF JOIN \
             (VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT), 20)) p(k, ts, price) \
             MATCH_CONDITION (t.ts >= p.ts) ON t.k = p.k",
        )
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | price |",
            "+----+-------+",
            "| 7  | 20    |",
            "+----+-------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn asof_join_without_equality_keys_is_single_partition() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    register_asof_test_tables(&ctx).await?;
    let df = ctx
        .sql(
            "SELECT t.trade_id, p.price FROM trades t ASOF JOIN prices p \
             MATCH_CONDITION (t.ts >= p.ts)",
        )
        .await?;
    let sql = plan_to_sql(df.logical_plan())?.to_string();
    assert_contains!(sql.as_str(), "ASOF JOIN");
    assert!(!sql.contains(" ON "), "unexpected equality clause: {sql}");
    ctx.sql(&sql).await?;
    let plan = df.create_physical_plan().await?;
    let asof = find_asof_exec(&plan).expect("physical ASOF join must be present");
    assert_eq!(asof.output_partitioning().partition_count(), 1);
    assert!(asof.output_ordering().is_some());
    assert!(matches!(
        &asof.input_distribution_requirements().into_per_child()[..],
        [Distribution::SinglePartition, Distribution::SinglePartition]
    ));
    let batches = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    Ok(())
}

#[tokio::test]
async fn asof_join_explain_names_equality_and_match_conditions() -> Result<()> {
    let ctx = SessionContext::new();
    register_asof_test_tables(&ctx).await?;
    let batches = ctx
        .sql(
            "EXPLAIN SELECT t.trade_id, p.price FROM trades t \
             ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) \
             ON t.symbol = p.symbol",
        )
        .await?
        .collect()
        .await?;
    let explain = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    assert_contains!(explain.as_str(), "AsOf Join: match=[t.ts >= p.ts]");
    assert_contains!(explain.as_str(), "on=[t.symbol = p.symbol]");
    assert_contains!(explain.as_str(), "AsOfJoinExec:");
    assert_contains!(explain.as_str(), "on=[(symbol = symbol)]");
    assert_contains!(explain.as_str(), "match=[ts >= ts]");
    Ok(())
}

#[tokio::test]
async fn asof_join_rejects_unbounded_inputs_during_physical_planning() -> Result<()> {
    let ctx = SessionContext::new();
    let tmp_dir = TempDir::new()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::UInt32, false),
        Field::new("ts", DataType::UInt32, false),
    ]));
    let ordering = vec![vec![
        col("symbol").sort(true, true),
        col("ts").sort(true, true),
    ]];
    for table in ["left_stream", "right_stream"] {
        let path = tmp_dir.path().join(format!("{table}.csv"));
        File::create(&path)?;
        register_unbounded_file_with_ordering(
            &ctx,
            Arc::clone(&schema),
            &path,
            table,
            ordering.clone(),
        )?;
    }
    let error = ctx
        .sql(
            "SELECT * FROM left_stream l ASOF JOIN right_stream r \
             MATCH_CONDITION (l.ts >= r.ts) ON l.symbol = r.symbol",
        )
        .await?
        .create_physical_plan()
        .await
        .expect_err("ASOF physical planning must reject unbounded inputs");
    assert_contains!(error.to_string(), "AsOfJoinExec requires bounded inputs");
    Ok(())
}

#[tokio::test]
async fn asof_join_using_merges_key_and_unparser_round_trips() -> Result<()> {
    let ctx = SessionContext::new();
    register_asof_test_tables(&ctx).await?;
    let df = ctx
        .sql(
            "SELECT * FROM trades t ASOF JOIN prices p \
             MATCH_CONDITION (t.ts >= p.ts) USING (symbol)",
        )
        .await?;
    assert_eq!(
        df.schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["symbol", "ts", "trade_id", "ts", "price"]
    );
    let sql = plan_to_sql(df.logical_plan())?.to_string();
    assert!(sql.contains("ASOF JOIN"));
    assert!(sql.contains("MATCH_CONDITION"));
    assert!(sql.contains("USING(symbol)"), "unexpected SQL: {sql}");
    ctx.sql(&sql).await?;
    Ok(())
}

#[tokio::test]
async fn asof_join_unparser_preserves_right_preselection() -> Result<()> {
    let ctx = SessionContext::new();
    register_asof_test_tables(&ctx).await?;
    for query in [
        "SELECT t.trade_id, p.price FROM trades t \
         ASOF JOIN (SELECT * FROM prices WHERE price < 100) p \
         MATCH_CONDITION (t.ts >= p.ts) ON t.symbol = p.symbol \
         ORDER BY t.trade_id",
        "SELECT * FROM trades t \
         ASOF JOIN (SELECT * FROM prices WHERE price < 100) p \
         MATCH_CONDITION (t.ts >= p.ts) USING (symbol) \
         ORDER BY t.trade_id",
        "SELECT t.trade_id, p.price FROM trades t \
         JOIN prices q ON t.symbol = q.symbol AND t.ts = q.ts \
         ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) \
         ON q.symbol = p.symbol ORDER BY t.trade_id",
        "SELECT t.trade_id, q.trade_id FROM trades t \
         ASOF JOIN (prices p JOIN trades q \
         ON p.symbol = q.symbol AND p.ts = q.ts) \
         MATCH_CONDITION (t.ts >= q.ts) ON t.symbol = p.symbol \
         ORDER BY t.trade_id",
    ] {
        let expected = ctx.sql(query).await?.collect().await?;
        let plan = ctx.sql(query).await?.into_optimized_plan()?;
        let sql = plan_to_sql(&plan)?.to_string();
        let actual = ctx.sql(&sql).await?.collect().await?;
        assert_eq!(
            datafusion_common::test_util::batches_to_string(&expected),
            datafusion_common::test_util::batches_to_string(&actual),
            "unparsed SQL changed ASOF candidate preselection: {sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn asof_join_rejects_invalid_contracts() -> Result<()> {
    let ctx = SessionContext::new();
    register_asof_test_tables(&ctx).await?;
    for sql in [
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts = p.ts) ON t.symbol = p.symbol",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (p.ts >= t.ts) ON t.symbol = p.symbol",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) ON t.symbol > p.symbol",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (1 >= p.ts) ON t.symbol = p.symbol",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) ON 1 = 1",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts AND t.ts > p.ts) ON t.symbol = p.symbol",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "query should fail: {sql}");
    }
    Ok(())
}
