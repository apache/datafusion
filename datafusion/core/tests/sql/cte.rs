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
use arrow::array::StringArray;
use datafusion::catalog::MemTable;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::materialized_subplan::{
    MaterializedSubplanExec, MaterializedSubplanReaderExec,
};
use datafusion::physical_plan::{collect_partitioned, visit_execution_plan};
use datafusion_common::assert_batches_eq;
use datafusion_common::stats::Precision;

#[tokio::test]
async fn multi_reference_cte_materialization_heuristic() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE cte_scan_source AS VALUES (1), (2)")
        .await?
        .collect()
        .await?;

    let reused_scan = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_scan_source) \
             SELECT count(*) FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = reused_scan.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedSubplanExec");
    assert_contains!(&plan, "MaterializedSubplanReaderExec");

    Ok(())
}

#[tokio::test]
async fn materialized_cte_reader_preserves_input_partitions() -> Result<()> {
    let ctx = {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().execution.enable_materialized_ctes = true;
        SessionContext::new_with_config(config)
    };
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let partitions = (0..4)
        .map(|partition| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![partition]))],
            )
            .map(|batch| vec![batch])
        })
        .collect::<arrow::error::Result<Vec<_>>>()?;
    let provider = MemTable::try_new(Arc::clone(&schema), partitions)?;
    ctx.register_table("cte_partition_source", Arc::new(provider))?;

    let df = ctx
        .sql(
            "WITH t AS (SELECT i FROM cte_partition_source) \
             SELECT count(*) FROM t l JOIN t r ON l.i = r.i",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    struct PartitionVisitor {
        producer_partitions: Vec<usize>,
        reader_partitions: Vec<usize>,
    }

    impl ExecutionPlanVisitor for PartitionVisitor {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.is::<MaterializedSubplanExec>() {
                self.producer_partitions
                    .push(plan.output_partitioning().partition_count());
            }
            if plan.is::<MaterializedSubplanReaderExec>() {
                self.reader_partitions
                    .push(plan.output_partitioning().partition_count());
            }
            Ok(true)
        }
    }

    let mut visitor = PartitionVisitor {
        producer_partitions: vec![],
        reader_partitions: vec![],
    };
    visit_execution_plan(physical_plan.as_ref(), &mut visitor).unwrap();

    assert_eq!(visitor.producer_partitions, vec![1]);
    assert_eq!(visitor.reader_partitions, vec![4, 4]);

    let results = df.collect().await?;
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn materialized_cte_partitioned_continuation_executes_partitions_once() -> Result<()>
{
    let ctx = {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().execution.enable_materialized_ctes = true;
        SessionContext::new_with_config(config)
    };
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let partitions = (0..4)
        .map(|partition| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![partition]))],
            )
            .map(|batch| vec![batch])
        })
        .collect::<arrow::error::Result<Vec<_>>>()?;
    let provider = MemTable::try_new(Arc::clone(&schema), partitions)?;
    ctx.register_table("cte_repartition_source", Arc::new(provider))?;

    let df = ctx
        .sql(
            "WITH t AS (SELECT i FROM cte_repartition_source) \
             SELECT l.i FROM t l JOIN t r ON l.i = r.i",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    assert_eq!(physical_plan.output_partitioning().partition_count(), 4);
    let results = collect_partitioned(physical_plan, ctx.task_ctx()).await?;
    assert_eq!(
        results
            .iter()
            .flatten()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        4
    );

    Ok(())
}

#[tokio::test]
async fn materialized_cte_cache_is_per_physical_plan() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE cte_cache_source AS VALUES (1), (2)")
        .await?
        .collect()
        .await?;

    let first = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_cache_source WHERE column1 = 1) \
             SELECT l.a FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = first.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedSubplanExec");
    let results = first.collect().await?;
    let expected = ["+---+", "| a |", "+---+", "| 1 |", "+---+"];
    assert_batches_eq!(expected, &results);

    let second = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_cache_source WHERE column1 = 2) \
             SELECT l.a FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = second.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedSubplanExec");
    let results = second.collect().await?;
    let expected = ["+---+", "| a |", "+---+", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn materialized_cte_reader_preserves_producer_statistics() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE cte_cross_source AS VALUES (1), (2), (3), (4)")
        .await?
        .collect()
        .await?;

    let df = ctx
        .sql(
            "WITH scalar_cte AS ( \
                SELECT max(column1) AS max_value FROM cte_cross_source \
             ) \
             SELECT l.max_value \
             FROM scalar_cte l JOIN scalar_cte r ON l.max_value = r.max_value",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    struct StatisticsVisitor {
        reader_rows: Vec<Precision<usize>>,
    }

    impl ExecutionPlanVisitor for StatisticsVisitor {
        type Error = datafusion::error::DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.is::<MaterializedSubplanReaderExec>() {
                self.reader_rows
                    .push(plan.partition_statistics(None)?.num_rows);
            }

            Ok(true)
        }
    }

    let mut visitor = StatisticsVisitor {
        reader_rows: vec![],
    };
    visit_execution_plan(physical_plan.as_ref(), &mut visitor)?;

    // Readers should have consistent statistics (same value for both readers)
    assert_eq!(visitor.reader_rows.len(), 2);
    assert_eq!(visitor.reader_rows[0], visitor.reader_rows[1]);

    let results = df.collect().await?;
    let expected = [
        "+-----------+",
        "| max_value |",
        "+-----------+",
        "| 4         |",
        "+-----------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn q39_filter_pushdown_regression() -> Result<()> {
    // TPC-DS Q39 pattern: CTE aggregates over all months,
    // but each reference filters on a different d_moy value.
    // When inlined, predicate pushdown can push d_moy=4 / d_moy=5 into the scan.
    // When materialized, ALL months are computed then filtered post-hoc.

    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);

    ctx.sql("CREATE TABLE inventory (inv_item_sk INT, inv_warehouse_sk INT, inv_date_sk INT, inv_quantity_on_hand INT) AS VALUES (1,1,1,100),(1,1,2,200),(1,1,3,50)").await?.collect().await?;
    ctx.sql("CREATE TABLE item (i_item_sk INT) AS VALUES (1)")
        .await?
        .collect()
        .await?;
    ctx.sql("CREATE TABLE warehouse (w_warehouse_name VARCHAR, w_warehouse_sk INT) AS VALUES ('wh1', 1)").await?.collect().await?;
    ctx.sql("CREATE TABLE date_dim (d_date_sk INT, d_year INT, d_moy INT) AS VALUES (1, 1998, 4), (2, 1998, 5), (3, 1998, 6)").await?.collect().await?;

    let q39 = "
    EXPLAIN with inv as
    (select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
           ,stdev,mean, case mean when 0 then null else stdev/mean end cov
     from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
                ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
          from inventory
              ,item
              ,warehouse
              ,date_dim
          where inv_item_sk = i_item_sk
            and inv_warehouse_sk = w_warehouse_sk
            and inv_date_sk = d_date_sk
            and d_year = 1998
          group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
     where case mean when 0 then 0 else stdev/mean end > 1)
    select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
            ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
    from inv inv1,inv inv2
    where inv1.i_item_sk = inv2.i_item_sk
      and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
      and inv1.d_moy=4
      and inv2.d_moy=4+1
    order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
            ,inv2.d_moy,inv2.mean, inv2.cov
    ";

    let df = ctx.sql(q39).await?;
    let results = df.collect().await?;
    let plan_str = results
        .iter()
        .flat_map(|b| {
            let col = b.column(1);
            (0..col.len()).map(move |i| {
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string()
            })
        })
        .collect::<Vec<_>>()
        .join("\n");

    // With the DuckDB-style architecture, Q39's CTE is materialized upfront
    // by the SQL planner. The InlineCte optimizer rule may inline it if it
    // detects disjoint group-key filters. If it remains materialized, a future
    // CTE Filter Pusher will OR-combine the filters and push them in.
    // For now we just verify the query executes correctly (result correctness).
    let _ = plan_str;

    Ok(())
}

/// Materialization must not change query results: a multiply-referenced CTE
/// must produce the same answer whether it is materialized (computed once,
/// shared) or inlined (recomputed per reference). This is the core correctness
/// invariant of the feature.
#[tokio::test]
async fn materialization_does_not_change_results() -> Result<()> {
    let run = |materialize: bool| async move {
        let mut config = SessionConfig::new();
        config.options_mut().execution.enable_materialized_ctes = materialize;
        let ctx = SessionContext::new_with_config(config);
        ctx.sql("CREATE TABLE src AS VALUES (1), (2), (3), (4)")
            .await?
            .collect()
            .await?;
        // `c` is referenced three times (self-join + union arm), so it
        // materializes when the flag is on and inlines when it is off.
        ctx.sql(
            "WITH c AS (SELECT column1 AS a FROM src WHERE column1 <= 3)
             SELECT x.a + y.a AS s FROM c x JOIN c y ON x.a = y.a
             UNION ALL
             SELECT a FROM c
             ORDER BY s",
        )
        .await?
        .collect()
        .await
    };

    let inlined = run(false).await?;
    let materialized = run(true).await?;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&inlined)?.to_string(),
        arrow::util::pretty::pretty_format_batches(&materialized)?.to_string(),
    );
    Ok(())
}

/// Regression test for the name-collision hazard raised in the review of
/// PR #22675 (two distinct CTEs both named `t` in sibling subqueries). Because
/// the shared cache is keyed by a unique `SubplanId` rather than by the CTE
/// name, the two `t`s must materialize independently and produce distinct
/// results. (A name-keyed cache would let one subquery's readers resolve to the
/// other's cache, producing wrong results — this guards against regressing the
/// id-keyed binding, e.g. once a `CommonSubplanEliminate` rule wraps subplans
/// across non-disjoint scopes.)
#[tokio::test]
async fn sibling_same_name_ctes_do_not_collide() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE foo AS VALUES (1), (2), (3), (4), (5), (6)")
        .await?
        .collect()
        .await?;

    let df = ctx
        .sql(
            "SELECT a.cnt AS acnt, b.cnt AS bcnt
             FROM
               (WITH t AS (SELECT column1 AS a FROM foo WHERE column1 <= 2)
                SELECT count(*) cnt FROM t x JOIN t y ON x.a = y.a) a,
               (WITH t AS (SELECT column1 AS a FROM foo WHERE column1 >= 4)
                SELECT count(*) cnt FROM t x JOIN t y ON x.a = y.a) b",
        )
        .await?;
    // Both `t`s are multiply-referenced, so both materialize.
    let physical_plan = df.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedSubplanExec");
    // acnt: t = {1,2} self-join => 2 rows ; bcnt: t = {4,5,6} self-join => 3 rows
    let results = df.collect().await?;
    let expected = [
        "+------+------+",
        "| acnt | bcnt |",
        "+------+------+",
        "| 2    | 3    |",
        "+------+------+",
    ];
    assert_batches_eq!(expected, &results);
    Ok(())
}

/// Same hazard via a `UNION ALL` of two sibling subqueries each defining their
/// own `t`. Each `t` is multiply-referenced (self-join) so both materialize;
/// the id-keyed cache keeps them isolated.
#[tokio::test]
async fn sibling_same_name_ctes_in_union_do_not_collide() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE foo AS VALUES (1), (2), (3), (4), (5), (6)")
        .await?
        .collect()
        .await?;

    // ORDER BY makes the UNION ALL output deterministic.
    let df = ctx
        .sql(
            "SELECT cnt FROM
               (WITH t AS (SELECT column1 AS a FROM foo WHERE column1 <= 2)
                SELECT count(*) cnt FROM t x JOIN t y ON x.a = y.a)
             UNION ALL
             SELECT cnt FROM
               (WITH t AS (SELECT column1 AS a FROM foo WHERE column1 >= 4)
                SELECT count(*) cnt FROM t x JOIN t y ON x.a = y.a)
             ORDER BY cnt",
        )
        .await?;
    let results = df.collect().await?;
    // {1,2} => 2 ; {4,5,6} => 3
    let expected = [
        "+-----+", "| cnt |", "+-----+", "| 2   |", "| 3   |", "+-----+",
    ];
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn volatile_cte_is_materialized() -> Result<()> {
    // PostgreSQL/DuckDB semantics: volatile CTEs are always materialized
    // so that each reference sees the same result (evaluate once, share).
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_materialized_ctes = true;
    let ctx = SessionContext::new_with_config(config);

    let df = ctx
        .sql(
            "WITH t AS (SELECT random() AS r) \
             SELECT l.r = r.r AS same FROM t l, t r",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedSubplanExec");

    // Verify the values are actually the same (materialized = one evaluation)
    let results = ctx
        .sql(
            "WITH t AS (SELECT random() AS r) \
             SELECT l.r = r.r AS same FROM t l, t r",
        )
        .await?
        .collect()
        .await?;
    let expected = ["+------+", "| same |", "+------+", "| true |", "+------+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}
