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
use ::parquet::arrow::arrow_writer::ArrowWriter;
use ::parquet::file::properties::WriterProperties;
use datafusion::execution::options::ReadOptions;

#[tokio::test]
async fn window_frame_creation_type_checking() -> Result<()> {
    // The following qcargo fmtuery has type error. We should test the error could be detected
    // from either the logical plan (when `skip_failed_rules` is set to `false`) or
    // the physical plan (when `skip_failed_rules` is set to `true`).

    // We should remove the type checking in physical plan after we don't skip
    // the failed optimizing rules by default. (see more in https://github.com/apache/arrow-datafusion/issues/4615)
    async fn check_query(skip_failed_rules: bool, err_msg: &str) -> Result<()> {
        use datafusion_common::ScalarValue::Boolean;
        let config = SessionConfig::new().set(
            "datafusion.optimizer.skip_failed_rules",
            Boolean(Some(skip_failed_rules)),
        );
        let ctx = SessionContext::with_config(config);
        register_aggregate_csv(&ctx).await?;
        let df = ctx
            .sql(
                "SELECT
                    COUNT(c1) OVER (ORDER BY c2 RANGE BETWEEN '1 DAY' PRECEDING AND '2 DAY' FOLLOWING)
                    FROM aggregate_test_100;",
            )
            .await?;
        let results = df.collect().await;
        assert_contains!(results.err().unwrap().to_string(), err_msg);
        Ok(())
    }

    // Error is returned from the physical plan.
    check_query(
        true,
        "Internal error: Operator - is not implemented for types UInt32(1) and Utf8(\"1 DAY\")."
    ).await?;

    // Error is returned from the logical plan.
    check_query(
        false,
        "Internal error: Optimizer rule 'type_coercion' failed due to unexpected error: Execution error: Cannot cast Utf8(\"1 DAY\") to UInt32."
    ).await
}

#[tokio::test]
async fn test_window_agg_sort() -> Result<()> {
    // We need to specify the target partition number.
    // Otherwise, the default value used may vary on different environment
    // with different cpu core number, which may cause the UT failure.
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
      c9,
      SUM(c9) OVER(ORDER BY c9) as sum1,
      SUM(c9) OVER(ORDER BY c9, c8) as sum2
      FROM aggregate_test_100";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@1 as c9, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as sum1, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST, aggregate_test_100.c8 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as sum2]",
            "  BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "      SortExec: expr=[c9@1 ASC NULLS LAST,c8@0 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual_trim_last:#?}\n\n"
    );
    Ok(())
}

#[tokio::test]
async fn over_order_by_sort_keys_sorting_prefix_compacting() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT c2, MAX(c9) OVER (ORDER BY c2), SUM(c9) OVER (), MIN(c9) OVER (ORDER BY c2, c9) from aggregate_test_100";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c2@0 as c2, MAX(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as MAX(aggregate_test_100.c9), SUM(aggregate_test_100.c9) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@4 as SUM(aggregate_test_100.c9), MIN(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c2 ASC NULLS LAST, aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as MIN(aggregate_test_100.c9)]",
            "  WindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)) }]",
            "    BoundedWindowAggExec: wdw=[MAX(aggregate_test_100.c9): Ok(Field { name: \"MAX(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "      BoundedWindowAggExec: wdw=[MIN(aggregate_test_100.c9): Ok(Field { name: \"MIN(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "        SortExec: expr=[c2@0 ASC NULLS LAST,c9@1 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual_trim_last:#?}\n\n"
    );
    Ok(())
}

/// FIXME: for now we are not detecting prefix of sorting keys in order to re-arrange with global and save one SortExec
#[tokio::test]
async fn over_order_by_sort_keys_sorting_global_order_compacting() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT c2, MAX(c9) OVER (ORDER BY c9, c2), SUM(c9) OVER (), MIN(c9) OVER (ORDER BY c2, c9) from aggregate_test_100 ORDER BY c2";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // 3 SortExec are added
    let expected = {
        vec![
            "SortExec: expr=[c2@0 ASC NULLS LAST]",
            "  ProjectionExec: expr=[c2@0 as c2, MAX(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST, aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as MAX(aggregate_test_100.c9), SUM(aggregate_test_100.c9) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@4 as SUM(aggregate_test_100.c9), MIN(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c2 ASC NULLS LAST, aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as MIN(aggregate_test_100.c9)]",
            "    WindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)) }]",
            "      BoundedWindowAggExec: wdw=[MAX(aggregate_test_100.c9): Ok(Field { name: \"MAX(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "        SortExec: expr=[c9@1 ASC NULLS LAST,c2@0 ASC NULLS LAST]",
            "          BoundedWindowAggExec: wdw=[MIN(aggregate_test_100.c9): Ok(Field { name: \"MIN(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "            SortExec: expr=[c2@0 ASC NULLS LAST,c9@1 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual_trim_last:#?}\n\n"
    );
    Ok(())
}

#[tokio::test]
async fn test_window_partition_by_order_by() -> Result<()> {
    let ctx = SessionContext::with_config(
        SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(4096),
    );
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT \
               SUM(c4) OVER(PARTITION BY c1, c2 ORDER BY c2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               COUNT(*) OVER(PARTITION BY c1 ORDER BY c2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
               FROM aggregate_test_100";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let expected = {
        vec![
            "ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1, aggregate_test_100.c2] ORDER BY [aggregate_test_100.c2 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@3 as SUM(aggregate_test_100.c4), COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c2 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@4 as COUNT(UInt8(1))]",
            "  BoundedWindowAggExec: wdw=[COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }]",
            "    SortExec: expr=[c1@0 ASC NULLS LAST,c2@1 ASC NULLS LAST]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
            "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }]",
            "            SortExec: expr=[c1@0 ASC NULLS LAST,c2@1 ASC NULLS LAST]",
            "              CoalesceBatchesExec: target_batch_size=4096",
            "                RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }, Column { name: \"c2\", index: 1 }], 2), input_partitions=2",
            "                  RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual_trim_last:#?}\n\n"
    );
    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_reversed_plan() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    SUM(c9) OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1,
    SUM(c9) OVER(ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@0 as c9, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@2 as sum1, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@1 as sum2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "        SortExec: expr=[c9@0 DESC]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+-------------+-------------+",
        "| c9         | sum1        | sum2        |",
        "+------------+-------------+-------------+",
        "| 4268716378 | 8498370520  | 24997484146 |",
        "| 4229654142 | 12714811027 | 29012926487 |",
        "| 4216440507 | 16858984380 | 28743001064 |",
        "| 4144173353 | 20935849039 | 28472563256 |",
        "| 4076864659 | 24997484146 | 28118515915 |",
        "+------------+-------------+-------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_reversed_plan_builtin() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    FIRST_VALUE(c9) OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as fv1,
    FIRST_VALUE(c9) OVER(ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as fv2,
    LAG(c9, 2, 10101) OVER(ORDER BY c9 ASC) as lag1,
    LAG(c9, 2, 10101) OVER(ORDER BY c9 DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lag2,
    LEAD(c9, 2, 10101) OVER(ORDER BY c9 ASC) as lead1,
    LEAD(c9, 2, 10101) OVER(ORDER BY c9 DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lead2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@0 as c9, FIRST_VALUE(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@4 as fv1, FIRST_VALUE(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@1 as fv2, LAG(aggregate_test_100.c9,Int64(2),Int64(10101)) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as lag1, LAG(aggregate_test_100.c9,Int64(2),Int64(10101)) ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@2 as lag2, LEAD(aggregate_test_100.c9,Int64(2),Int64(10101)) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@6 as lead1, LEAD(aggregate_test_100.c9,Int64(2),Int64(10101)) ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@3 as lead2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[FIRST_VALUE(aggregate_test_100.c9): Ok(Field { name: \"FIRST_VALUE(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }, LAG(aggregate_test_100.c9,Int64(2),Int64(10101)): Ok(Field { name: \"LAG(aggregate_test_100.c9,Int64(2),Int64(10101))\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(UInt32(NULL)) }, LEAD(aggregate_test_100.c9,Int64(2),Int64(10101)): Ok(Field { name: \"LEAD(aggregate_test_100.c9,Int64(2),Int64(10101))\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(UInt32(NULL)) }]",
            "      BoundedWindowAggExec: wdw=[FIRST_VALUE(aggregate_test_100.c9): Ok(Field { name: \"FIRST_VALUE(aggregate_test_100.c9)\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }, LAG(aggregate_test_100.c9,Int64(2),Int64(10101)): Ok(Field { name: \"LAG(aggregate_test_100.c9,Int64(2),Int64(10101))\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LEAD(aggregate_test_100.c9,Int64(2),Int64(10101)): Ok(Field { name: \"LEAD(aggregate_test_100.c9,Int64(2),Int64(10101))\", data_type: UInt32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }]",
            "        SortExec: expr=[c9@0 DESC]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+------------+------------+------------+------------+------------+------------+",
        "| c9         | fv1        | fv2        | lag1       | lag2       | lead1      | lead2      |",
        "+------------+------------+------------+------------+------------+------------+------------+",
        "| 4268716378 | 4229654142 | 4268716378 | 4216440507 | 10101      | 10101      | 4216440507 |",
        "| 4229654142 | 4216440507 | 4268716378 | 4144173353 | 10101      | 10101      | 4144173353 |",
        "| 4216440507 | 4144173353 | 4229654142 | 4076864659 | 4268716378 | 4268716378 | 4076864659 |",
        "| 4144173353 | 4076864659 | 4216440507 | 4061635107 | 4229654142 | 4229654142 | 4061635107 |",
        "| 4076864659 | 4061635107 | 4144173353 | 4015442341 | 4216440507 | 4216440507 | 4015442341 |",
        "+------------+------------+------------+------------+------------+------------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_non_reversed_plan() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    ROW_NUMBER() OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as rn1,
    ROW_NUMBER() OVER(ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as rn2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // We cannot reverse each window function (ROW_NUMBER is not reversible)
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@0 as c9, ROW_NUMBER() ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@2 as rn1, ROW_NUMBER() ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@1 as rn2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "      SortExec: expr=[c9@0 ASC NULLS LAST]",
            "        BoundedWindowAggExec: wdw=[ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "          SortExec: expr=[c9@0 DESC]"
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+-----+-----+",
        "| c9        | rn1 | rn2 |",
        "+-----------+-----+-----+",
        "| 28774375  | 1   | 100 |",
        "| 63044568  | 2   | 99  |",
        "| 141047417 | 3   | 98  |",
        "| 141680161 | 4   | 97  |",
        "| 145294611 | 5   | 96  |",
        "+-----------+-----+-----+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_multi_layer_non_reversed_plan() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    SUM(c9) OVER(ORDER BY c9 ASC, c1 ASC, c2 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1,
    SUM(c9) OVER(ORDER BY c9 DESC, c1 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2,
    ROW_NUMBER() OVER(ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as rn2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // We cannot reverse each window function (ROW_NUMBER is not reversible)
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@2 as c9, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST, aggregate_test_100.c1 ASC NULLS LAST, aggregate_test_100.c2 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@5 as sum1, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST, aggregate_test_100.c1 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@3 as sum2, ROW_NUMBER() ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@4 as rn2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "      SortExec: expr=[c9@2 ASC NULLS LAST,c1@0 ASC NULLS LAST,c2@1 ASC NULLS LAST]",
            "        BoundedWindowAggExec: wdw=[ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "            SortExec: expr=[c9@2 DESC,c1@0 DESC]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+------------+-----------+-----+",
        "| c9        | sum1       | sum2      | rn2 |",
        "+-----------+------------+-----------+-----+",
        "| 28774375  | 745354217  | 91818943  | 100 |",
        "| 63044568  | 988558066  | 232866360 | 99  |",
        "| 141047417 | 1285934966 | 374546521 | 98  |",
        "| 141680161 | 1654839259 | 519841132 | 97  |",
        "| 145294611 | 1980231675 | 745354217 | 96  |",
        "+-----------+------------+-----------+-----+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_partition_by_set() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(1));
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    SUM(c9) OVER(PARTITION BY c1, c2 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1,
    SUM(c9) OVER(PARTITION BY c2, c1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2
    FROM aggregate_test_100
    ORDER BY c9
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // We cannot reverse each window function (ROW_NUMBER is not reversible)
    let expected = {
        vec![
            "GlobalLimitExec: skip=0, fetch=5",
            "  SortExec: fetch=5, expr=[c9@0 ASC NULLS LAST]",
            "    ProjectionExec: expr=[c9@2 as c9, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c1, aggregate_test_100.c2] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@3 as sum1, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c2, aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@4 as sum2]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "        BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "          SortExec: expr=[c1@0 ASC NULLS LAST,c2@1 ASC NULLS LAST,c9@2 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+------------+------------+",
        "| c9        | sum1       | sum2       |",
        "+-----------+------------+------------+",
        "| 28774375  | 9144476174 | 9144476174 |",
        "| 63044568  | 5125627947 | 5125627947 |",
        "| 141047417 | 3650978969 | 3650978969 |",
        "| 141680161 | 8526017165 | 8526017165 |",
        "| 145294611 | 6802765992 | 6802765992 |",
        "+-----------+------------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_complex_plan() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(2));
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as a,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as b,
    SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as c,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as d,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as e,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as f,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as g,
    SUM(c1) OVER (ORDER BY c3) as h,
    SUM(c1) OVER (ORDER BY c3 DESC) as i,
    SUM(c1) OVER (ORDER BY c3 NULLS first) as j,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS first) as k,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS last) as l,
    SUM(c1) OVER (ORDER BY c3, c2) as m,
    SUM(c1) OVER (ORDER BY c3, c1 DESC) as n,
    SUM(c1) OVER (ORDER BY c3 DESC, c1) as o,
    SUM(c1) OVER (ORDER BY c3, c1 NULLs first) as p,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as a1,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as b1,
    SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as c1,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as d1,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as e1,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as f1,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as g1,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as h1,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as j1,
    SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as k1,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as l1,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as m1,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as n1,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as o1,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as h11,
    SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as j11,
    SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as k11,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as l11,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as m11,
    SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as n11,
    SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as o11
    FROM null_cases
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Unnecessary SortExecs are removed
    let expected = {
        vec![
            "ProjectionExec: expr=[SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@19 as a, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@19 as b, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@4 as c, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@12 as d, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@8 as e, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@4 as f, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING@12 as g, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@20 as h, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as i, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@13 as j, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as k, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@9 as l, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST, null_cases.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@18 as m, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST, null_cases.c1 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@16 as n, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST, null_cases.c1 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as o, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST, null_cases.c1 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@17 as p, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@21 as a1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@21 as b1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@6 as c1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@14 as d1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@10 as e1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@6 as f1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING@14 as g1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@20 as h1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@20 as j1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as k1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@13 as l1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@9 as m1, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as n1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@13 as o1, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@22 as h11, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS LAST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@22 as j11, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@7 as k11, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@15 as l11, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS LAST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@11 as m11, SUM(null_cases.c1) ORDER BY [null_cases.c3 DESC NULLS FIRST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@7 as n11, SUM(null_cases.c1) ORDER BY [null_cases.c3 ASC NULLS FIRST] RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING@15 as o11]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    WindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(10)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int64(NULL)) }]",
            "      BoundedWindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }]",
            "        SortExec: expr=[c3@2 ASC NULLS LAST,c2@1 ASC NULLS LAST]",
            "          BoundedWindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }]",
            "            SortExec: expr=[c3@2 ASC NULLS LAST,c1@0 ASC]",
            "              WindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int64(NULL)) }]",
            "                WindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(11)), end_bound: Following(Int64(10)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int64(NULL)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(11)), end_bound: Following(Int64(NULL)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }]",
            "                  WindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(10)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int64(NULL)) }]",
            "                    WindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(10)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: Following(Int64(11)) }, SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int64(NULL)) }]",
            "                      BoundedWindowAggExec: wdw=[SUM(null_cases.c1): Ok(Field { name: \"SUM(null_cases.c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int64(NULL)), end_bound: CurrentRow }]",
            "                        SortExec: expr=[c3@2 DESC,c1@0 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_orderby_reversed_partitionby_plan() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(2);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    SUM(c9) OVER(ORDER BY c1, c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1,
    SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@1 as c9, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c1 ASC NULLS LAST, aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@2 as sum1, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@3 as sum2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "        SortExec: expr=[c1@0 ASC NULLS LAST,c9@1 DESC]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+-------------+-------------+",
        "| c9         | sum1        | sum2        |",
        "+------------+-------------+-------------+",
        "| 4015442341 | 21907044499 | 21907044499 |",
        "| 3998790955 | 24576419362 | 24576419362 |",
        "| 3959216334 | 23063303501 | 23063303501 |",
        "| 3717551163 | 21560567246 | 21560567246 |",
        "| 3276123488 | 19815386638 | 19815386638 |",
        "+------------+-------------+-------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_partitionby_reversed_plan() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(2);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
    c9,
    SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1,
    SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c9@1 as c9, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@3 as sum1, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@2 as sum2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "        SortExec: expr=[c1@0 ASC NULLS LAST,c9@1 DESC]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+-------------+-------------+",
        "| c9         | sum1        | sum2        |",
        "+------------+-------------+-------------+",
        "| 4015442341 | 8014233296  | 21907044499 |",
        "| 3998790955 | 11973449630 | 24576419362 |",
        "| 3959216334 | 15691000793 | 23063303501 |",
        "| 3717551163 | 18967124281 | 21560567246 |",
        "| 3276123488 | 21907044499 | 19815386638 |",
        "+------------+-------------+-------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_orderby_reversed_binary_expr() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(2);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c3,
    SUM(c9) OVER(ORDER BY c3+c4 DESC, c9 DESC, c2 ASC) as sum1,
    SUM(c9) OVER(ORDER BY c3+c4 ASC, c9 ASC ) as sum2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c3@1 as c3, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 DESC NULLS FIRST, aggregate_test_100.c9 DESC NULLS FIRST, aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@4 as sum1, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 ASC NULLS LAST, aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 as sum2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    WindowAggExec: wdw=[SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 ASC NULLS LAST, aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Ok(Field { name: \"SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 ASC NULLS LAST, aggregate_test_100.c9 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(Int16(NULL)) }]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 DESC NULLS FIRST, aggregate_test_100.c9 DESC NULLS FIRST, aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Ok(Field { name: \"SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 + aggregate_test_100.c4 DESC NULLS FIRST, aggregate_test_100.c9 DESC NULLS FIRST, aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int16(NULL)), end_bound: CurrentRow }]",
            "        SortExec: expr=[CAST(c3@1 AS Int16) + c4@2 DESC,c9@3 DESC,c2@0 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+-------------+--------------+",
        "| c3  | sum1        | sum2         |",
        "+-----+-------------+--------------+",
        "| -86 | 2861911482  | 222089770060 |",
        "| 13  | 5075947208  | 219227858578 |",
        "| 125 | 8701233618  | 217013822852 |",
        "| 123 | 11293564174 | 213388536442 |",
        "| 97  | 14767488750 | 210796205886 |",
        "+-----+-------------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort_in_sub_query() -> Result<()> {
    let config = SessionConfig::new()
        .with_target_partitions(8)
        .with_batch_size(4096)
        .with_repartition_windows(true);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT count(*) as global_count FROM
                 (SELECT count(*), c1
                  FROM aggregate_test_100
                  WHERE c13 != 'C2GT5KVyOPZpgKVl110TyZO0NcJ434'
                  GROUP BY c1
                  ORDER BY c1 ) AS a ";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Unnecessary Sort in the sub query is removed
    let expected = {
        vec![
            "ProjectionExec: expr=[COUNT(UInt8(1))@0 as global_count]",
            "  AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]",
            "    CoalescePartitionsExec",
            "      AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=8",
            "          AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[COUNT(UInt8(1))]",
            "            CoalesceBatchesExec: target_batch_size=4096",
            "              RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 8), input_partitions=8",
            "                AggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[COUNT(UInt8(1))]",
            "                  CoalesceBatchesExec: target_batch_size=4096",
            "                    FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
            "                      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| global_count |",
        "+--------------+",
        "| 5            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_sort_orderby_reversed_partitionby_reversed_plan() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(2);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c3,
    SUM(c9) OVER(ORDER BY c3 DESC, c9 DESC, c2 ASC) as sum1,
    SUM(c9) OVER(PARTITION BY c3 ORDER BY c9 DESC ) as sum2
    FROM aggregate_test_100
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[c3@1 as c3, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c3 DESC NULLS FIRST, aggregate_test_100.c9 DESC NULLS FIRST, aggregate_test_100.c2 ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as sum1, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c3] ORDER BY [aggregate_test_100.c9 DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@4 as sum2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt32(NULL)), end_bound: CurrentRow }]",
            "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int8(NULL)), end_bound: CurrentRow }]",
            "        SortExec: expr=[c3@1 DESC,c9@2 DESC,c2@0 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+-------------+------------+",
        "| c3  | sum1        | sum2       |",
        "+-----+-------------+------------+",
        "| 125 | 3625286410  | 3625286410 |",
        "| 123 | 7192027599  | 3566741189 |",
        "| 123 | 9784358155  | 6159071745 |",
        "| 122 | 13845993262 | 4061635107 |",
        "| 120 | 16676974334 | 2830981072 |",
        "+-----+-------------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_global_sort() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(true)
        .with_target_partitions(2)
        .with_repartition_sorts(true);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, ROW_NUMBER() OVER (PARTITION BY c1) as rn1 FROM aggregate_test_100 ORDER BY c1 ASC";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "SortPreservingMergeExec: [c1@0 ASC NULLS LAST]",
            "  ProjectionExec: expr=[c1@0 as c1, ROW_NUMBER() PARTITION BY [aggregate_test_100.c1] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@1 as rn1]",
            "    BoundedWindowAggExec: wdw=[ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)) }]",
            "      SortExec: expr=[c1@0 ASC NULLS LAST]",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
            "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn test_window_agg_global_sort_parallelize_sort_disabled() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(true)
        .with_target_partitions(2)
        .with_repartition_sorts(false);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, ROW_NUMBER() OVER (PARTITION BY c1) as rn1 FROM aggregate_test_100 ORDER BY c1 ASC";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "SortExec: expr=[c1@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    ProjectionExec: expr=[c1@0 as c1, ROW_NUMBER() PARTITION BY [aggregate_test_100.c1] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@1 as rn1]",
            "      BoundedWindowAggExec: wdw=[ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)) }]",
            "        SortExec: expr=[c1@0 ASC NULLS LAST]",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
            "              RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn test_window_agg_global_sort_intermediate_parallel_sort() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(true)
        .with_target_partitions(2)
        .with_repartition_sorts(true);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, \
    SUM(C9) OVER (PARTITION BY C1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 3 FOLLOWING) as sum1, \
    SUM(C9) OVER (ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum2 \
    FROM aggregate_test_100 ORDER BY c1 ASC";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "SortExec: expr=[c1@0 ASC NULLS LAST]",
            "  ProjectionExec: expr=[c1@0 as c1, SUM(aggregate_test_100.c9) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 3 FOLLOWING@2 as sum1, SUM(aggregate_test_100.c9) ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@3 as sum2]",
            "    BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(5)) }]",
            "      SortPreservingMergeExec: [c9@1 ASC NULLS LAST]",
            "        SortExec: expr=[c9@1 ASC NULLS LAST]",
            "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c9): Ok(Field { name: \"SUM(aggregate_test_100.c9)\", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(3)) }]",
            "            SortExec: expr=[c1@0 ASC NULLS LAST,c9@1 ASC NULLS LAST]",
            "              CoalesceBatchesExec: target_batch_size=8192",
            "                RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
            "                  RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn test_window_agg_with_global_limit() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(1);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT ARRAY_AGG(c13) as array_agg1 FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 1)";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    // Only 1 SortExec was added
    let expected = {
        vec![
            "ProjectionExec: expr=[ARRAYAGG(aggregate_test_100.c13)@0 as array_agg1]",
            "  AggregateExec: mode=Final, gby=[], aggr=[ARRAYAGG(aggregate_test_100.c13)]",
            "    AggregateExec: mode=Partial, gby=[], aggr=[ARRAYAGG(aggregate_test_100.c13)]",
            "      GlobalLimitExec: skip=0, fetch=1",
            "        SortExec: fetch=1, expr=[c13@0 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| array_agg1                       |",
        "+----------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm] |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_window_agg_low_cardinality() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(32);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
        SUM(c4) OVER(PARTITION BY c4 ORDER BY c3 GROUPS BETWEEN 1 PRECEDING AND 3 FOLLOWING) as summation1,
        SUM(c5) OVER(PARTITION BY c4 ORDER BY c4 GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation2
    FROM aggregate_test_100
    ORDER BY c9
    LIMIT 5";

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+-------------+",
        "| summation1 | summation2  |",
        "+------------+-------------+",
        "| -16110     | 61035129    |",
        "| 3917       | -108973366  |",
        "| -16974     | 623103518   |",
        "| -1114      | -1927628110 |",
        "| 15673      | -1899175111 |",
        "+------------+-------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_source_linear_partition_by() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT SUM(c5) OVER(ORDER BY c1, c2, c5 ROWS BETWEEN 4 PRECEDING AND 1 FOLLOWING) as summation1,
    SUM(c5) OVER(PARTITION BY c1, c3 ORDER BY c1, c2 ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) as summation2
    FROM aggregate_test_100
    ORDER BY c9
    LIMIT 5";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let expected = {
        vec![
            "ProjectionExec: expr=[summation1@0 as summation1, summation2@1 as summation2]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    SortExec: fetch=5, expr=[c9@2 ASC NULLS LAST]",
            "      ProjectionExec: expr=[SUM(aggregate_test_100.c5) ORDER BY [aggregate_test_100.c1 ASC NULLS LAST, aggregate_test_100.c2 ASC NULLS LAST, aggregate_test_100.c5 ASC NULLS LAST] ROWS BETWEEN 4 PRECEDING AND 1 FOLLOWING@5 as summation1, SUM(aggregate_test_100.c5) PARTITION BY [aggregate_test_100.c1, aggregate_test_100.c3] ORDER BY [aggregate_test_100.c1 ASC NULLS LAST, aggregate_test_100.c2 ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING@6 as summation2, c9@4 as c9]",
            "        BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c5): Ok(Field { name: \"SUM(aggregate_test_100.c5)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(3)), end_bound: Following(UInt64(1)) }]",
            "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c5): Ok(Field { name: \"SUM(aggregate_test_100.c5)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(4)), end_bound: Following(UInt64(1)) }]",
            "            SortExec: expr=[c1@0 ASC NULLS LAST,c2@1 ASC NULLS LAST,c5@3 ASC NULLS LAST]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------+-------------+",
        "| summation1  | summation2  |",
        "+-------------+-------------+",
        "| 3808603231  | 61035129    |",
        "| 3842254278  | -108973366  |",
        "| 1674385152  | 623103518   |",
        "| -4505110804 | -1927628110 |",
        "| 4688570752  | -1899175111 |",
        "+-------------+-------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

fn write_test_data_to_parquet(tmpdir: &TempDir, n_file: usize) -> Result<()> {
    let ts_field = Field::new("ts", DataType::Int32, false);
    let inc_field = Field::new("inc_col", DataType::Int32, false);
    let desc_field = Field::new("desc_col", DataType::Int32, false);
    let low_card_field = Field::new("low_card_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![
        ts_field,
        inc_field,
        desc_field,
        low_card_field,
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                1, 1, 5, 9, 10, 11, 16, 21, 22, 26, 26, 28, 31, 33, 38, 42, 47, 51, 53,
                53, 58, 63, 67, 68, 70, 72, 72, 76, 81, 85, 86, 88, 91, 96, 97, 98, 100,
                101, 102, 104, 104, 108, 112, 113, 113, 114, 114, 117, 122, 126, 131,
                131, 136, 136, 136, 139, 141, 146, 147, 147, 152, 154, 159, 161, 163,
                164, 167, 172, 173, 177, 180, 185, 186, 191, 195, 195, 199, 203, 207,
                210, 213, 218, 221, 224, 226, 230, 232, 235, 238, 238, 239, 244, 245,
                247, 250, 254, 258, 262, 264, 264,
            ])),
            Arc::new(Int32Array::from_slice([
                1, 5, 10, 15, 20, 21, 26, 29, 30, 33, 37, 40, 43, 44, 45, 49, 51, 53, 58,
                61, 65, 70, 75, 78, 83, 88, 90, 91, 95, 97, 100, 105, 109, 111, 115, 119,
                120, 124, 126, 129, 131, 135, 140, 143, 144, 147, 148, 149, 151, 155,
                156, 159, 160, 163, 165, 170, 172, 177, 181, 182, 186, 187, 192, 196,
                197, 199, 203, 207, 209, 213, 214, 216, 219, 221, 222, 225, 226, 231,
                236, 237, 242, 245, 247, 248, 253, 254, 259, 261, 266, 269, 272, 275,
                278, 283, 286, 289, 291, 296, 301, 305,
            ])),
            Arc::new(Int32Array::from_slice([
                100, 98, 93, 91, 86, 84, 81, 77, 75, 71, 70, 69, 64, 62, 59, 55, 50, 45,
                41, 40, 39, 36, 31, 28, 23, 22, 17, 13, 10, 6, 5, 2, 1, -1, -4, -5, -6,
                -8, -12, -16, -17, -19, -24, -25, -29, -34, -37, -42, -47, -48, -49, -53,
                -57, -58, -61, -65, -67, -68, -71, -73, -75, -76, -78, -83, -87, -91,
                -95, -98, -101, -105, -106, -111, -114, -116, -120, -125, -128, -129,
                -134, -139, -142, -143, -146, -150, -154, -158, -163, -168, -172, -176,
                -181, -184, -189, -193, -196, -201, -203, -208, -210, -213,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 3, 4, 3, 2, 2, 0, 1, 1, 3, 4, 1, 0, 1, 3, 2, 1, 4, 4, 1, 4, 3, 4, 4,
                0, 4, 1, 2, 4, 3, 2, 1, 4, 2, 2, 4, 2, 4, 1, 0, 1, 1, 4, 4, 2, 1, 4, 1,
                4, 1, 1, 3, 4, 4, 3, 0, 4, 0, 3, 0, 4, 4, 0, 0, 4, 3, 1, 0, 3, 3, 1, 1,
                0, 4, 3, 3, 0, 0, 4, 2, 1, 3, 2, 2, 1, 3, 1, 4, 1, 3, 3, 1, 2, 0, 1, 0,
                3, 2, 1, 2,
            ])),
        ],
    )?;
    let n_chunk = batch.num_rows() / n_file;
    for i in 0..n_file {
        let target_file = tmpdir.path().join(format!("{i}.parquet"));
        let file = File::create(target_file).unwrap();
        // Default writer properties
        let props = WriterProperties::builder().build();
        let chunks_start = i * n_chunk;
        let cur_batch = batch.slice(chunks_start, n_chunk);
        // let chunks_end = chunks_start + n_chunk;
        let mut writer =
            ArrowWriter::try_new(file, cur_batch.schema(), Some(props)).unwrap();

        writer.write(&cur_batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();
    }
    Ok(())
}

async fn get_test_context(tmpdir: &TempDir) -> Result<SessionContext> {
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(session_config);

    let parquet_read_options = ParquetReadOptions::default();
    // The sort order is specified (not actually correct in this case)
    let file_sort_order = [col("ts")]
        .into_iter()
        .map(|e| {
            let ascending = true;
            let nulls_first = false;
            e.sort(ascending, nulls_first)
        })
        .collect::<Vec<_>>();

    let options_sort = parquet_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order));

    write_test_data_to_parquet(tmpdir, 1)?;
    let provided_schema = None;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        provided_schema,
        sql_definition,
    )
    .await
    .unwrap();
    Ok(ctx)
}

fn write_test_data_to_parquet2(tmpdir: &TempDir, n_file: usize) -> Result<()> {
    let low_card_col1 = Field::new("low_card_col1", DataType::Int32, false);
    let low_card_col2 = Field::new("low_card_col2", DataType::Int32, false);
    let inc_col = Field::new("inc_col", DataType::Int32, false);
    let unsorted_col = Field::new("unsorted_col", DataType::Int32, false);
    // let inc_field = Field::new("inc_col", DataType::Int32, false);
    // let desc_field = Field::new("desc_col", DataType::Int32, false);
    // let low_card_field = Field::new("low_card_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![
        low_card_col1,
        low_card_col2,
        inc_col,
        unsorted_col,
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
                3, 3, 3, 3,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
                39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
                57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
                75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92,
                93, 94, 95, 96, 97, 98, 99,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 2, 0, 0, 1, 1, 0, 2, 1, 4, 4, 2, 2, 1, 2, 3, 3, 2, 1, 4, 0, 3, 0, 0,
                4, 0, 2, 0, 1, 1, 3, 4, 2, 2, 4, 0, 1, 4, 0, 1, 1, 3, 3, 2, 3, 0, 0, 1,
                1, 3, 0, 3, 1, 1, 4, 2, 1, 1, 1, 2, 4, 3, 1, 4, 4, 0, 2, 4, 1, 1, 0, 2,
                1, 1, 4, 2, 0, 2, 1, 4, 2, 0, 4, 2, 1, 1, 1, 4, 3, 4, 1, 2, 0, 0, 2, 0,
                4, 2, 4, 3,
            ])),
        ],
    )?;
    let n_chunk = batch.num_rows() / n_file;
    for i in 0..n_file {
        let target_file = tmpdir.path().join(format!("{i}.parquet"));
        let file = File::create(target_file).unwrap();
        // Default writer properties
        let props = WriterProperties::builder().build();
        let chunks_start = i * n_chunk;
        let cur_batch = batch.slice(chunks_start, n_chunk);
        // let chunks_end = chunks_start + n_chunk;
        let mut writer =
            ArrowWriter::try_new(file, cur_batch.schema(), Some(props)).unwrap();

        writer.write(&cur_batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();
    }
    Ok(())
}

async fn get_test_context2(tmpdir: &TempDir) -> Result<SessionContext> {
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(session_config);

    let parquet_read_options = ParquetReadOptions::default();
    // The sort order is specified (not actually correct in this case)
    // let file_sort_order = [col("ts")]
    //     .into_iter()
    //     .map(|e| {
    //         let ascending = true;
    //         let nulls_first = false;
    //         e.sort(ascending, nulls_first)
    //     })
    //     .collect::<Vec<_>>();
    let file_sort_order = vec![
        col("low_card_col1").sort(true, false),
        col("low_card_col2").sort(true, false),
        col("inc_col").sort(true, false),
    ];

    let options_sort = parquet_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order));

    write_test_data_to_parquet2(tmpdir, 1)?;
    let provided_schema = None;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data2",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        provided_schema,
        sql_definition,
    )
    .await
    .unwrap();
    Ok(ctx)
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn test_source_sorted_aggregate() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            SUM(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as sum1,
            SUM(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as sum2,
            SUM(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as sum3,
            MIN(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as min1,
            MIN(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as min2,
            MIN(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as min3,
            MAX(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as max1,
            MAX(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as max2,
            MAX(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as max3,
            COUNT(*) OVER(ORDER BY ts RANGE BETWEEN 4 PRECEDING AND 8 FOLLOWING) as cnt1,
            COUNT(*) OVER(ORDER BY ts ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cnt2,
            SUM(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING AND 4 FOLLOWING) as sumr1,
            SUM(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING AND 8 FOLLOWING) as sumr2,
            SUM(desc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sumr3,
            MIN(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as minr1,
            MIN(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as minr2,
            MIN(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as minr3,
            MAX(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as maxr1,
            MAX(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as maxr2,
            MAX(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as maxr3,
            COUNT(*) OVER(ORDER BY ts DESC RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING) as cntr1,
            COUNT(*) OVER(ORDER BY ts DESC ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cntr2,
            SUM(desc_col) OVER(ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as sum4,
            COUNT(*) OVER(ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cnt3
            FROM annotated_data
            ORDER BY inc_col DESC
            LIMIT 5
            ";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[sum1@0 as sum1, sum2@1 as sum2, sum3@2 as sum3, min1@3 as min1, min2@4 as min2, min3@5 as min3, max1@6 as max1, max2@7 as max2, max3@8 as max3, cnt1@9 as cnt1, cnt2@10 as cnt2, sumr1@11 as sumr1, sumr2@12 as sumr2, sumr3@13 as sumr3, minr1@14 as minr1, minr2@15 as minr2, minr3@16 as minr3, maxr1@17 as maxr1, maxr2@18 as maxr2, maxr3@19 as maxr3, cntr1@20 as cntr1, cntr2@21 as cntr2, sum4@22 as sum4, cnt3@23 as cnt3]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@24 DESC]",
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@14 as sum1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@15 as sum2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@16 as sum3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@17 as min1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@18 as min2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@19 as min3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@20 as max1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@21 as max2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@22 as max3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 4 PRECEDING AND 8 FOLLOWING@23 as cnt1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@24 as cnt2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 4 FOLLOWING@3 as sumr1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 8 FOLLOWING@4 as sumr2, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@5 as sumr3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@6 as minr1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@7 as minr2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@8 as minr3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@9 as maxr1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@10 as maxr2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@11 as maxr3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING@12 as cntr1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@13 as cntr2, SUM(annotated_data.desc_col) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@25 as sum4, COUNT(UInt8(1)) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@26 as cnt3, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(8)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }]",
                "            BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(8)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(2)), end_bound: Following(Int32(6)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(8)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
            "| sum1 | sum2 | sum3 | min1 | min2 | min3 | max1 | max2 | max3 | cnt1 | cnt2 | sumr1 | sumr2 | sumr3 | minr1 | minr2 | minr3 | maxr1 | maxr2 | maxr3 | cntr1 | cntr2 | sum4  | cnt3 |",
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
            "| 1482 | -631 | 606  | 289  | -213 | 301  | 305  | -208 | 305  | 3    | 9    | 902   | -834  | -1231 | 301   | -213  | 269   | 305   | -210  | 305   | 3     | 2     | -1797 | 9    |",
            "| 1482 | -631 | 902  | 289  | -213 | 296  | 305  | -208 | 305  | 3    | 10   | 902   | -834  | -1424 | 301   | -213  | 266   | 305   | -210  | 305   | 3     | 3     | -1978 | 10   |",
            "| 876  | -411 | 1193 | 289  | -208 | 291  | 296  | -203 | 305  | 4    | 10   | 587   | -612  | -1400 | 296   | -213  | 261   | 305   | -208  | 301   | 3     | 4     | -1941 | 10   |",
            "| 866  | -404 | 1482 | 286  | -203 | 289  | 291  | -201 | 305  | 5    | 10   | 580   | -600  | -1374 | 291   | -208  | 259   | 305   | -203  | 296   | 4     | 5     | -1903 | 10   |",
            "| 1411 | -397 | 1768 | 275  | -201 | 286  | 289  | -196 | 305  | 4    | 10   | 575   | -590  | -1347 | 289   | -203  | 254   | 305   | -201  | 291   | 2     | 6     | -1863 | 10   |",
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_builtin() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            FIRST_VALUE(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as fv1,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as fv2,
            LAST_VALUE(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as lv1,
            LAST_VALUE(inc_col) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lv2,
            NTH_VALUE(inc_col, 5) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as nv1,
            NTH_VALUE(inc_col, 5) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as nv2,
            ROW_NUMBER() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS rn1,
            ROW_NUMBER() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as rn2,
            RANK() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS rank1,
            RANK() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as rank2,
            DENSE_RANK() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS dense_rank1,
            DENSE_RANK() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as dense_rank2,
            LAG(inc_col, 1, 1001) OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lag1,
            LAG(inc_col, 2, 1002) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lag2,
            LEAD(inc_col, -1, 1001) OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lead1,
            LEAD(inc_col, 4, 1004) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lead2,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as fvr1,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as fvr2,
            LAST_VALUE(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as lvr1,
            LAST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lvr2,
            LAG(inc_col, 1, 1001) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lagr1,
            LAG(inc_col, 2, 1002) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lagr2,
            LEAD(inc_col, -1, 1001) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS leadr1,
            LEAD(inc_col, 4, 1004) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as leadr2
            FROM annotated_data
            ORDER BY ts DESC
            LIMIT 5
            ";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[fv1@0 as fv1, fv2@1 as fv2, lv1@2 as lv1, lv2@3 as lv2, nv1@4 as nv1, nv2@5 as nv2, rn1@6 as rn1, rn2@7 as rn2, rank1@8 as rank1, rank2@9 as rank2, dense_rank1@10 as dense_rank1, dense_rank2@11 as dense_rank2, lag1@12 as lag1, lag2@13 as lag2, lead1@14 as lead1, lead2@15 as lead2, fvr1@16 as fvr1, fvr2@17 as fvr2, lvr1@18 as lvr1, lvr2@19 as lvr2, lagr1@20 as lagr1, lagr2@21 as lagr2, leadr1@22 as leadr1, leadr2@23 as leadr2]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[ts@24 DESC]",
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@10 as fv1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@11 as fv2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@12 as lv1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@13 as lv2, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@14 as nv1, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@15 as nv2, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@16 as rn1, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@17 as rn2, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@18 as rank1, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@19 as rank2, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@20 as dense_rank1, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@21 as dense_rank2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@22 as lag1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@23 as lag2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@24 as lead1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@25 as lead2, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@2 as fvr1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@3 as fvr2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@4 as lvr1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@5 as lvr2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@6 as lagr1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@7 as lagr2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@8 as leadr1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@9 as leadr2, ts@0 as ts]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
            "| fv1 | fv2 | lv1 | lv2 | nv1 | nv2 | rn1 | rn2 | rank1 | rank2 | dense_rank1 | dense_rank2 | lag1 | lag2 | lead1 | lead2 | fvr1 | fvr2 | lvr1 | lvr2 | lagr1 | lagr2 | leadr1 | leadr2 |",
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
            "| 289 | 266 | 305 | 305 | 305 | 278 | 99  | 99  | 99    | 99    | 86          | 86          | 296  | 291  | 296   | 1004  | 305  | 305  | 301  | 296  | 305   | 1002  | 305    | 286    |",
            "| 289 | 269 | 305 | 305 | 305 | 283 | 100 | 100 | 99    | 99    | 86          | 86          | 301  | 296  | 301   | 1004  | 305  | 305  | 301  | 301  | 1001  | 1002  | 1001   | 289    |",
            "| 289 | 261 | 296 | 301 |     | 275 | 98  | 98  | 98    | 98    | 85          | 85          | 291  | 289  | 291   | 1004  | 305  | 305  | 296  | 291  | 301   | 305   | 301    | 283    |",
            "| 286 | 259 | 291 | 296 |     | 272 | 97  | 97  | 97    | 97    | 84          | 84          | 289  | 286  | 289   | 1004  | 305  | 305  | 291  | 289  | 296   | 301   | 296    | 278    |",
            "| 275 | 254 | 289 | 291 | 289 | 269 | 96  | 96  | 96    | 96    | 83          | 83          | 286  | 283  | 286   | 305   | 305  | 305  | 289  | 286  | 291   | 296   | 291    | 275    |",
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_unbounded_preceding() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            SUM(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as sum1,
            SUM(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as sum2,
            MIN(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as min1,
            MIN(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as min2,
            MAX(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as max1,
            MAX(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as max2,
            COUNT(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as count1,
            COUNT(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as count2,
            AVG(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as avg1,
            AVG(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as avg2
            FROM annotated_data
            ORDER BY inc_col ASC
            LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[sum1@0 as sum1, sum2@1 as sum2, min1@2 as min1, min2@3 as min2, max1@4 as max1, max2@5 as max2, count1@6 as count1, count2@7 as count2, avg1@8 as avg1, avg2@9 as avg2]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@10 ASC NULLS LAST]",
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@7 as sum1, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@2 as sum2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@8 as min1, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@3 as min2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@9 as max1, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@4 as max2, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@10 as count1, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@5 as count2, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@11 as avg1, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@6 as avg2, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
            "| sum1 | sum2 | min1 | min2 | max1 | max2 | count1 | count2 | avg1              | avg2              |",
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
            "| 16   | 6    | 1    | 1    | 10   | 5    | 3      | 2      | 5.333333333333333 | 3.0               |",
            "| 16   | 6    | 1    | 1    | 10   | 5    | 3      | 2      | 5.333333333333333 | 3.0               |",
            "| 51   | 16   | 1    | 1    | 20   | 10   | 5      | 3      | 10.2              | 5.333333333333333 |",
            "| 72   | 72   | 1    | 1    | 21   | 21   | 6      | 6      | 12.0              | 12.0              |",
            "| 72   | 72   | 1    | 1    | 21   | 21   | 6      | 6      | 12.0              | 12.0              |",
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_unbounded_preceding_builtin() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
           FIRST_VALUE(inc_col) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as first_value1,
           FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as first_value2,
           LAST_VALUE(inc_col) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as last_value1,
           LAST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as last_value2,
           NTH_VALUE(inc_col, 2) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as nth_value1
           FROM annotated_data
           ORDER BY inc_col ASC
           LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[first_value1@0 as first_value1, first_value2@1 as first_value2, last_value1@2 as last_value1, last_value2@3 as last_value2, nth_value1@4 as nth_value1]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@5 ASC NULLS LAST]",
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@4 as first_value1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@2 as first_value2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@5 as last_value1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@3 as last_value2, NTH_VALUE(annotated_data.inc_col,Int64(2)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@6 as nth_value1, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(2)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(2))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+--------------+--------------+-------------+-------------+------------+",
            "| first_value1 | first_value2 | last_value1 | last_value2 | nth_value1 |",
            "+--------------+--------------+-------------+-------------+------------+",
            "| 1            | 15           | 5           | 1           | 5          |",
            "| 1            | 20           | 10          | 1           | 5          |",
            "| 1            | 21           | 15          | 1           | 5          |",
            "| 1            | 26           | 20          | 1           | 5          |",
            "| 1            | 29           | 21          | 1           | 5          |",
            "+--------------+--------------+-------------+-------------+------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_unsorted_partition_by() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT inc_col, low_card_col,
        SUM(inc_col) OVER(PARTITION BY low_card_col ORDER BY ts ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum1,
        SUM(inc_col) OVER(PARTITION BY low_card_col ORDER BY ts ASC ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) as sum2
           FROM annotated_data
           ORDER BY inc_col ASC";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "SortExec: expr=[inc_col@0 ASC NULLS LAST]",
                "  ProjectionExec: expr=[inc_col@1 as inc_col, low_card_col@2 as low_card_col, SUM(annotated_data.inc_col) PARTITION BY [annotated_data.low_card_col] ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@3 as sum1, SUM(annotated_data.inc_col) PARTITION BY [annotated_data.low_card_col] ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING@4 as sum2]",
                "    BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(3)), end_bound: Following(UInt64(1)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------+--------------+------+------+",
            "| inc_col | low_card_col | sum1 | sum2 |",
            "+---------+--------------+------+------+",
            "| 1       | 0            | 27   | 27   |",
            "| 5       | 3            | 20   | 20   |",
            "| 10      | 4            | 47   | 47   |",
            "| 15      | 3            | 53   | 53   |",
            "| 20      | 2            | 41   | 41   |",
            "| 21      | 2            | 90   | 90   |",
            "| 26      | 0            | 70   | 70   |",
            "| 29      | 1            | 59   | 59   |",
            "| 30      | 1            | 99   | 99   |",
            "| 33      | 3            | 98   | 98   |",
            "| 37      | 4            | 100  | 100  |",
            "| 40      | 1            | 143  | 143  |",
            "| 43      | 0            | 153  | 153  |",
            "| 44      | 1            | 165  | 194  |",
            "| 45      | 3            | 163  | 168  |",
            "| 49      | 2            | 181  | 181  |",
            "| 51      | 1            | 196  | 226  |",
            "| 53      | 4            | 158  | 158  |",
            "| 58      | 4            | 213  | 223  |",
            "| 61      | 1            | 246  | 286  |",
            "| 65      | 4            | 251  | 288  |",
            "| 70      | 3            | 245  | 260  |",
            "| 75      | 4            | 276  | 329  |",
            "| 78      | 4            | 306  | 364  |",
            "| 83      | 0            | 281  | 282  |",
            "| 88      | 4            | 336  | 401  |",
            "| 90      | 1            | 307  | 351  |",
            "| 91      | 2            | 261  | 281  |",
            "| 95      | 4            | 370  | 445  |",
            "| 97      | 3            | 371  | 404  |",
            "| 100     | 2            | 351  | 372  |",
            "| 105     | 1            | 382  | 433  |",
            "| 109     | 4            | 411  | 489  |",
            "| 111     | 2            | 417  | 466  |",
            "| 115     | 2            | 446  | 537  |",
            "| 119     | 4            | 447  | 535  |",
            "| 120     | 2            | 490  | 590  |",
            "| 124     | 4            | 492  | 587  |",
            "| 126     | 1            | 452  | 513  |",
            "| 129     | 0            | 425  | 451  |",
            "| 131     | 1            | 497  | 587  |",
            "| 135     | 1            | 539  | 644  |",
            "| 140     | 4            | 526  | 635  |",
            "| 143     | 4            | 555  | 674  |",
            "| 144     | 2            | 616  | 727  |",
            "| 147     | 1            | 562  | 688  |",
            "| 148     | 4            | 582  | 706  |",
            "| 149     | 1            | 586  | 717  |",
            "| 151     | 4            | 602  | 742  |",
            "| 155     | 1            | 607  | 742  |",
            "| 156     | 1            | 663  | 810  |",
            "| 159     | 3            | 491  | 536  |",
            "| 160     | 4            | 622  | 765  |",
            "| 163     | 4            | 646  | 794  |",
            "| 165     | 3            | 602  | 672  |",
            "| 170     | 0            | 559  | 602  |",
            "| 172     | 4            | 681  | 832  |",
            "| 177     | 0            | 658  | 741  |",
            "| 181     | 3            | 704  | 801  |",
            "| 182     | 0            | 721  | 850  |",
            "| 186     | 4            | 708  | 868  |",
            "| 187     | 4            | 742  | 905  |",
            "| 192     | 0            | 747  | 917  |",
            "| 196     | 0            | 777  | 954  |",
            "| 197     | 4            | 791  | 963  |",
            "| 199     | 3            | 754  | 913  |",
            "| 203     | 1            | 728  | 877  |",
            "| 207     | 0            | 814  | 996  |",
            "| 209     | 3            | 802  | 967  |",
            "| 213     | 3            | 843  | 1024 |",
            "| 214     | 1            | 789  | 944  |",
            "| 216     | 1            | 875  | 1031 |",
            "| 219     | 0            | 848  | 1040 |",
            "| 221     | 4            | 841  | 1027 |",
            "| 222     | 3            | 869  | 1068 |",
            "| 225     | 3            | 905  | 1114 |",
            "| 226     | 0            | 883  | 1079 |",
            "| 231     | 0            | 959  | 1166 |",
            "| 236     | 4            | 915  | 1102 |",
            "| 237     | 2            | 748  | 863  |",
            "| 242     | 1            | 925  | 1128 |",
            "| 245     | 3            | 946  | 1159 |",
            "| 247     | 2            | 876  | 996  |",
            "| 248     | 2            | 1010 | 1154 |",
            "| 253     | 1            | 970  | 1184 |",
            "| 254     | 3            | 993  | 1215 |",
            "| 259     | 1            | 1020 | 1236 |",
            "| 261     | 4            | 718  | 915  |",
            "| 266     | 1            | 1053 | 1295 |",
            "| 269     | 3            | 1040 | 1265 |",
            "| 272     | 3            | 1086 | 1331 |",
            "| 275     | 1            | 1086 | 1339 |",
            "| 278     | 2            | 1069 | 1306 |",
            "| 283     | 0            | 1029 | 1248 |",
            "| 286     | 1            | 1128 | 1387 |",
            "| 289     | 0            | 803  | 1029 |",
            "| 291     | 3            | 832  | 1086 |",
            "| 296     | 2            | 1127 | 1374 |",
            "| 301     | 1            | 862  | 1128 |",
            "| 305     | 2            | 879  | 1127 |",
            "+---------+--------------+------+------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_partially_sorted_partition_by() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context2(&tmpdir).await?;

        let sql = "SELECT low_card_col1, low_card_col2, inc_col,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, unsorted_col ORDER BY low_card_col2, inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum1,
        SUM(inc_col) OVER(PARTITION BY unsorted_col ORDER BY low_card_col1, low_card_col2, inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum2,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2 ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum3,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1 ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum4,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum5,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum6
           FROM annotated_data2
           ORDER BY inc_col ASC";

        // let sql = "SELECT
        // SUM(inc_col) OVER(PARTITION BY low_card_col1, inc_col ORDER BY low_card_col2 ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum5,
        // SUM(inc_col) OVER(PARTITION BY inc_col, low_card_col1 ORDER BY low_card_col2 ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum6
        //    FROM annotated_data2
        //    ORDER BY inc_col ASC";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "SortExec: expr=[inc_col@2 ASC NULLS LAST]",
                "  ProjectionExec: expr=[low_card_col1@0 as low_card_col1, low_card_col2@1 as low_card_col2, inc_col@2 as inc_col, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@6 as sum1, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col1 ASC NULLS LAST, annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@9 as sum2, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@4 as sum3, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@7 as sum4, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@5 as sum5, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@8 as sum6]",
                "    BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
                "      BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
                "            BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
                "              BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------+---------------+---------+------+------+------+------+------+------+",
            "| low_card_col1 | low_card_col2 | inc_col | sum1 | sum2 | sum3 | sum4 | sum5 | sum6 |",
            "+---------------+---------------+---------+------+------+------+------+------+------+",
            "| 0             | 0             | 0       | 2    | 2    | 1    | 1    | 2    | 2    |",
            "| 0             | 0             | 1       | 8    | 8    | 3    | 3    | 8    | 8    |",
            "| 0             | 0             | 2       | 5    | 5    | 6    | 6    | 5    | 5    |",
            "| 0             | 0             | 3       | 11   | 11   | 10   | 10   | 11   | 11   |",
            "| 0             | 0             | 4       | 9    | 9    | 14   | 14   | 9    | 9    |",
            "| 0             | 0             | 5       | 17   | 17   | 18   | 18   | 17   | 17   |",
            "| 0             | 0             | 6       | 31   | 31   | 22   | 22   | 31   | 31   |",
            "| 0             | 0             | 7       | 19   | 19   | 26   | 26   | 19   | 19   |",
            "| 0             | 0             | 8       | 30   | 30   | 30   | 30   | 30   | 30   |",
            "| 0             | 0             | 9       | 19   | 19   | 34   | 34   | 19   | 19   |",
            "| 0             | 0             | 10      | 38   | 38   | 38   | 38   | 38   | 38   |",
            "| 0             | 0             | 11      | 31   | 31   | 42   | 42   | 31   | 31   |",
            "| 0             | 0             | 12      | 44   | 44   | 46   | 46   | 44   | 44   |",
            "| 0             | 0             | 13      | 44   | 44   | 50   | 50   | 44   | 44   |",
            "| 0             | 0             | 14      | 54   | 54   | 54   | 54   | 54   | 54   |",
            "| 0             | 0             | 15      | 31   | 31   | 58   | 58   | 31   | 31   |",
            "| 0             | 0             | 16      | 52   | 52   | 62   | 62   | 52   | 52   |",
            "| 0             | 0             | 17      | 69   | 69   | 66   | 66   | 43   | 43   |",
            "| 0             | 0             | 18      | 67   | 67   | 70   | 70   | 39   | 39   |",
            "| 0             | 0             | 19      | 62   | 62   | 74   | 74   | 62   | 62   |",
            "| 0             | 0             | 20      | 51   | 51   | 78   | 78   | 51   | 51   |",
            "| 0             | 0             | 21      | 82   | 82   | 82   | 82   | 52   | 52   |",
            "| 0             | 0             | 22      | 71   | 71   | 86   | 86   | 71   | 71   |",
            "| 0             | 0             | 23      | 90   | 90   | 90   | 90   | 65   | 65   |",
            "| 0             | 0             | 24      | 84   | 84   | 69   | 69   | 53   | 53   |",
            "| 0             | 1             | 25      | 97   | 97   | 51   | 51   | 52   | 52   |",
            "| 0             | 1             | 26      | 89   | 89   | 78   | 78   | 58   | 58   |",
            "| 0             | 1             | 27      | 110  | 110  | 106  | 106  | 87   | 87   |",
            "| 0             | 1             | 28      | 88   | 88   | 110  | 110  | 57   | 57   |",
            "| 0             | 1             | 29      | 111  | 111  | 114  | 114  | 93   | 93   |",
            "| 0             | 1             | 30      | 108  | 108  | 118  | 118  | 71   | 71   |",
            "| 0             | 1             | 31      | 108  | 108  | 122  | 122  | 65   | 65   |",
            "| 0             | 1             | 32      | 108  | 108  | 126  | 126  | 91   | 91   |",
            "| 0             | 1             | 33      | 134  | 134  | 130  | 130  | 134  | 134  |",
            "| 0             | 1             | 34      | 126  | 126  | 134  | 134  | 102  | 102  |",
            "| 0             | 1             | 35      | 125  | 125  | 138  | 138  | 125  | 125  |",
            "| 0             | 1             | 36      | 132  | 132  | 142  | 142  | 132  | 132  |",
            "| 0             | 1             | 37      | 102  | 156  | 146  | 146  | 102  | 102  |",
            "| 0             | 1             | 38      | 145  | 145  | 150  | 150  | 145  | 145  |",
            "| 0             | 1             | 39      | 144  | 144  | 154  | 154  | 144  | 144  |",
            "| 0             | 1             | 40      | 162  | 162  | 158  | 158  | 162  | 162  |",
            "| 0             | 1             | 41      | 134  | 134  | 162  | 162  | 113  | 113  |",
            "| 0             | 1             | 42      | 157  | 157  | 166  | 166  | 157  | 157  |",
            "| 0             | 1             | 43      | 108  | 163  | 170  | 170  | 108  | 108  |",
            "| 0             | 1             | 44      | 176  | 176  | 174  | 174  | 176  | 176  |",
            "| 0             | 1             | 45      | 164  | 164  | 178  | 178  | 164  | 164  |",
            "| 0             | 1             | 46      | 129  | 179  | 182  | 182  | 129  | 129  |",
            "| 0             | 1             | 47      | 174  | 174  | 186  | 186  | 174  | 174  |",
            "| 0             | 1             | 48      | 135  | 187  | 190  | 190  | 135  | 135  |",
            "| 0             | 1             | 49      | 135  | 186  | 144  | 144  | 135  | 135  |",
            "| 1             | 2             | 50      | 115  | 206  | 101  | 101  | 115  | 115  |",
            "| 1             | 2             | 51      | 112  | 205  | 153  | 153  | 112  | 112  |",
            "| 1             | 2             | 52      | 105  | 200  | 206  | 206  | 105  | 105  |",
            "| 1             | 2             | 53      | 161  | 209  | 210  | 210  | 161  | 161  |",
            "| 1             | 2             | 54      | 114  | 185  | 214  | 214  | 114  | 114  |",
            "| 1             | 2             | 55      | 114  | 190  | 218  | 218  | 114  | 114  |",
            "| 1             | 2             | 56      | 218  | 218  | 222  | 222  | 218  | 218  |",
            "| 1             | 2             | 57      | 224  | 224  | 226  | 226  | 224  | 224  |",
            "| 1             | 2             | 58      | 233  | 233  | 230  | 230  | 233  | 233  |",
            "| 1             | 2             | 59      | 180  | 223  | 234  | 234  | 180  | 180  |",
            "| 1             | 2             | 60      | 177  | 214  | 238  | 238  | 177  | 177  |",
            "| 1             | 2             | 61      | 200  | 249  | 242  | 242  | 112  | 112  |",
            "| 1             | 2             | 62      | 245  | 245  | 246  | 246  | 245  | 245  |",
            "| 1             | 2             | 63      | 241  | 241  | 250  | 250  | 241  | 241  |",
            "| 1             | 2             | 64      | 254  | 254  | 254  | 254  | 254  | 254  |",
            "| 1             | 2             | 65      | 185  | 231  | 258  | 258  | 185  | 185  |",
            "| 1             | 2             | 66      | 251  | 251  | 262  | 262  | 251  | 251  |",
            "| 1             | 2             | 67      | 268  | 268  | 266  | 266  | 268  | 268  |",
            "| 1             | 2             | 68      | 257  | 257  | 270  | 270  | 257  | 257  |",
            "| 1             | 2             | 69      | 271  | 271  | 274  | 274  | 271  | 271  |",
            "| 1             | 2             | 70      | 261  | 261  | 278  | 278  | 185  | 185  |",
            "| 1             | 2             | 71      | 271  | 271  | 282  | 282  | 196  | 196  |",
            "| 1             | 2             | 72      | 282  | 282  | 286  | 286  | 282  | 282  |",
            "| 1             | 2             | 73      | 292  | 292  | 290  | 290  | 214  | 214  |",
            "| 1             | 2             | 74      | 284  | 284  | 219  | 219  | 205  | 205  |",
            "| 1             | 3             | 75      | 289  | 289  | 151  | 151  | 152  | 152  |",
            "| 1             | 3             | 76      | 292  | 292  | 228  | 228  | 157  | 157  |",
            "| 1             | 3             | 77      | 303  | 303  | 306  | 306  | 232  | 232  |",
            "| 1             | 3             | 78      | 307  | 307  | 310  | 310  | 162  | 162  |",
            "| 1             | 3             | 79      | 302  | 302  | 314  | 314  | 161  | 161  |",
            "| 1             | 3             | 80      | 315  | 315  | 318  | 318  | 315  | 315  |",
            "| 1             | 3             | 81      | 319  | 319  | 322  | 322  | 249  | 249  |",
            "| 1             | 3             | 82      | 322  | 322  | 326  | 326  | 248  | 248  |",
            "| 1             | 3             | 83      | 331  | 331  | 330  | 330  | 331  | 331  |",
            "| 1             | 3             | 84      | 320  | 320  | 334  | 334  | 247  | 247  |",
            "| 1             | 3             | 85      | 333  | 333  | 338  | 338  | 333  | 333  |",
            "| 1             | 3             | 86      | 345  | 345  | 342  | 342  | 345  | 345  |",
            "| 1             | 3             | 87      | 337  | 337  | 346  | 346  | 337  | 337  |",
            "| 1             | 3             | 88      | 299  | 299  | 350  | 350  | 187  | 187  |",
            "| 1             | 3             | 89      | 354  | 354  | 354  | 354  | 354  | 354  |",
            "| 1             | 3             | 90      | 261  | 261  | 358  | 358  | 261  | 261  |",
            "| 1             | 3             | 91      | 348  | 348  | 362  | 362  | 348  | 348  |",
            "| 1             | 3             | 92      | 342  | 342  | 366  | 366  | 342  | 342  |",
            "| 1             | 3             | 93      | 361  | 361  | 370  | 370  | 361  | 361  |",
            "| 1             | 3             | 94      | 365  | 365  | 374  | 374  | 365  | 365  |",
            "| 1             | 3             | 95      | 280  | 280  | 378  | 378  | 280  | 280  |",
            "| 1             | 3             | 96      | 370  | 370  | 382  | 382  | 370  | 370  |",
            "| 1             | 3             | 97      | 282  | 282  | 386  | 386  | 282  | 282  |",
            "| 1             | 3             | 98      | 283  | 283  | 390  | 390  | 283  | 283  |",
            "| 1             | 3             | 99      | 248  | 248  | 294  | 294  | 187  | 187  |",
            "+---------------+---------------+---------+------+------+------+------+------+------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }
}
