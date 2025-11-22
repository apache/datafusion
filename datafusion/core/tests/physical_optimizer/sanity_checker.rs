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
use std::sync::Arc;

use crate::physical_optimizer::test_utils::{
    bounded_window_exec, global_limit_exec, local_limit_exec, memory_exec,
    projection_exec, repartition_exec, sort_exec, sort_expr, sort_expr_options,
    sort_merge_join_exec, sort_preserving_merge_exec, union_exec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_common::{JoinType, Result, ScalarValue};
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr::expressions::{col, Literal};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{displayable, ExecutionPlan};

use async_trait::async_trait;

async fn register_current_csv(
    ctx: &SessionContext,
    table_name: &str,
    infinite: bool,
) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = datafusion::test_util::aggr_test_schema();
    let path = format!("{testdata}/csv/aggregate_test_100.csv");

    match infinite {
        true => {
            let source = FileStreamProvider::new_file(schema, path.into());
            let config = StreamConfig::new(Arc::new(source));
            ctx.register_table(table_name, Arc::new(StreamTable::new(Arc::new(config))))?;
        }
        false => {
            ctx.register_csv(table_name, &path, CsvReadOptions::new().schema(&schema))
                .await?;
        }
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
pub enum SourceType {
    Unbounded,
    Bounded,
}

#[async_trait]
pub trait SqlTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()>;
    fn expect_fail(&self) -> bool;
}

/// [UnaryTestCase] is designed for single input [ExecutionPlan]s.
pub struct UnaryTestCase {
    pub source_type: SourceType,
    pub expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for UnaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let table_is_infinite = self.source_type == SourceType::Unbounded;
        register_current_csv(ctx, "test", table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}

/// [BinaryTestCase] is designed for binary input [ExecutionPlan]s.
pub struct BinaryTestCase {
    pub source_types: (SourceType, SourceType),
    pub expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for BinaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let left_table_is_infinite = self.source_types.0 == SourceType::Unbounded;
        let right_table_is_infinite = self.source_types.1 == SourceType::Unbounded;
        register_current_csv(ctx, "left", left_table_is_infinite).await?;
        register_current_csv(ctx, "right", right_table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}

pub struct QueryCase {
    pub sql: String,
    pub cases: Vec<Arc<dyn SqlTestCase>>,
    pub error_operator: String,
}

impl QueryCase {
    /// Run the test cases
    pub async fn run(&self) -> Result<()> {
        for case in &self.cases {
            let ctx = SessionContext::new();
            case.register_table(&ctx).await?;
            let error = if case.expect_fail() {
                Some(&self.error_operator)
            } else {
                None
            };
            self.run_case(ctx, error).await?;
        }
        Ok(())
    }

    async fn run_case(&self, ctx: SessionContext, error: Option<&String>) -> Result<()> {
        let dataframe = ctx.sql(self.sql.as_str()).await?;
        let plan = dataframe.create_physical_plan().await;
        if let Some(error) = error {
            let plan_error = plan.unwrap_err();
            assert!(
                plan_error.to_string().contains(error.as_str()),
                "plan_error: {:?} doesn't contain message: {:?}",
                plan_error,
                error.as_str()
            );
        } else {
            assert!(plan.is_ok())
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_hash_left_join_swap() -> Result<()> {
    let test1 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Bounded),
        expect_fail: false,
    };

    let test2 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Unbounded),
        // Left join for bounded build side and unbounded probe side can generate
        // both incremental matched rows and final non-matched rows.
        expect_fail: false,
    };
    let test3 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Bounded),
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "SELECT t2.c1 FROM left as t1 LEFT JOIN right as t2 ON t1.c1 = t2.c1"
            .to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
        error_operator: "operator: HashJoinExec".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_hash_right_join_swap() -> Result<()> {
    let test1 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Bounded),
        expect_fail: true,
    };
    let test2 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Unbounded),
        expect_fail: false,
    };
    let test3 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Bounded),
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "SELECT t2.c1 FROM left as t1 RIGHT JOIN right as t2 ON t1.c1 = t2.c1"
            .to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
        error_operator: "operator: HashJoinExec".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_hash_inner_join_swap() -> Result<()> {
    let test1 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Bounded),
        expect_fail: false,
    };
    let test2 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Unbounded),
        expect_fail: false,
    };
    let test3 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Bounded),
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "SELECT t2.c1 FROM left as t1 JOIN right as t2 ON t1.c1 = t2.c1".to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
        error_operator: "Join Error".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_hash_full_outer_join_swap() -> Result<()> {
    let test1 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Bounded),
        expect_fail: true,
    };
    let test2 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Unbounded),
        // Full join for bounded build side and unbounded probe side can generate
        // both incremental matched rows and final non-matched rows.
        expect_fail: false,
    };
    let test3 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Bounded),
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "SELECT t2.c1 FROM left as t1 FULL JOIN right as t2 ON t1.c1 = t2.c1"
            .to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
        error_operator: "operator: HashJoinExec".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_aggregate() -> Result<()> {
    let test1 = UnaryTestCase {
        source_type: SourceType::Bounded,
        expect_fail: false,
    };
    let test2 = UnaryTestCase {
        source_type: SourceType::Unbounded,
        expect_fail: true,
    };
    let case = QueryCase {
        sql: "SELECT c1, MIN(c4) FROM test GROUP BY c1".to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2)],
        error_operator: "operator: AggregateExec".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_window_agg_hash_partition() -> Result<()> {
    let test1 = UnaryTestCase {
        source_type: SourceType::Bounded,
        expect_fail: false,
    };
    let test2 = UnaryTestCase {
        source_type: SourceType::Unbounded,
        expect_fail: true,
    };
    let case = QueryCase {
        sql: "SELECT
                c9,
                SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) as sum1
              FROM test
              LIMIT 5".to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2)],
        error_operator: "operator: SortExec".to_string()
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_window_agg_single_partition() -> Result<()> {
    let test1 = UnaryTestCase {
        source_type: SourceType::Bounded,
        expect_fail: false,
    };
    let test2 = UnaryTestCase {
        source_type: SourceType::Unbounded,
        expect_fail: true,
    };
    let case = QueryCase {
        sql: "SELECT
                    c9,
                    SUM(c9) OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) as sum1
              FROM test".to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2)],
        error_operator: "operator: SortExec".to_string()
    };
    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_hash_cross_join() -> Result<()> {
    let test1 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Bounded),
        expect_fail: true,
    };
    let test2 = BinaryTestCase {
        source_types: (SourceType::Unbounded, SourceType::Unbounded),
        expect_fail: true,
    };
    let test3 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Unbounded),
        expect_fail: true,
    };
    let test4 = BinaryTestCase {
        source_types: (SourceType::Bounded, SourceType::Bounded),
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "SELECT t2.c1 FROM left as t1 CROSS JOIN right as t2".to_string(),
        cases: vec![
            Arc::new(test1),
            Arc::new(test2),
            Arc::new(test3),
            Arc::new(test4),
        ],
        error_operator: "operator: CrossJoinExec".to_string(),
    };

    case.run().await?;
    Ok(())
}

#[tokio::test]
async fn test_analyzer() -> Result<()> {
    let test1 = UnaryTestCase {
        source_type: SourceType::Bounded,
        expect_fail: false,
    };
    let test2 = UnaryTestCase {
        source_type: SourceType::Unbounded,
        expect_fail: false,
    };
    let case = QueryCase {
        sql: "EXPLAIN ANALYZE SELECT * FROM test".to_string(),
        cases: vec![Arc::new(test1), Arc::new(test2)],
        error_operator: "Analyze Error".to_string(),
    };

    case.run().await?;
    Ok(())
}

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("c9", DataType::Int32, true)]))
}

fn create_test_schema2() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]))
}

/// Check if sanity checker should accept or reject plans.
fn assert_sanity_check(plan: &Arc<dyn ExecutionPlan>, is_sane: bool) {
    let sanity_checker = SanityCheckPlan::new();
    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    assert_eq!(
        sanity_checker
            .optimize_plan(plan.clone(), &optimizer_context)
            .is_ok(),
        is_sane
    );
}

#[tokio::test]
/// Tests that plan is valid when the sort requirements are satisfied.
async fn test_bounded_window_agg_sort_requirement() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr_options(
        "c9",
        &source.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering.clone(), source);
    let bw = bounded_window_exec("c9", ordering, sort);
    let plan_str = displayable(bw.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r#"
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      SortExec: expr=[c9@0 ASC NULLS LAST], preserve_partitioning=[false]
        DataSourceExec: partitions=1, partition_sizes=[0]
    "#
    );
    assert_sanity_check(&bw, true);
    Ok(())
}

#[tokio::test]
/// Tests that plan is invalid when the sort requirements are not satisfied.
async fn test_bounded_window_agg_no_sort_requirement() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr_options(
        "c9",
        &source.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )];
    let bw = bounded_window_exec("c9", sort_exprs, source);
    let plan_str = displayable(bw.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r#"
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: partitions=1, partition_sizes=[0]
    "#
    );
    // Order requirement of the `BoundedWindowAggExec` is not satisfied. We expect to receive error during sanity check.
    assert_sanity_check(&bw, false);
    Ok(())
}

#[tokio::test]
/// A valid when a single partition requirement
/// is satisfied.
async fn test_global_limit_single_partition() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = global_limit_exec(source, 0, Some(100));

    let plan_str = displayable(limit.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    GlobalLimitExec: skip=0, fetch=100
      DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    assert_sanity_check(&limit, true);
    Ok(())
}

#[tokio::test]
/// An invalid plan when a single partition requirement
/// is not satisfied.
async fn test_global_limit_multi_partition() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = global_limit_exec(repartition_exec(source), 0, Some(100));

    let plan_str = displayable(limit.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    GlobalLimitExec: skip=0, fetch=100
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    // Distribution requirement of the `GlobalLimitExec` is not satisfied. We expect to receive error during sanity check.
    assert_sanity_check(&limit, false);
    Ok(())
}

#[tokio::test]
/// A plan with no requirements should satisfy.
async fn test_local_limit() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = local_limit_exec(source, 100);

    let plan_str = displayable(limit.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    LocalLimitExec: fetch=100
      DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    assert_sanity_check(&limit, true);
    Ok(())
}

#[tokio::test]
/// Valid plan with multiple children satisfy both order and distribution.
async fn test_sort_merge_join_satisfied() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let source2 = memory_exec(&schema2);
    let sort_opts = SortOptions::default();
    let ordering1 = [sort_expr_options("c9", &source1.schema(), sort_opts)].into();
    let ordering2 = [sort_expr_options("a", &source2.schema(), sort_opts)].into();
    let left = sort_exec(ordering1, source1);
    let right = sort_exec(ordering2, source2);
    let left_jcol = col("c9", &left.schema())?;
    let right_jcol = col("a", &right.schema())?;
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(vec![right_jcol.clone()], 10),
    )?);

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    let plan_str = displayable(smj.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]
      RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[0]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    assert_sanity_check(&smj, true);
    Ok(())
}

#[tokio::test]
/// Invalid case when the order is not satisfied by the 2nd
/// child.
async fn test_sort_merge_join_order_missing() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let right = memory_exec(&schema2);
    let ordering1 = [sort_expr_options(
        "c9",
        &source1.schema(),
        SortOptions::default(),
    )]
    .into();
    let left = sort_exec(ordering1, source1);
    // Missing sort of the right child here..
    let left_jcol = col("c9", &left.schema())?;
    let right_jcol = col("a", &right.schema())?;
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(vec![right_jcol.clone()], 10),
    )?);

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    let plan_str = displayable(smj.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]
      RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[0]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    // Order requirement for the `SortMergeJoin` is not satisfied for right child. We expect to receive error during sanity check.
    assert_sanity_check(&smj, false);
    Ok(())
}

#[tokio::test]
/// Invalid case when the distribution is not satisfied by the 2nd
/// child.
async fn test_sort_merge_join_dist_missing() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let source2 = memory_exec(&schema2);
    let sort_opts = SortOptions::default();
    let ordering1 = [sort_expr_options("c9", &source1.schema(), sort_opts)].into();
    let ordering2 = [sort_expr_options("a", &source2.schema(), sort_opts)].into();
    let left = sort_exec(ordering1, source1);
    let right = sort_exec(ordering2, source2);
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::RoundRobinBatch(10),
    )?);
    let left_jcol = col("c9", &left.schema())?;
    let right_jcol = col("a", &right.schema())?;
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    // Missing hash partitioning on right child.

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    let plan_str = displayable(smj.as_ref()).indent(true).to_string();
    let actual = plan_str.trim();
    assert_snapshot!(
        actual,
        @r"
    SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]
      RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[0]
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );
    // Distribution requirement for the `SortMergeJoin` is not satisfied for right child (has round-robin partitioning). We expect to receive error during sanity check.
    assert_sanity_check(&smj, false);
    Ok(())
}

/// A particular edge case.
///
/// See <https://github.com/apache/datafusion/issues/17372>.
#[tokio::test]
async fn test_union_with_sorts_and_constants() -> Result<()> {
    let schema_in = create_test_schema2();

    let proj_exprs_1 = vec![
        (
            Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_owned())))) as _,
            "const_1".to_owned(),
        ),
        (
            Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_owned())))) as _,
            "const_2".to_owned(),
        ),
        (col("a", &schema_in).unwrap(), "a".to_owned()),
    ];
    let proj_exprs_2 = vec![
        (
            Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_owned())))) as _,
            "const_1".to_owned(),
        ),
        (
            Arc::new(Literal::new(ScalarValue::Utf8(Some("bar".to_owned())))) as _,
            "const_2".to_owned(),
        ),
        (col("a", &schema_in).unwrap(), "a".to_owned()),
    ];

    let source_1 = memory_exec(&schema_in);
    let source_1 = projection_exec(proj_exprs_1.clone(), source_1).unwrap();
    let schema_sources = source_1.schema();
    let ordering_sources: LexOrdering =
        [sort_expr("a", &schema_sources).nulls_last()].into();
    let source_1 = sort_exec(ordering_sources.clone(), source_1);

    let source_2 = memory_exec(&schema_in);
    let source_2 = projection_exec(proj_exprs_2, source_2).unwrap();
    let source_2 = sort_exec(ordering_sources.clone(), source_2);

    let plan = union_exec(vec![source_1, source_2]);

    let schema_out = plan.schema();
    let ordering_out: LexOrdering = [
        sort_expr("const_1", &schema_out).nulls_last(),
        sort_expr("const_2", &schema_out).nulls_last(),
        sort_expr("a", &schema_out).nulls_last(),
    ]
    .into();

    let plan = sort_preserving_merge_exec(ordering_out, plan);

    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    let plan_str = plan_str.trim();
    assert_snapshot!(
        plan_str,
        @r"
    SortPreservingMergeExec: [const_1@0 ASC NULLS LAST, const_2@1 ASC NULLS LAST, a@2 ASC NULLS LAST]
      UnionExec
        SortExec: expr=[a@2 ASC NULLS LAST], preserve_partitioning=[false]
          ProjectionExec: expr=[foo as const_1, foo as const_2, a@0 as a]
            DataSourceExec: partitions=1, partition_sizes=[0]
        SortExec: expr=[a@2 ASC NULLS LAST], preserve_partitioning=[false]
          ProjectionExec: expr=[foo as const_1, bar as const_2, a@0 as a]
            DataSourceExec: partitions=1, partition_sizes=[0]
    "
    );

    assert_sanity_check(&plan, true);

    Ok(())
}
