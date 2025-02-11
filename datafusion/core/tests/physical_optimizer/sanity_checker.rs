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

use crate::physical_optimizer::test_utils::{
    bounded_window_exec, global_limit_exec, local_limit_exec, memory_exec,
    repartition_exec, sort_exec, sort_expr_options, sort_merge_join_exec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{JoinType, Result};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
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
    let opts = ConfigOptions::default();
    assert_eq!(
        sanity_checker.optimize(plan.clone(), &opts).is_ok(),
        is_sane
    );
}

/// Check if the plan we created is as expected by comparing the plan
/// formatted as a string.
fn assert_plan(plan: &dyn ExecutionPlan, expected_lines: Vec<&str>) {
    let plan_str = displayable(plan).indent(true).to_string();
    let actual_lines: Vec<&str> = plan_str.trim().lines().collect();
    assert_eq!(actual_lines, expected_lines);
}

#[tokio::test]
/// Tests that plan is valid when the sort requirements are satisfied.
async fn test_bounded_window_agg_sort_requirement() -> Result<()> {
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
    let sort = sort_exec(sort_exprs.clone(), source);
    let bw = bounded_window_exec("c9", sort_exprs, sort);
    assert_plan(bw.as_ref(), vec![
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortExec: expr=[c9@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]"
    ]);
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
    assert_plan(bw.as_ref(), vec![
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]"
    ]);
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
    let limit = global_limit_exec(source);

    assert_plan(
        limit.as_ref(),
        vec![
            "GlobalLimitExec: skip=0, fetch=100",
            "  DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
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
    let limit = global_limit_exec(repartition_exec(source));

    assert_plan(
        limit.as_ref(),
        vec![
            "GlobalLimitExec: skip=0, fetch=100",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
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
    let limit = local_limit_exec(source);

    assert_plan(
        limit.as_ref(),
        vec![
            "LocalLimitExec: fetch=100",
            "  DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
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
    let sort_exprs1 = vec![sort_expr_options("c9", &source1.schema(), sort_opts)];
    let sort_exprs2 = vec![sort_expr_options("a", &source2.schema(), sort_opts)];
    let left = sort_exec(sort_exprs1, source1);
    let right = sort_exec(sort_exprs2, source2);
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
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

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1",
            "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
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
    let sort_exprs1 = vec![sort_expr_options(
        "c9",
        &source1.schema(),
        SortOptions::default(),
    )];
    let left = sort_exec(sort_exprs1, source1);
    // Missing sort of the right child here..
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
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

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1",
            "    DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
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
    let sort_exprs1 = vec![sort_expr_options("c9", &source1.schema(), sort_opts)];
    let sort_exprs2 = vec![sort_expr_options("a", &source2.schema(), sort_opts)];
    let left = sort_exec(sort_exprs1, source1);
    let right = sort_exec(sort_exprs2, source2);
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::RoundRobinBatch(10),
    )?);
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    // Missing hash partitioning on right child.

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]",
        ],
    );
    // Distribution requirement for the `SortMergeJoin` is not satisfied for right child (has round-robin partitioning). We expect to receive error during sanity check.
    assert_sanity_check(&smj, false);
    Ok(())
}
