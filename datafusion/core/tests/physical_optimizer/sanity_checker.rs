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

use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_common::Result;

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
