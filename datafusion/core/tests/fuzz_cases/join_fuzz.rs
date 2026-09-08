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
use std::time::SystemTime;

use crate::fuzz_cases::join_fuzz::JoinTestType::{HjSmj, NljHj};

use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::JoinSide;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
    SortMergeJoinExec,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, common};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{NullEquality, ScalarValue};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

use itertools::Itertools;
use rand::Rng;
use rand::{SeedableRng, rngs::StdRng};
use test_utils::stagger_batch_with_seed;

// Determines what Fuzz tests needs to run
// Ideally all tests should match, but in reality some tests
// passes only partial cases
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JoinTestType {
    // compare NestedLoopJoin and HashJoin
    NljHj,
    // compare HashJoin and SortMergeJoin, no need to compare SortMergeJoin and NestedLoopJoin
    // because if existing variants both passed that means SortMergeJoin and NestedLoopJoin also passes
    HjSmj,
}

fn col_lt_col_filter(schema1: Arc<Schema>, schema2: Arc<Schema>) -> JoinFilter {
    let less_filter = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("x", 1)),
        Operator::Lt,
        Arc::new(Column::new("x", 0)),
    )) as _;
    let column_indices = vec![
        ColumnIndex {
            index: 2,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 2,
            side: JoinSide::Right,
        },
    ];
    let intermediate_schema = Schema::new(vec![
        schema1
            .field_with_name("x")
            .unwrap()
            .clone()
            .with_nullable(true),
        schema2
            .field_with_name("x")
            .unwrap()
            .clone()
            .with_nullable(true),
    ]);

    JoinFilter::new(less_filter, column_indices, Arc::new(intermediate_schema))
}

#[tokio::test]
async fn test_inner_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Inner,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_inner_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Inner,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Left,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Left,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Right,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Right,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_full_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Full,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_full_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::Full,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[NljHj, HjSmj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_semi_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftSemi,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_semi_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftSemi,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_semi_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightSemi,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_semi_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightSemi,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_anti_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftAnti,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_anti_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftAnti,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_anti_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightAnti,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_anti_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightAnti,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_mark_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftMark,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_mark_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::LeftMark,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

// todo: add JoinTestType::HjSmj after Right mark SortMergeJoin support
#[tokio::test]
async fn test_right_mark_join_1k() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightMark,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_mark_join_1k_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_i32(1000, left_extra),
            make_staggered_batches_i32(1000, right_extra),
            JoinType::RightMark,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_inner_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Inner,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_inner_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Inner,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Left,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Left,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Right,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Right,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_full_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Full,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_full_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::Full,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[NljHj, HjSmj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_semi_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftSemi,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_semi_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftSemi,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_semi_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightSemi,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_semi_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightSemi,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_anti_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftAnti,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_anti_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftAnti,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_anti_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightAnti,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_anti_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightAnti,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_mark_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftMark,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_left_mark_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::LeftMark,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

// todo: add JoinTestType::HjSmj after Right mark SortMergeJoin support
#[tokio::test]
async fn test_right_mark_join_1k_binary() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightMark,
            None,
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

#[tokio::test]
async fn test_right_mark_join_1k_binary_filtered() {
    for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
        JoinFuzzTestCase::new(
            make_staggered_batches_binary(1000, left_extra),
            make_staggered_batches_binary(1000, right_extra),
            JoinType::RightMark,
            Some(Box::new(col_lt_col_filter)),
        )
        .run_test(&[HjSmj, NljHj], false)
        .await
    }
}

type JoinFilterBuilder = Box<dyn Fn(Arc<Schema>, Arc<Schema>) -> JoinFilter>;

struct JoinFuzzTestCase {
    batch_sizes: &'static [usize],
    input1: Vec<RecordBatch>,
    input2: Vec<RecordBatch>,
    join_type: JoinType,
    join_filter_builder: Option<JoinFilterBuilder>,
}

impl JoinFuzzTestCase {
    fn new(
        input1: Vec<RecordBatch>,
        input2: Vec<RecordBatch>,
        join_type: JoinType,
        join_filter_builder: Option<JoinFilterBuilder>,
    ) -> Self {
        Self {
            batch_sizes: &[1, 2, 7, 49, 50, 51, 100],
            input1,
            input2,
            join_type,
            join_filter_builder,
        }
    }

    fn on_columns(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        vec![
            (
                Arc::new(Column::new_with_schema("a", &schema1).unwrap()) as _,
                Arc::new(Column::new_with_schema("a", &schema2).unwrap()) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b", &schema1).unwrap()) as _,
                Arc::new(Column::new_with_schema("b", &schema2).unwrap()) as _,
            ),
        ]
    }

    /// Helper function for building NLJoin filter, returning intermediate
    /// schema as a union of origin filter intermediate schema and
    /// on-condition schema
    fn intermediate_schema(&self) -> Schema {
        let filter_schema = if let Some(filter) = self.join_filter() {
            filter.schema().as_ref().to_owned()
        } else {
            Schema::empty()
        };

        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();

        let on_schema = Schema::new(vec![
            schema1
                .field_with_name("a")
                .unwrap()
                .to_owned()
                .with_nullable(true),
            schema1
                .field_with_name("b")
                .unwrap()
                .to_owned()
                .with_nullable(true),
            schema2.field_with_name("a").unwrap().to_owned(),
            schema2.field_with_name("b").unwrap().to_owned(),
        ]);

        Schema::new(
            filter_schema
                .fields
                .into_iter()
                .cloned()
                .chain(on_schema.fields.into_iter().cloned())
                .collect_vec(),
        )
    }

    /// Helper function for building NLJoin filter, returns the union
    /// of original filter expression and on-condition expression
    fn composite_filter_expression(&self) -> PhysicalExprRef {
        let (filter_expression, column_idx_offset) =
            if let Some(filter) = self.join_filter() {
                (
                    filter.expression().to_owned(),
                    filter.schema().fields().len(),
                )
            } else {
                (Arc::new(Literal::new(ScalarValue::from(true))) as _, 0)
            };

        let equal_a = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", column_idx_offset)),
            Operator::Eq,
            Arc::new(Column::new("a", column_idx_offset + 2)),
        ));
        let equal_b = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", column_idx_offset + 1)),
            Operator::Eq,
            Arc::new(Column::new("b", column_idx_offset + 3)),
        ));
        let on_expression = Arc::new(BinaryExpr::new(equal_a, Operator::And, equal_b));

        Arc::new(BinaryExpr::new(
            filter_expression,
            Operator::And,
            on_expression,
        ))
    }

    /// Helper function for building NLJoin filter, returning the union
    /// of original filter column indices and on-condition column indices.
    /// Result must match intermediate schema.
    fn column_indices(&self) -> Vec<ColumnIndex> {
        let mut column_indices = if let Some(filter) = self.join_filter() {
            filter.column_indices().to_vec()
        } else {
            vec![]
        };

        let on_column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];

        column_indices.extend(on_column_indices);
        column_indices
    }

    fn left_right(&self) -> (Arc<DataSourceExec>, Arc<DataSourceExec>) {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        let left = MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.input1),
            schema1,
            None,
        )
        .unwrap();
        let right = MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.input2),
            schema2,
            None,
        )
        .unwrap();
        (left, right)
    }

    fn join_filter(&self) -> Option<JoinFilter> {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        self.join_filter_builder
            .as_ref()
            .map(|builder| builder(schema1, schema2))
    }

    fn sort_merge_join(&self) -> Arc<SortMergeJoinExec> {
        let (left, right) = self.left_right();
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                self.on_columns().clone(),
                self.join_filter(),
                self.join_type,
                vec![SortOptions::default(); self.on_columns().len()],
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }

    fn hash_join(&self) -> Arc<HashJoinExec> {
        let (left, right) = self.left_right();
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                self.on_columns().clone(),
                self.join_filter(),
                &self.join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn nested_loop_join(&self) -> Arc<NestedLoopJoinExec> {
        let (left, right) = self.left_right();

        let column_indices = self.column_indices();
        let intermediate_schema = self.intermediate_schema();
        let expression = self.composite_filter_expression();

        let filter =
            JoinFilter::new(expression, column_indices, Arc::new(intermediate_schema));

        Arc::new(
            NestedLoopJoinExec::try_new(left, right, Some(filter), &self.join_type, None)
                .unwrap(),
        )
    }

    /// Perform joins tests on same inputs and verify outputs are equal
    /// `join_tests` - identifies what join types to test
    /// if `debug` flag is set the test will save randomly generated inputs and outputs to user folders,
    /// so it is easy to debug a test on top of the failed data
    async fn run_test(&self, join_tests: &[JoinTestType], debug: bool) {
        for batch_size in self.batch_sizes {
            let session_config = SessionConfig::new().with_batch_size(*batch_size);
            let ctx = SessionContext::new_with_config(session_config);
            let task_ctx = ctx.task_ctx();

            let hj = self.hash_join();
            let hj_collected = collect(hj, task_ctx.clone()).await.unwrap();

            let smj = self.sort_merge_join();
            let smj_collected = collect(smj, task_ctx.clone()).await.unwrap();

            let nlj = self.nested_loop_join();
            let nlj_collected = collect(nlj, task_ctx.clone()).await.unwrap();

            // Get actual row counts(without formatting overhead) for HJ and SMJ
            let hj_rows = hj_collected.iter().fold(0, |acc, b| acc + b.num_rows());
            let smj_rows = smj_collected.iter().fold(0, |acc, b| acc + b.num_rows());
            let nlj_rows = nlj_collected.iter().fold(0, |acc, b| acc + b.num_rows());

            // compare
            let smj_formatted =
                pretty_format_batches(&smj_collected).unwrap().to_string();
            let hj_formatted = pretty_format_batches(&hj_collected).unwrap().to_string();
            let nlj_formatted =
                pretty_format_batches(&nlj_collected).unwrap().to_string();

            let mut smj_formatted_sorted: Vec<&str> =
                smj_formatted.trim().lines().collect();
            smj_formatted_sorted.sort_unstable();

            let mut hj_formatted_sorted: Vec<&str> =
                hj_formatted.trim().lines().collect();
            hj_formatted_sorted.sort_unstable();

            let mut nlj_formatted_sorted: Vec<&str> =
                nlj_formatted.trim().lines().collect();
            nlj_formatted_sorted.sort_unstable();

            if debug
                && ((join_tests.contains(&NljHj) && nlj_rows != hj_rows)
                    || (join_tests.contains(&HjSmj) && smj_rows != hj_rows))
            {
                let fuzz_debug = "fuzz_test_debug";
                std::fs::remove_dir_all(fuzz_debug).unwrap_or(());
                std::fs::create_dir_all(fuzz_debug).unwrap();
                let out_dir_name = &format!("{fuzz_debug}/batch_size_{batch_size}");
                println!(
                    "Test result data mismatch found. HJ rows {hj_rows}, SMJ rows {smj_rows}, NLJ rows {nlj_rows}"
                );
                println!("The debug is ON. Input data will be saved to {out_dir_name}");

                Self::save_partitioned_batches_as_parquet(
                    &self.input1,
                    out_dir_name,
                    "input1",
                );
                Self::save_partitioned_batches_as_parquet(
                    &self.input2,
                    out_dir_name,
                    "input2",
                );

                if join_tests.contains(&NljHj) && nlj_rows != hj_rows {
                    println!("=============== HashJoinExec ==================");
                    for s in &hj_formatted_sorted {
                        println!("{s}");
                    }
                    println!("=============== NestedLoopJoinExec ==================");
                    for s in &nlj_formatted_sorted {
                        println!("{s}");
                    }
                    Self::save_partitioned_batches_as_parquet(
                        &nlj_collected,
                        out_dir_name,
                        "nlj",
                    );
                    Self::save_partitioned_batches_as_parquet(
                        &hj_collected,
                        out_dir_name,
                        "hj",
                    );
                }

                if join_tests.contains(&HjSmj) && smj_rows != hj_rows {
                    println!("=============== HashJoinExec ==================");
                    for s in &hj_formatted_sorted {
                        println!("{s}");
                    }
                    println!("=============== SortMergeJoinExec ==================");
                    for s in &smj_formatted_sorted {
                        println!("{s}");
                    }

                    Self::save_partitioned_batches_as_parquet(
                        &hj_collected,
                        out_dir_name,
                        "hj",
                    );
                    Self::save_partitioned_batches_as_parquet(
                        &smj_collected,
                        out_dir_name,
                        "smj",
                    );
                }
            }

            if join_tests.contains(&NljHj) {
                let err_msg_rowcnt = format!(
                    "NestedLoopJoinExec and HashJoinExec produced different row counts, batch_size: {batch_size}"
                );
                assert_eq!(nlj_rows, hj_rows, "{}", err_msg_rowcnt.as_str());
                if nlj_rows == 0 && hj_rows == 0 {
                    // both joins returned no rows, skip content comparison
                    continue;
                }

                let err_msg_contents = format!(
                    "NestedLoopJoinExec and HashJoinExec produced different results, batch_size: {batch_size}"
                );
                // row level compare if any of joins returns the result
                // the reason is different formatting when there is no rows
                for (i, (nlj_line, hj_line)) in nlj_formatted_sorted
                    .iter()
                    .zip(&hj_formatted_sorted)
                    .enumerate()
                {
                    assert_eq!(
                        (i, nlj_line),
                        (i, hj_line),
                        "{}",
                        err_msg_contents.as_str()
                    );
                }
            }

            if join_tests.contains(&HjSmj) {
                let err_msg_row_cnt = format!(
                    "HashJoinExec and SortMergeJoinExec produced different row counts, batch_size: {batch_size}"
                );
                assert_eq!(hj_rows, smj_rows, "{}", err_msg_row_cnt.as_str());

                let err_msg_contents = format!(
                    "SortMergeJoinExec and HashJoinExec produced different results, batch_size: {batch_size}"
                );
                // row level compare if any of joins returns the result
                // the reason is different formatting when there is no rows
                if smj_rows > 0 || hj_rows > 0 {
                    for (i, (smj_line, hj_line)) in smj_formatted_sorted
                        .iter()
                        .zip(&hj_formatted_sorted)
                        .enumerate()
                    {
                        assert_eq!(
                            (i, smj_line),
                            (i, hj_line),
                            "{}",
                            err_msg_contents.as_str()
                        );
                    }
                }
            }
        }
    }

    /// This method useful for debugging fuzz tests
    /// It helps to save randomly generated input test data for both join inputs into the user folder
    /// as a parquet files preserving partitioning.
    /// Once the data is saved it is possible to run a custom test on top of the saved data and debug
    ///
    /// #[tokio::test]
    /// async fn test1() {
    ///     let left: Vec<RecordBatch> = JoinFuzzTestCase::load_partitioned_batches_from_parquet("fuzz_test_debug/batch_size_2/input1").await.unwrap();
    ///     let right: Vec<RecordBatch> = JoinFuzzTestCase::load_partitioned_batches_from_parquet("fuzz_test_debug/batch_size_2/input2").await.unwrap();
    ///
    ///     JoinFuzzTestCase::new(
    ///         left,
    ///         right,
    ///         JoinType::LeftSemi,
    ///         Some(Box::new(col_lt_col_filter)),
    ///     )
    ///     .run_test(&[JoinTestType::HjSmj], false)
    ///     .await;
    /// }
    fn save_partitioned_batches_as_parquet(
        input: &[RecordBatch],
        output_dir: &str,
        out_name: &str,
    ) {
        let out_path = &format!("{output_dir}/{out_name}");
        std::fs::remove_dir_all(out_path).unwrap_or(());
        std::fs::create_dir_all(out_path).unwrap();

        input.iter().enumerate().for_each(|(idx, batch)| {
            let file_path = format!("{out_path}/file_{idx}.parquet");
            let mut file = std::fs::File::create(&file_path).unwrap();
            println!(
                "{}: Saving batch idx {} rows {} to parquet {}",
                out_name,
                idx,
                batch.num_rows(),
                file_path
            );
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                &mut file,
                input.first().unwrap().schema(),
                None,
            )
            .expect("creating writer");
            writer.write(batch).unwrap();
            writer.close().unwrap();
        });
    }

    /// Read parquet files preserving partitions, i.e. 1 file -> 1 partition
    /// Files can be of different sizes
    /// The method can be useful to read partitions have been saved by `save_partitioned_batches_as_parquet`
    /// for test debugging purposes
    #[expect(dead_code)]
    async fn load_partitioned_batches_from_parquet(
        dir: &str,
    ) -> std::io::Result<Vec<RecordBatch>> {
        let ctx: SessionContext = SessionContext::new();
        let mut batches: Vec<RecordBatch> = vec![];
        let mut entries = std::fs::read_dir(dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;

        // important to read files using the same order as they have been written
        // sort by modification time
        entries.sort_by_key(|path| {
            std::fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });

        for entry in entries {
            let path = entry.as_path();

            if path.is_file() {
                let mut batch = ctx
                    .read_parquet(
                        path.to_str().unwrap(),
                        datafusion::prelude::ParquetReadOptions::default(),
                    )
                    .await?
                    .collect()
                    .await?;

                batches.append(&mut batch);
            }
        }
        Ok(batches)
    }
}

/// Fuzz test: compare SMJ (with spilling) against HJ (no spill) for filtered
/// outer joins under memory pressure. This exercises the deferred filtering +
/// spill read-back path that unit tests can't easily cover with random data.
#[tokio::test]
async fn test_filtered_join_spill_fuzz() {
    let join_types = [JoinType::Left, JoinType::Right, JoinType::Full];

    let runtime_spill = RuntimeEnvBuilder::new()
        .with_memory_limit(4096, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()
        .unwrap();

    for join_type in &join_types {
        for (left_extra, right_extra) in [(true, true), (false, true), (true, false)] {
            let input1 = make_staggered_batches_i32(1000, left_extra);
            let input2 = make_staggered_batches_i32(1000, right_extra);

            let schema1 = input1[0].schema();
            let schema2 = input2[0].schema();
            let filter = col_lt_col_filter(schema1.clone(), schema2.clone());

            let on = vec![
                (
                    Arc::new(Column::new_with_schema("a", &schema1).unwrap()) as _,
                    Arc::new(Column::new_with_schema("a", &schema2).unwrap()) as _,
                ),
                (
                    Arc::new(Column::new_with_schema("b", &schema1).unwrap()) as _,
                    Arc::new(Column::new_with_schema("b", &schema2).unwrap()) as _,
                ),
            ];

            for batch_size in [2, 49, 100] {
                let session_config = SessionConfig::new().with_batch_size(batch_size);

                // HJ baseline (no memory limit)
                let left_hj = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&input1),
                    schema1.clone(),
                    None,
                )
                .unwrap();
                let right_hj = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&input2),
                    schema2.clone(),
                    None,
                )
                .unwrap();
                let hj = Arc::new(
                    HashJoinExec::try_new(
                        left_hj,
                        right_hj,
                        on.clone(),
                        Some(filter.clone()),
                        join_type,
                        None,
                        PartitionMode::Partitioned,
                        NullEquality::NullEqualsNothing,
                        false,
                    )
                    .unwrap(),
                );
                let ctx_hj = SessionContext::new_with_config(session_config.clone());
                let hj_collected = collect(hj, ctx_hj.task_ctx()).await.unwrap();

                // SMJ with spilling
                let left_smj = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&input1),
                    schema1.clone(),
                    None,
                )
                .unwrap();
                let right_smj = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&input2),
                    schema2.clone(),
                    None,
                )
                .unwrap();
                let smj = Arc::new(
                    SortMergeJoinExec::try_new(
                        left_smj,
                        right_smj,
                        on.clone(),
                        Some(filter.clone()),
                        *join_type,
                        vec![SortOptions::default(); on.len()],
                        NullEquality::NullEqualsNothing,
                    )
                    .unwrap(),
                );
                let task_ctx_spill = Arc::new(
                    TaskContext::default()
                        .with_session_config(session_config)
                        .with_runtime(Arc::clone(&runtime_spill)),
                );
                let smj_collected = collect(smj, task_ctx_spill).await.unwrap();

                let hj_rows: usize = hj_collected.iter().map(|b| b.num_rows()).sum();
                let smj_rows: usize = smj_collected.iter().map(|b| b.num_rows()).sum();

                assert_eq!(
                    hj_rows, smj_rows,
                    "Row count mismatch for {join_type:?} batch_size={batch_size} \
                     left_extra={left_extra} right_extra={right_extra}: \
                     HJ={hj_rows} SMJ={smj_rows}"
                );

                if hj_rows > 0 {
                    let hj_fmt =
                        pretty_format_batches(&hj_collected).unwrap().to_string();
                    let smj_fmt =
                        pretty_format_batches(&smj_collected).unwrap().to_string();

                    let mut hj_sorted: Vec<&str> = hj_fmt.trim().lines().collect();
                    hj_sorted.sort_unstable();
                    let mut smj_sorted: Vec<&str> = smj_fmt.trim().lines().collect();
                    smj_sorted.sort_unstable();

                    assert_eq!(
                        hj_sorted, smj_sorted,
                        "Content mismatch for {join_type:?} batch_size={batch_size} \
                         left_extra={left_extra} right_extra={right_extra}"
                    );
                }
            }
        }
    }
}

/// Return randomly sized record batches with:
/// two sorted int32 columns 'a', 'b' ranged from 0..99 as join columns
/// two random int32 columns 'x', 'y' as other columns
fn make_staggered_batches_i32(len: usize, with_extra_column: bool) -> Vec<RecordBatch> {
    let mut rng = rand::rng();
    let mut input12: Vec<(i32, i32)> = vec![(0, 0); len];
    let mut input3: Vec<i32> = vec![0; len];
    let mut input4: Vec<i32> = vec![0; len];
    for v in &mut input12 {
        *v = (rng.random_range(0..100), rng.random_range(0..100));
    }
    rng.fill(&mut input3[..]);
    rng.fill(&mut input4[..]);
    input12.sort_unstable();
    let input1 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.0));
    let input2 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.1));
    let input3 = Int32Array::from_iter(input3.into_iter().map(|v| {
        // ~10% NULLs in filter column to exercise NULL filter handling
        if rng.random_range(0..10) == 0 {
            None
        } else {
            Some(v)
        }
    }));
    let input4 = Int32Array::from_iter_values(input4);

    let mut columns = vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("x", Arc::new(input3) as ArrayRef),
    ];

    if with_extra_column {
        columns.push(("y", Arc::new(input4) as ArrayRef));
    }

    // split into several record batches
    let batch = RecordBatch::try_from_iter(columns).unwrap();

    // use a random number generator to pick a random sized output
    stagger_batch_with_seed(batch, 42)
}

fn rand_bytes<R: Rng>(rng: &mut R, min: usize, max: usize) -> Vec<u8> {
    let n = rng.random_range(min..=max);
    let mut v = vec![0u8; n];
    rng.fill(&mut v[..]);
    v
}

/// Return randomly sized record batches with:
/// two sorted binary columns 'a', 'b' (lexicographically) as join columns
/// two random binary columns 'x', 'y' as other columns
fn make_staggered_batches_binary(
    len: usize,
    with_extra_column: bool,
) -> Vec<RecordBatch> {
    let mut rng = rand::rng();

    // produce (a,b) pairs then sort lexicographically so SMJ has naturally sorted keys
    let mut input12: Vec<(Vec<u8>, Vec<u8>)> = (0..len)
        .map(|_| (rand_bytes(&mut rng, 4, 16), rand_bytes(&mut rng, 4, 16)))
        .collect();
    input12.sort_unstable(); // lexicographic on Vec<u8>

    // payload cols (also binary so the existing x < x filter is well-typed)
    let input3: Vec<Vec<u8>> = (0..len).map(|_| rand_bytes(&mut rng, 4, 24)).collect();
    let input4: Vec<Vec<u8>> = (0..len).map(|_| rand_bytes(&mut rng, 4, 24)).collect();

    let a = BinaryArray::from_iter_values(input12.iter().map(|k| &k.0));
    let b = BinaryArray::from_iter_values(input12.iter().map(|k| &k.1));
    let x = BinaryArray::from_iter_values(input3.iter());
    let y = BinaryArray::from_iter_values(input4.iter());

    let mut columns = vec![
        ("a", Arc::new(a) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
        ("x", Arc::new(x) as ArrayRef),
    ];

    if with_extra_column {
        columns.push(("y", Arc::new(y) as ArrayRef));
    }

    let batch = RecordBatch::try_from_iter(columns).unwrap();

    // preserve your existing randomized partitioning
    stagger_batch_with_seed(batch, 42)
}

// ---- Differential fuzz: PiecewiseMergeJoin existence joins vs NestedLoopJoin ----
//
// `PiecewiseMergeJoinExec` takes a range predicate and no equi keys, so it cannot be added to
// `JoinFuzzTestCase` above (that harness joins on `a` and `b` and folds the equality into the
// NestedLoopJoin filter). These tests use `NestedLoopJoinExec` with an equivalent filter as
// the oracle instead.
//
// What only randomization reaches: `LeftSemi`/`LeftAnti` record matches in a shared
// `AtomicUsize` watermark and emit once, from whichever streamed partition finishes last. The
// streamed side below is spread round-robin over several partitions as one-row batches, so
// batches arrive in an order no static test pins down, and the counter that gates the final
// pass is seeded from a partition count that deliberately disagrees with the `num_partitions`
// argument.

fn pwmj_kv_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int32, true),
    ]))
}

fn pwmj_kv_batch(ids: &[i32], keys: &[Option<i32>]) -> RecordBatch {
    RecordBatch::try_new(
        pwmj_kv_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(keys.to_vec())),
        ],
    )
    .unwrap()
}

/// Single-partition, single-batch input: the buffered side, and the oracle's probe side.
fn pwmj_single_exec(ids: &[i32], keys: &[Option<i32>]) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(
        &[vec![pwmj_kv_batch(ids, keys)]],
        pwmj_kv_schema(),
        None,
    )
    .unwrap()
}

/// Streamed side spread round-robin across `nparts` partitions, one row per batch.
fn pwmj_parts_exec(
    ids: &[i32],
    keys: &[Option<i32>],
    nparts: usize,
) -> Arc<dyn ExecutionPlan> {
    let nparts = nparts.max(1);
    let mut partitions: Vec<Vec<RecordBatch>> = vec![Vec::new(); nparts];
    for (row, (&id, key)) in ids.iter().zip(keys.iter()).enumerate() {
        partitions[row % nparts].push(pwmj_kv_batch(&[id], &[*key]));
    }
    for p in partitions.iter_mut() {
        if p.is_empty() {
            p.push(pwmj_kv_batch(&[], &[]));
        }
    }
    MemorySourceConfig::try_new_exec(&partitions, pwmj_kv_schema(), None).unwrap()
}

fn pwmj_plan(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    op: Operator,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    // Matches `PiecewiseMergeJoinExec::required_input_ordering`: descending for `<`/`<=`,
    // ascending for `>`/`>=`, NULLs first either way.
    let sort_options = match op {
        Operator::Lt | Operator::LtEq => SortOptions::new(true, true),
        Operator::Gt | Operator::GtEq => SortOptions::new(false, true),
        other => panic!("not a range operator: {other:?}"),
    };
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("k", 1)),
        sort_options,
    )])
    .unwrap();
    let sorted_left = Arc::new(SortExec::new(ordering, left));
    let on: (PhysicalExprRef, PhysicalExprRef) =
        (Arc::new(Column::new("k", 1)), Arc::new(Column::new("k", 1)));
    // `num_partitions` is 1 while the streamed side has up to 3: the final-pass counter must
    // come from the streamed side's partition count, not from this argument.
    Arc::new(
        PiecewiseMergeJoinExec::try_new(sorted_left, right, on, op, join_type, 1)
            .unwrap(),
    )
}

fn pwmj_nlj_oracle_plan(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    op: Operator,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let intermediate_schema = Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int32, true),
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int32, true),
    ]);
    let expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("k", 0)),
        op,
        Arc::new(Column::new("k", 1)),
    )) as PhysicalExprRef;
    let column_indices = vec![
        ColumnIndex {
            index: 1,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 1,
            side: JoinSide::Right,
        },
    ];
    let filter = JoinFilter::new(expr, column_indices, Arc::new(intermediate_schema));
    Arc::new(
        NestedLoopJoinExec::try_new(left, right, Some(filter), &join_type, None).unwrap(),
    )
}

/// Executes every output partition concurrently and returns the join's rows as
/// `(left id, right id)` pairs, sorted so partition interleaving does not affect the
/// comparison. `None` means the join filled that side with NULLs, or -- for the existence
/// joins, whose output carries the left side only -- that the side is absent entirely.
///
/// Concurrent rather than one partition at a time: the partitions share the watermark and race
/// to be the one that runs the final pass, which is the part a sequential drain cannot reach.
async fn pwmj_collect_id_pairs(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Vec<(Option<i32>, Option<i32>)> {
    let streams = (0..plan.output_partitioning().partition_count())
        .map(|partition| plan.execute(partition, Arc::clone(&task_ctx)).unwrap())
        .collect::<Vec<_>>();
    let per_partition =
        futures::future::join_all(streams.into_iter().map(|stream| {
            SpawnedTask::spawn(async move { common::collect(stream).await })
        }))
        .await;

    let id_column = |batch: &RecordBatch, col: usize| {
        batch
            .column(col)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone()
    };

    let mut pairs = Vec::new();
    for batches in per_partition {
        for batch in batches.unwrap().unwrap() {
            let left = id_column(&batch, 0);
            // Left is (id, k), so the right side's `id` follows it -- when there is one.
            let right = (batch.num_columns() > 2).then(|| id_column(&batch, 2));
            for row in 0..batch.num_rows() {
                pairs.push((
                    left.is_valid(row).then(|| left.value(row)),
                    right
                        .as_ref()
                        .filter(|r| r.is_valid(row))
                        .map(|r| r.value(row)),
                ));
            }
        }
    }
    pairs.sort_unstable();
    pairs
}

/// Differential test for every join type `PiecewiseMergeJoin` supports, against a
/// `NestedLoopJoin` oracle.
///
/// `Left`/`Full` are the ones with teeth: their unmatched buffered rows are derived from the
/// shared `min_marked` watermark rather than materialized per row, and that encoding is only
/// valid because every match marks a *suffix* of the buffered side. The dimensions the
/// cheaper tests do not reach are the ones that matter here -- `pwmj.slt` and the unit tests
/// both run the streamed side at a single partition and the default batch size, so neither
/// covers several partitions racing to run the final pass, nor the mid-scan resume path a
/// small batch size forces.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_pwmj_matches_nested_loop() {
    // A small batch size splits output across several coalesced batches even for these tiny
    // inputs, covering that boundary too.
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(3)),
    );
    let ops = [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq];
    let join_types = [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
    ];

    for seed in 0..60u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let left_len = rng.random_range(0..25usize);
        let right_len = rng.random_range(0..25usize);
        // A narrow key range forces duplicates and equal-boundary cases.
        let key_range = rng.random_range(1..6i32);
        let nparts = rng.random_range(1..4usize);

        let gen_keys = |n: usize, rng: &mut StdRng| -> Vec<Option<i32>> {
            (0..n)
                .map(|_| (!rng.random_bool(0.2)).then(|| rng.random_range(0..key_range)))
                .collect()
        };

        let left_ids: Vec<i32> = (0..left_len as i32).collect();
        let left_keys = gen_keys(left_len, &mut rng);
        let right_ids: Vec<i32> = (0..right_len as i32).collect();
        let right_keys = gen_keys(right_len, &mut rng);

        for op in ops {
            for join_type in join_types {
                let got = pwmj_collect_id_pairs(
                    pwmj_plan(
                        pwmj_single_exec(&left_ids, &left_keys),
                        pwmj_parts_exec(&right_ids, &right_keys, nparts),
                        op,
                        join_type,
                    ),
                    Arc::clone(&task_ctx),
                )
                .await;
                let want = pwmj_collect_id_pairs(
                    pwmj_nlj_oracle_plan(
                        pwmj_single_exec(&left_ids, &left_keys),
                        pwmj_single_exec(&right_ids, &right_keys),
                        op,
                        join_type,
                    ),
                    Arc::clone(&task_ctx),
                )
                .await;

                assert_eq!(
                    got, want,
                    "mismatch seed={seed} op={op:?} join_type={join_type:?} \
                     nparts={nparts} left_keys={left_keys:?} right_keys={right_keys:?}"
                );
            }
        }
    }
}

/// A named, deterministic companion to [`fuzz_pwmj_matches_nested_loop`] for the two join
/// types that read the `min_marked` watermark, with the expected output spelled out by hand.
///
/// The fuzz test covers these dimensions already, but only through seeds -- when it fails it
/// hands back a seed and a pair of generated key vectors to reconstruct from. These cases pin
/// the input instead: NULL keys on both sides, duplicate keys, a streamed side split across 3
/// partitions racing to run the final pass, and `batch_size = 3` to force the mid-scan resume
/// path. A regression in the watermark path therefore names itself.
///
/// Both branches of the encoding get a case. In the first, buffered `k` is `[1, 1, 2, NULL, 3]`
/// against streamed `[2, NULL, 3, 2, 5]`: buffered row 3 is the only unmatched left row and
/// streamed row 1 the only unmatched right row, both because a NULL key matches nothing.
/// Sorted descending for `<`, the buffered side is `[NULL, 3, 2, 1, 1]`, so the matched suffix
/// is `[1, 5)` and the unmatched prefix the single NULL row -- what `min_marked` has to encode.
/// The second case has no match at all, so `min_marked` is never lowered and stays
/// `usize::MAX`; the clamp against the buffered row count is what keeps the final pass from
/// slicing past the end.
#[tokio::test(flavor = "multi_thread")]
async fn pwmj_watermark_nulls_duplicates_multi_partition() {
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(3)),
    );
    let left_ids: Vec<i32> = vec![0, 1, 2, 3, 4];
    let left_keys = vec![Some(1), Some(1), Some(2), None, Some(3)];
    let nparts = 3;

    // The matched pairs are enumerated by hand from `left.k < right.k`; `Left` adds the
    // unmatched buffered rows and `Full` those plus the unmatched streamed ones.
    struct Case {
        name: &'static str,
        streamed_ids: Vec<i32>,
        streamed_keys: Vec<Option<i32>>,
        /// `(left id, right id)` pairs the predicate matches.
        matched: Vec<(i32, i32)>,
        unmatched_streamed: Vec<i32>,
    }

    let cases = vec![
        Case {
            name: "partial match",
            streamed_ids: vec![0, 1, 2, 3, 4],
            streamed_keys: vec![Some(2), None, Some(3), Some(2), Some(5)],
            matched: vec![
                // streamed 0 (k=2) and streamed 3 (k=2): buffered 0 and 1 (k=1)
                (0, 0),
                (1, 0),
                (0, 3),
                (1, 3),
                // streamed 2 (k=3): buffered 0, 1 (k=1) and buffered 2 (k=2)
                (0, 2),
                (1, 2),
                (2, 2),
                // streamed 4 (k=5): every non-NULL buffered row
                (0, 4),
                (1, 4),
                (2, 4),
                (4, 4),
            ],
            // streamed 1 (k=NULL) matches nothing
            unmatched_streamed: vec![1],
        },
        Case {
            // Nothing is below the smallest buffered key, so no buffered row is ever marked.
            name: "no match",
            streamed_ids: vec![0, 1, 2],
            streamed_keys: vec![Some(0), None, Some(1)],
            matched: vec![],
            unmatched_streamed: vec![0, 1, 2],
        },
    ];

    for case in cases {
        let matched_left: Vec<i32> = case.matched.iter().map(|&(l, _)| l).collect();
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = case
            .matched
            .iter()
            .map(|&(l, r)| (Some(l), Some(r)))
            .collect();
        // The unmatched buffered rows are the ones no streamed row paired with.
        pairs.extend(
            left_ids
                .iter()
                .filter(|id| !matched_left.contains(id))
                .map(|&id| (Some(id), None)),
        );

        for join_type in [JoinType::Left, JoinType::Full] {
            let mut want = pairs.clone();
            if join_type == JoinType::Full {
                want.extend(case.unmatched_streamed.iter().map(|&id| (None, Some(id))));
            }
            want.sort_unstable();

            let got = pwmj_collect_id_pairs(
                pwmj_plan(
                    pwmj_single_exec(&left_ids, &left_keys),
                    pwmj_parts_exec(&case.streamed_ids, &case.streamed_keys, nparts),
                    Operator::Lt,
                    join_type,
                ),
                Arc::clone(&task_ctx),
            )
            .await;

            let name = case.name;
            assert_eq!(got, want, "mismatch case={name} join_type={join_type:?}");
        }
    }
}
