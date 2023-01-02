// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The [PipelineFixer] rule tries to modify a given plan so that it can
//! accommodate its infinite sources, if there are any. In other words,
//! it tries to obtain a runnable query (with the given infinite sources)
//! from an non-runnable query by transforming pipeline-breaking operations
//! to pipeline-friendly ones. If this can not be done, the rule emits a
//! diagnostic error message.
//!
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::join_selection::swap_hash_join;
use crate::physical_optimizer::pipeline_checker::{
    check_finiteness_requirements, PipelineStatePropagator,
};
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::joins::utils::{JoinFilter, JoinSide};
use crate::physical_plan::joins::{
    HashJoinExec, PartitionMode, SortedFilterExpr, SymmetricHashJoinExec,
};
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::ExecutionPlan;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::JoinType;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion_physical_expr::physical_expr_visitor::{
    ExprVisitable, PhysicalExpressionVisitor, Recursion,
};
use datafusion_physical_expr::rewrite::TreeNodeRewritable as physical;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

use std::collections::HashMap;
use std::sync::Arc;

/// The [PipelineFixer] rule tries to modify a given plan so that it can
/// accommodate its infinite sources, if there are any. If this is not
/// possible, the rule emits a diagnostic error message.
#[derive(Default)]
pub struct PipelineFixer {}

impl PipelineFixer {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
type PipelineFixerSubrule =
    dyn Fn(&PipelineStatePropagator) -> Option<Result<PipelineStatePropagator>>;
impl PhysicalOptimizerRule for PipelineFixer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pipeline = PipelineStatePropagator::new(plan);
        let physical_optimizer_subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let state = pipeline.transform_up(&|p| {
            apply_subrules_and_check_finiteness_requirements(
                p,
                &physical_optimizer_subrules,
            )
        })?;
        Ok(state.plan)
    }

    fn name(&self) -> &str {
        "PipelineFixer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Interval calculations is supported by these operators. This will extend in the future.
fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::Lt
    )
}

/// Currently, integer data types are supported.
fn is_datatype_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::Int64
            | &DataType::Int32
            | &DataType::Int16
            | &DataType::Int8
            | &DataType::UInt64
            | &DataType::UInt32
            | &DataType::UInt16
            | &DataType::UInt8
    )
}

/// We do not support every type of [PhysicalExpr] for interval calculation. Also, we are not
/// supporting every type of [Operator]s in [BinaryExpr]. This check is subject to change while
/// we are adding additional [PhysicalExpr] and [Operator] support.
///
/// We support [CastExpr], [BinaryExpr], [Column], and [Literal] for interval calculations.
#[derive(Debug)]
pub struct UnsupportedPhysicalExprVisitor<'a> {
    /// Supported state
    pub supported: &'a mut bool,
}

impl PhysicalExpressionVisitor for UnsupportedPhysicalExprVisitor<'_> {
    fn pre_visit(self, expr: Arc<dyn PhysicalExpr>) -> Result<Recursion<Self>> {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let op_is_supported = is_operator_supported(binary_expr.op());
            *self.supported = *self.supported && op_is_supported;
        } else if !(expr.as_any().is::<Column>()
            || expr.as_any().is::<Literal>()
            || expr.as_any().is::<CastExpr>())
        {
            *self.supported = false;
        }
        Ok(Recursion::Continue(self))
    }
}

// If it does not contain any unsupported [Operator].
// Sorting information must cover every column in filter expression.
// Float is unsupported
// Anything beside than BinaryExpr, Col, Literal and operations Ge and Le is currently unsupported.
fn is_suitable_for_symmetric_hash_join(filter: &JoinFilter) -> Result<bool> {
    let expr: Arc<dyn PhysicalExpr> = filter.expression().clone();
    let mut is_expr_supported = true;
    expr.accept(UnsupportedPhysicalExprVisitor {
        supported: &mut is_expr_supported,
    })?;
    let is_fields_supported = filter
        .schema()
        .fields()
        .iter()
        .all(|f| is_datatype_supported(f.data_type()));
    Ok(is_expr_supported && is_fields_supported)
}

// TODO: Implement CollectLeft, CollectRight and CollectBoth modes for SymmetricHashJoin
fn enforce_symmetric_hash_join(
    hash_join: &HashJoinExec,
    sorted_columns: Vec<SortedFilterExpr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    let new_join = Arc::new(SymmetricHashJoinExec::try_new(
        Arc::clone(left),
        Arc::clone(right),
        hash_join
            .on()
            .iter()
            .map(|(l, r)| (l.clone(), r.clone()))
            .collect(),
        hash_join.filter().unwrap().clone(),
        sorted_columns,
        hash_join.join_type(),
        hash_join.null_equals_null(),
    )?);
    Ok(new_join)
}

/// We assume that the sort information is on a column, not a binary expr.
/// If a + b is sorted, we can not find intervals for 'a" and 'b".
///
/// Here, we map the main column information to filter intermediate schema.
/// child_orders are derived from child schemas. However, filter schema is different. We enforce
/// that we get the column order for each corresponding [ColumnIndex]. If not, we return None and
/// assume this expression is not capable of pruning.
fn build_filter_input_order(
    filter: &JoinFilter,
    left_main_schema: SchemaRef,
    right_main_schema: SchemaRef,
    child_orders: Vec<Option<&[PhysicalSortExpr]>>,
) -> Result<Option<Vec<SortedFilterExpr>>> {
    let left_child = child_orders[0];
    let right_child = child_orders[1];
    let mut left_hashmap: HashMap<Column, Column> = HashMap::new();
    let mut right_hashmap: HashMap<Column, Column> = HashMap::new();
    if left_child.is_none() || right_child.is_none() {
        return Ok(None);
    }
    let left_sort_information = left_child.unwrap();
    let right_sort_information = right_child.unwrap();
    for (filter_schema_index, index) in filter.column_indices().iter().enumerate() {
        if index.side.eq(&JoinSide::Left) {
            let main_field = left_main_schema.field(index.index);
            let main_col =
                Column::new_with_schema(main_field.name(), left_main_schema.as_ref())?;
            let filter_field = filter.schema().field(filter_schema_index);
            let filter_col = Column::new(filter_field.name(), filter_schema_index);
            left_hashmap.insert(main_col, filter_col);
        } else {
            let main_field = right_main_schema.field(index.index);
            let main_col =
                Column::new_with_schema(main_field.name(), right_main_schema.as_ref())?;
            let filter_field = filter.schema().field(filter_schema_index);
            let filter_col = Column::new(filter_field.name(), filter_schema_index);
            right_hashmap.insert(main_col, filter_col);
        }
    }
    let mut sorted_exps = vec![];
    for sort_expr in left_sort_information {
        let expr = sort_expr.expr.clone();
        let new_expr =
            expr.transform_up(&|p| convert_filter_columns(p, &left_hashmap))?;
        sorted_exps.push(SortedFilterExpr::new(
            JoinSide::Left,
            sort_expr.expr.clone(),
            new_expr.clone(),
            sort_expr.options,
        ));
    }
    for sort_expr in right_sort_information {
        let expr = sort_expr.expr.clone();
        let new_expr =
            expr.transform_up(&|p| convert_filter_columns(p, &right_hashmap))?;
        sorted_exps.push(SortedFilterExpr::new(
            JoinSide::Right,
            sort_expr.expr.clone(),
            new_expr.clone(),
            sort_expr.options,
        ));
    }
    Ok(Some(sorted_exps))
}

fn convert_filter_columns(
    input: Arc<dyn PhysicalExpr>,
    column_change_information: &HashMap<Column, Column>,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    if let Some(col) = input.as_any().downcast_ref::<Column>() {
        let filter_col = column_change_information.get(col).unwrap().clone();
        Ok(Some(Arc::new(filter_col)))
    } else {
        Ok(Some(input))
    }
}

fn hash_join_convert_symmetric_subrule(
    input: &PipelineStatePropagator,
) -> Option<Result<PipelineStatePropagator>> {
    let plan = input.plan.clone();
    let children = &input.children_unbounded;
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let (left_unbounded, right_unbounded) = (children[0], children[1]);
        let new_plan = if left_unbounded && right_unbounded {
            match hash_join.filter() {
                Some(filter) => match is_suitable_for_symmetric_hash_join(filter) {
                    Ok(suitable) => {
                        if suitable {
                            let children = input.plan.children();
                            let input_order = children
                                .iter()
                                .map(|c| c.output_ordering())
                                .collect::<Vec<_>>();
                            match build_filter_input_order(
                                filter,
                                hash_join.left().schema(),
                                hash_join.right().schema(),
                                input_order,
                            ) {
                                Ok(Some(sort_columns)) => {
                                    enforce_symmetric_hash_join(hash_join, sort_columns)
                                }
                                Ok(None) => Ok(plan),
                                Err(e) => return Some(Err(e)),
                            }
                        } else {
                            Ok(plan)
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                None => Ok(plan),
            }
        } else {
            Ok(plan)
        };
        let new_state = new_plan.map(|plan| PipelineStatePropagator {
            plan,
            unbounded: left_unbounded || right_unbounded,
            children_unbounded: vec![left_unbounded, right_unbounded],
        });
        Some(new_state)
    } else {
        None
    }
}

/// This subrule will swap build/probe sides of a hash join depending on whether its inputs
/// may produce an infinite stream of records. The rule ensures that the left (build) side
/// of the hash join always operates on an input stream that will produce a finite set of.
/// records If the left side can not be chosen to be "finite", the order stays the
/// same as the original query.
/// ```text
/// For example, this rule makes the following transformation:
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Infinite     |    true      | Hash         |\true
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              |       |              |
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Finite       |    false     | Hash         |/false
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Finite       |    false     | Hash         |\false
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              | true  |              | true
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |-----
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Infinite     |    true      | Hash         |/true
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
/// ```
fn hash_join_swap_subrule(
    input: &PipelineStatePropagator,
) -> Option<Result<PipelineStatePropagator>> {
    let plan = input.plan.clone();
    let children = &input.children_unbounded;
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let (left_unbounded, right_unbounded) = (children[0], children[1]);
        let new_plan = if left_unbounded && !right_unbounded {
            if matches!(
                *hash_join.join_type(),
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
            ) {
                swap(hash_join)
            } else {
                Ok(plan)
            }
        } else {
            Ok(plan)
        };
        let new_state = new_plan.map(|plan| PipelineStatePropagator {
            plan,
            unbounded: left_unbounded || right_unbounded,
            children_unbounded: vec![left_unbounded, right_unbounded],
        });
        Some(new_state)
    } else {
        None
    }
}

/// This function swaps sides of a hash join to make it runnable even if one of its
/// inputs are infinite. Note that this is not always possible; i.e. [JoinType::Full],
/// [JoinType::Right], [JoinType::RightAnti] and [JoinType::RightSemi] can not run with
/// an unbounded left side, even if we swap. Therefore, we do not consider them here.
fn swap(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (
            _,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full,
        ) => Err(DataFusionError::Internal(format!(
            "{join_type} join cannot be swapped for unbounded input."
        ))),
        (PartitionMode::Partitioned, _) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, _) => {
            swap_hash_join(hash_join, PartitionMode::CollectLeft)
        }
        (PartitionMode::Auto, _) => Err(DataFusionError::Internal(
            "Auto is not acceptable for unbounded input here.".to_string(),
        )),
    }
}

fn apply_subrules_and_check_finiteness_requirements(
    mut input: PipelineStatePropagator,
    physical_optimizer_subrules: &Vec<Box<PipelineFixerSubrule>>,
) -> Result<Option<PipelineStatePropagator>> {
    for sub_rule in physical_optimizer_subrules {
        if let Some(value) = sub_rule(&input).transpose()? {
            input = value;
        }
    }
    check_finiteness_requirements(input)
}

#[cfg(test)]
mod hash_join_tests {
    use super::*;
    use crate::physical_optimizer::join_selection::swap_join_type;
    use crate::physical_optimizer::test_utils::SourceType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::{physical_plan::joins::PartitionMode, test::exec::UnboundedExec};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    struct TestCase {
        case: String,
        initial_sources_unbounded: (SourceType, SourceType),
        initial_join_type: JoinType,
        initial_mode: PartitionMode,
        expected_sources_unbounded: (SourceType, SourceType),
        expected_join_type: JoinType,
        expected_mode: PartitionMode,
        expecting_swap: bool,
    }

    #[tokio::test]
    async fn test_join_with_swap_full() -> Result<()> {
        // NOTE: Currently, some initial conditions are not viable after join order selection.
        //       For example, full join always comes in partitioned mode. See the warning in
        //       function "swap". If this changes in the future, we should update these tests.
        let cases = vec![
            TestCase {
                case: "Bounded - Unbounded 1".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Bounded 2".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Bounded - Bounded 3".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Unbounded 4".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
        ];
        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cases_without_collect_left_check() -> Result<()> {
        let mut cases = vec![];
        let join_types = vec![JoinType::LeftSemi, JoinType::Inner];
        for join_type in join_types {
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_support_collect_left() -> Result<()> {
        let mut cases = vec![];
        // After [JoinSelection] optimization, these join types cannot run in CollectLeft mode except
        // [JoinType::LeftSemi]
        let the_ones_not_support_collect_left = vec![JoinType::Left, JoinType::LeftAnti];
        for join_type in the_ones_not_support_collect_left {
            cases.push(TestCase {
                case: "Unbounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_supporting_swaps_possible_collect_left() -> Result<()> {
        let mut cases = vec![];
        let the_ones_not_support_collect_left =
            vec![JoinType::Right, JoinType::RightAnti, JoinType::RightSemi];
        for join_type in the_ones_not_support_collect_left {
            // We expect that (SourceType::Unbounded, SourceType::Bounded) will change, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // We expect that (SourceType::Bounded, SourceType::Unbounded) will stay same, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            //
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // If cases are partitioned, only unbounded & bounded check will affect the order.
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }
    #[allow(clippy::vtable_address_comparisons)]
    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) -> Result<()> {
        let left_unbounded = t.initial_sources_unbounded.0 == SourceType::Unbounded;
        let right_unbounded = t.initial_sources_unbounded.1 == SourceType::Unbounded;
        let left_exec = Arc::new(UnboundedExec::new(
            left_unbounded,
            Schema::new(vec![Field::new("a", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundedExec::new(
            right_unbounded,
            Schema::new(vec![Field::new("b", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;

        let join = HashJoinExec::try_new(
            Arc::clone(&left_exec),
            Arc::clone(&right_exec),
            vec![(
                Column::new_with_schema("a", &left_exec.schema())?,
                Column::new_with_schema("b", &right_exec.schema())?,
            )],
            None,
            &t.initial_join_type,
            t.initial_mode,
            &false,
        )?;

        let initial_hash_join_state = PipelineStatePropagator {
            plan: Arc::new(join),
            unbounded: false,
            children_unbounded: vec![left_unbounded, right_unbounded],
        };
        let optimized_hash_join =
            hash_join_swap_subrule(&initial_hash_join_state).unwrap()?;
        let optimized_join_plan = optimized_hash_join.plan;

        // If swap did happen
        let projection_added = optimized_join_plan.as_any().is::<ProjectionExec>();
        let plan = if projection_added {
            let proj = optimized_join_plan
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            proj.input().clone()
        } else {
            optimized_join_plan
        };

        if let Some(HashJoinExec {
            left,
            right,
            join_type,
            mode,
            ..
        }) = plan.as_any().downcast_ref::<HashJoinExec>()
        {
            let left_changed = Arc::ptr_eq(left, &right_exec);
            let right_changed = Arc::ptr_eq(right, &left_exec);
            // If this is not equal, we have a bigger problem.
            assert_eq!(left_changed, right_changed);
            assert_eq!(
                (
                    t.case.as_str(),
                    if left.unbounded_output(&[])? {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    if right.unbounded_output(&[])? {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    join_type,
                    mode,
                    left_changed && right_changed
                ),
                (
                    t.case.as_str(),
                    t.expected_sources_unbounded.0,
                    t.expected_sources_unbounded.1,
                    &t.expected_join_type,
                    &t.expected_mode,
                    t.expecting_swap
                )
            );
        };
        Ok(())
    }
}
