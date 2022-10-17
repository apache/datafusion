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

//! Enforcement optimizer rules are used to make sure the plan's Distribution and Ordering
//! requirements are met by inserting necessary [[RepartitionExec]] and [[SortExec]].
//!
use crate::error::Result;
use crate::physical_optimizer::utils::transform_up;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use crate::prelude::SessionConfig;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    normalize_sort_expr_with_equivalence_properties, PhysicalSortExpr,
};
use std::sync::Arc;

/// BasicEnforcement rule, it ensures the Distribution and Ordering requirements are met
/// in the strictest way. It might add additional [[RepartitionExec]] to the plan tree
/// and give a non-optimal plan, but it can avoid the possible data skew in joins
///
/// For example for a HashJoin with keys(a, b, c), the required Distribution(a, b, c) can be satisfied by
/// several alternative partitioning ways: [(a, b, c), (a, b), (a, c), (b, c), (a), (b), (c), ( )].
///
/// This rule only chooses the exactly match and satisfies the Distribution(a, b, c) by a HashPartition(a, b, c).
#[derive(Default)]
pub struct BasicEnforcement {}

impl BasicEnforcement {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for BasicEnforcement {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Distribution and Ordering enforcement need to be applied bottom-up.
        let target_partitions = config.target_partitions;
        transform_up(plan, &{
            |plan| Some(ensure_distribution_and_ordering(plan, target_partitions))
        })
    }

    fn name(&self) -> &str {
        "BasicEnforcement"
    }
}

fn ensure_distribution_and_ordering(
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
    target_partitions: usize,
) -> Arc<dyn crate::physical_plan::ExecutionPlan> {
    if plan.children().is_empty() {
        return plan;
    }
    let required_input_distributions = plan.required_input_distribution();
    let required_input_orderings = plan.required_input_ordering();
    let children: Vec<Arc<dyn ExecutionPlan>> = plan.children();
    assert_eq!(children.len(), required_input_distributions.len());
    assert_eq!(children.len(), required_input_orderings.len());

    // Add RepartitionExec to guarantee output partitioning
    let children = children
        .into_iter()
        .zip(required_input_distributions.into_iter())
        .map(|(child, required)| {
            if child
                .output_partitioning()
                .satisfy(required.clone(), || child.equivalence_properties())
            {
                child
            } else {
                let new_child: Arc<dyn ExecutionPlan> = match required {
                    Distribution::SinglePartition
                        if child.output_partitioning().partition_count() > 1 =>
                    {
                        Arc::new(CoalescePartitionsExec::new(child.clone()))
                    }
                    _ => {
                        let partition = required.create_partitioning(target_partitions);
                        Arc::new(RepartitionExec::try_new(child, partition).unwrap())
                    }
                };
                new_child
            }
        });

    // Add SortExec to guarantee output ordering
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .zip(required_input_orderings.into_iter())
        .map(|(child, required)| {
            if ordering_satisfy(child.output_ordering(), required, || {
                child.equivalence_properties()
            }) {
                child
            } else {
                let sort_expr = required.unwrap().to_vec();
                if child.output_partitioning().partition_count() > 1 {
                    Arc::new(SortExec::new_with_partitioning(
                        sort_expr, child, true, None,
                    ))
                } else {
                    Arc::new(SortExec::try_new(sort_expr, child, None).unwrap())
                }
            }
        })
        .collect::<Vec<_>>();

    with_new_children_if_necessary(plan, new_children).unwrap()
}

/// DynamicEnforcement rule
///
///
#[derive(Default)]
pub struct DynamicEnforcement {}

// TODO
impl DynamicEnforcement {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Check the required ordering requirements are satisfied by the provided PhysicalSortExprs.
fn ordering_satisfy<F: FnOnce() -> Vec<Vec<Column>>>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortExpr]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            if required.len() > provided.len() {
                false
            } else {
                let fast_match = required
                    .iter()
                    .zip(provided.iter())
                    .all(|(order1, order2)| order1.eq(order2));

                if !fast_match {
                    let eq_properties = equal_properties();
                    if !eq_properties.is_empty() {
                        let normalized_required_exprs = required
                            .iter()
                            .map(|e| {
                                normalize_sort_expr_with_equivalence_properties(
                                    e.clone(),
                                    &eq_properties,
                                )
                            })
                            .collect::<Vec<_>>();
                        let normalized_provided_exprs = provided
                            .iter()
                            .map(|e| {
                                normalize_sort_expr_with_equivalence_properties(
                                    e.clone(),
                                    &eq_properties,
                                )
                            })
                            .collect::<Vec<_>>();
                        normalized_required_exprs
                            .iter()
                            .zip(normalized_provided_exprs.iter())
                            .all(|(order1, order2)| order1.eq(order2))
                    } else {
                        fast_match
                    }
                } else {
                    fast_match
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{expressions, PhysicalExpr};

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::hash_join::{HashJoinExec, PartitionMode};
    use crate::physical_plan::join_utils::JoinOn;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sort_merge_join::SortMergeJoinExec;
    use crate::physical_plan::{displayable, Statistics};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]))
    }

    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            None,
            None,
        ))
    }

    fn projection_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut exprs = vec![];
        for (column, alias) in alias_pairs.iter() {
            exprs.push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
        for (column, alias) in alias_pairs.iter() {
            group_by_expr
                .push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        let group_by = PhysicalGroupBy::new_single(group_by_expr.clone());

        let final_group_by_expr = group_by_expr
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                (
                    Arc::new(expressions::Column::new(name, index))
                        as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let final_grouping = PhysicalGroupBy::new_single(final_group_by_expr);

        Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                final_grouping,
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        group_by,
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn hash_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                None,
                join_type,
                PartitionMode::Partitioned,
                &false,
            )
            .unwrap(),
        )
    }

    fn sort_merge_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                *join_type,
                vec![SortOptions::default(); join_on.len()],
                false,
            )
            .unwrap(),
        )
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = BasicEnforcement {};
            let optimized = optimizer
                .optimize($PLAN, &SessionConfig::new().with_target_partitions(10))?;

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent().to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn muti_hash_joins() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
        ];

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];

        // Join on (a == c)
        let top_join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        for join_type in join_types {
            let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let top_join =
                hash_join_exec(join.clone(), right.clone(), &top_join_on, &join_type);

            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"c\", index: 2 }})]", join_type);
            let join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"b\", index: 1 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs
                JoinType::Inner | JoinType::Left => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);

            // This time we use (b == c) for top join
            // Join on (b == c)
            let top_join_on = vec![(
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("c", &schema()).unwrap(),
            )];

            let top_join = hash_join_exec(join, right.clone(), &top_join_on, &join_type);
            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={}, on=[(Column {{ name: \"b\", index: 1 }}, Column {{ name: \"c\", index: 2 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs
                JoinType::Inner | JoinType::Right => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    join_plan.as_str(),
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);
        }

        Ok(())
    }

    #[test]
    fn muti_joins_after_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(as as a1, a as a2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("a".to_string(), "a2".to_string()),
        ];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Join on (a1 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a1", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(
            projection.clone(),
            right.clone(),
            &top_join_on,
            &JoinType::Inner,
        );

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a1\", index: 0 }, Column { name: \"c\", index: 2 })]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, top_join);

        // Join on (a2 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a2", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a2\", index: 1 }, Column { name: \"c\", index: 2 })]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join);
        Ok(())
    }

    #[test]
    fn muti_joins_after_multi_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];

        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(c as c1)
        let alias_pairs: Vec<(String, String)> =
            vec![("c".to_string(), "c1".to_string())];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Projection(c1 as a)
        let alias_pairs: Vec<(String, String)> =
            vec![("c1".to_string(), "a".to_string())];
        let projection2 = projection_exec_with_alias(projection, alias_pairs);

        // Join on (a == c)
        let top_join_on = vec![(
            Column::new_with_schema("a", &projection2.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection2, right, &top_join_on, &JoinType::Inner);

        // The Column 'a' has different meaning now after the two Projections
        // The original Output partition can not satisfy the Join requirements and need to add an additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"c\", index: 2 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ProjectionExec: expr=[c1@0 as a]",
            "ProjectionExec: expr=[c@2 as c1]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a\", index: 0 }, Column { name: \"b\", index: 1 })]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join);
        Ok(())
    }

    #[test]
    fn join_after_agg_alias() -> Result<()> {
        // group by (a as a1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );
        // group by (a as a2)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a2".to_string())],
        );

        // Join on (a1 == a2)
        let join_on = vec![(
            Column::new_with_schema("a1", &left.schema()).unwrap(),
            Column::new_with_schema("a2", &right.schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"a1\", index: 0 }, Column { name: \"a2\", index: 0 })]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a1\", index: 0 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 0 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
        Ok(())
    }

    #[test]
    fn hash_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        // Join on (b1 == b && a1 == a)
        let join_on = vec![
            (
                Column::new_with_schema("b1", &left.schema()).unwrap(),
                Column::new_with_schema("b", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a1", &left.schema()).unwrap(),
                Column::new_with_schema("a", &right.schema()).unwrap(),
            ),
        ];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"b1\", index: 1 }, Column { name: \"b\", index: 0 }), (Column { name: \"a1\", index: 0 }, Column { name: \"a\", index: 1 })]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1, b1@1 as b1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a1\", index: 0 }, Column { name: \"b1\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a1, b@1 as b1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 0 }, Column { name: \"a\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
        Ok(())
    }

    #[test]
    fn muti_smj_joins() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
        ];

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];

        // Join on (a == c)
        let top_join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let top_join = sort_merge_join_exec(
                join.clone(),
                right.clone(),
                &top_join_on,
                &join_type,
            );

            let top_join_plan =
                format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"c\", index: 2 }})]", join_type);
            let join_plan =
                format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"a\", index: 0 }}, Column {{ name: \"b\", index: 1 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs 3 SortExecs
                JoinType::Inner | JoinType::Left => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);

            // This time we use (b == c) for top join
            // Join on (b == c)
            let top_join_on = vec![(
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("c", &schema()).unwrap(),
            )];

            let top_join =
                sort_merge_join_exec(join, right.clone(), &top_join_on, &join_type);
            let top_join_plan =
                format!("SortMergeJoin: join_type={}, on=[(Column {{ name: \"b\", index: 1 }}, Column {{ name: \"c\", index: 2 }})]", join_type);

            let expected = match join_type {
                // Should include 3 RepartitionExecs and 3 SortExecs
                JoinType::Inner | JoinType::Right => vec![
                    top_join_plan.as_str(),
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
                // Should include 4 RepartitionExecs and 4 SortExecs
                _ => vec![
                    top_join_plan.as_str(),
                    "SortExec: [b@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    join_plan.as_str(),
                    "SortExec: [a@0 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [b@1 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 1 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                    "SortExec: [c@2 ASC]",
                    "RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 10)",
                    "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join);
        }

        Ok(())
    }

    #[test]
    fn smj_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        // Join on (b1 == b && a1 == a)
        let join_on = vec![
            (
                Column::new_with_schema("b1", &left.schema()).unwrap(),
                Column::new_with_schema("b", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a1", &left.schema()).unwrap(),
                Column::new_with_schema("a", &right.schema()).unwrap(),
            ),
        ];
        let join = sort_merge_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "SortMergeJoin: join_type=Inner, on=[(Column { name: \"b1\", index: 1 }, Column { name: \"b\", index: 0 }), (Column { name: \"a1\", index: 0 }, Column { name: \"a\", index: 1 })]",
            "SortExec: [b1@1 ASC,a1@0 ASC]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1, b1@1 as b1], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"a1\", index: 0 }, Column { name: \"b1\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[a@0 as a1, b@1 as b1], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
            "SortExec: [b@0 ASC,a@1 ASC]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([Column { name: \"b\", index: 0 }, Column { name: \"a\", index: 1 }], 10)",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "ParquetExec: limit=None, partitions=[x], projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join);
        Ok(())
    }
}
