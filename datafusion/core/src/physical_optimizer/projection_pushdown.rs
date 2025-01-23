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

//! This file implements the `ProjectionPushdown` physical optimization rule.
//! The function [`remove_unnecessary_projections`] tries to push down all
//! projections one by one if the operator below is amenable to this. If a
//! projection reaches a source, it can even disappear from the plan entirely.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::ExecutionPlan;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::PhysicalExpr;

use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::projection::{update_expr, ProjectionExec};

/// This rule inspects [`ProjectionExec`]'s in the given physical plan and tries to
/// remove or swap with its child.
#[derive(Default, Debug)]
pub struct ProjectionPushdown {}

impl ProjectionPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(remove_unnecessary_projections).data()
    }

    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function checks if `plan` is a [`ProjectionExec`], and inspects its
/// input(s) to test whether it can push `plan` under its input(s). This function
/// will operate on the entire tree and may ultimately remove `plan` entirely
/// by leveraging source providers with built-in projection capabilities.
pub fn remove_unnecessary_projections(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified = if let Some(projection) =
        plan.as_any().downcast_ref::<ProjectionExec>()
    {
        // If the projection does not cause any change on the input, we can
        // safely remove it:
        if is_projection_removable(projection) {
            return Ok(Transformed::yes(Arc::clone(projection.input())));
        }
        // If it does, check if we can push it under its child(ren):
        let input = projection.input().as_any();
        if let Some(child_projection) = input.downcast_ref::<ProjectionExec>() {
            let maybe_unified = try_unifying_projections(projection, child_projection)?;
            return if let Some(new_plan) = maybe_unified {
                // To unify 3 or more sequential projections:
                remove_unnecessary_projections(new_plan)
                    .data()
                    .map(Transformed::yes)
            } else {
                Ok(Transformed::no(plan))
            };
        } else {
            projection
                .input()
                .try_swapping_with_projection(projection)?
        }
    } else {
        return Ok(Transformed::no(plan));
    };

    Ok(maybe_modified.map_or(Transformed::no(plan), Transformed::yes))
}

/// Unifies `projection` with its input (which is also a [`ProjectionExec`]).
fn try_unifying_projections(
    projection: &ProjectionExec,
    child: &ProjectionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut projected_exprs = vec![];
    let mut column_ref_map: HashMap<Column, usize> = HashMap::new();

    // Collect the column references usage in the outer projection.
    projection.expr().iter().for_each(|(expr, _)| {
        expr.apply(|expr| {
            Ok({
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    *column_ref_map.entry(column.clone()).or_default() += 1;
                }
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
    });

    // Merging these projections is not beneficial, e.g
    // If an expression is not trivial and it is referred more than 1, unifies projections will be
    // beneficial as caching mechanism for non-trivial computations.
    // See discussion in: https://github.com/apache/datafusion/issues/8296
    if column_ref_map.iter().any(|(column, count)| {
        *count > 1 && !is_expr_trivial(&Arc::clone(&child.expr()[column.index()].0))
    }) {
        return Ok(None);
    }

    for (expr, alias) in projection.expr() {
        // If there is no match in the input projection, we cannot unify these
        // projections. This case will arise if the projection expression contains
        // a `PhysicalExpr` variant `update_expr` doesn't support.
        let Some(expr) = update_expr(expr, child.expr(), true)? else {
            return Ok(None);
        };
        projected_exprs.push((expr, alias.clone()));
    }

    ProjectionExec::try_new(projected_exprs, Arc::clone(child.input()))
        .map(|e| Some(Arc::new(e) as _))
}

/// Checks if the given expression is trivial.
/// An expression is considered trivial if it is either a `Column` or a `Literal`.
fn is_expr_trivial(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any().downcast_ref::<Column>().is_some()
        || expr.as_any().downcast_ref::<Literal>().is_some()
}

/// Compare the inputs and outputs of the projection. All expressions must be
/// columns without alias, and projection does not change the order of fields.
/// For example, if the input schema is `a, b`, `SELECT a, b` is removable,
/// but `SELECT b, a` and `SELECT a+1, b` and `SELECT a AS c, b` are not.
fn is_projection_removable(projection: &ProjectionExec) -> bool {
    let exprs = projection.expr();
    exprs.iter().enumerate().all(|(idx, (expr, alias))| {
        let Some(col) = expr.as_any().downcast_ref::<Column>() else {
            return false;
        };
        col.name() == alias && col.index() == idx
    }) && exprs.len() == projection.input().schema().fields().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    use crate::datasource::physical_plan::CsvExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion_physical_expr::{
        Distribution, Partitioning, PhysicalExpr, PhysicalSortExpr,
        PhysicalSortRequirement,
    };
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
    use datafusion_physical_optimizer::output_requirements::OutputRequirementExec;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion_physical_plan::streaming::StreamingTableExec;
    use datafusion_physical_plan::union::UnionExec;

    use crate::datasource::file_format::file_compression_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::physical_plan::FileScanConfig;
    use crate::physical_plan::get_plan_string;
    use crate::physical_plan::joins::{
        HashJoinExec, NestedLoopJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
    };

    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
    use datafusion_common::{JoinSide, JoinType, ScalarValue};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_expr::{
        ColumnarValue, Operator, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion_physical_expr::expressions::{
        binary, col, BinaryExpr, CaseExpr, CastExpr, NegativeExpr,
    };
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_plan::joins::PartitionMode;
    use datafusion_physical_plan::streaming::PartitionStream;

    use itertools::Itertools;

    /// Mocked UDF
    #[derive(Debug)]
    struct DummyUDF {
        signature: Signature,
    }

    impl DummyUDF {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for DummyUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "dummy_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_batch(
            &self,
            _args: &[ColumnarValue],
            _number_rows: usize,
        ) -> Result<ColumnarValue> {
            unimplemented!("DummyUDF::invoke")
        }
    }

    #[test]
    fn test_update_matching_exprs() -> Result<()> {
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 3)),
                Operator::Divide,
                Arc::new(Column::new("e", 5)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 3)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 4)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(ScalarUDF::new_from_impl(DummyUDF::new())),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 0)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 2))),
                vec![
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 2)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 5)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 5)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 2)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 3)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 5)),
                ))),
            )?),
        ];
        let child: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("c", 2)), "c".to_owned()),
            (Arc::new(Column::new("b", 1)), "b".to_owned()),
            (Arc::new(Column::new("d", 3)), "d".to_owned()),
            (Arc::new(Column::new("a", 0)), "a".to_owned()),
            (Arc::new(Column::new("f", 5)), "f".to_owned()),
            (Arc::new(Column::new("e", 4)), "e".to_owned()),
        ];

        let expected_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Divide,
                Arc::new(Column::new("e", 4)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 0)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 5)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(ScalarUDF::new_from_impl(DummyUDF::new())),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 2)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 3))),
                vec![
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 3)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 4)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 4)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 3)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 4)),
                ))),
            )?),
        ];

        for (expr, expected_expr) in exprs.into_iter().zip(expected_exprs.into_iter()) {
            assert!(update_expr(&expr, &child, true)?
                .unwrap()
                .eq(&expected_expr));
        }

        Ok(())
    }

    #[test]
    fn test_update_projected_exprs() -> Result<()> {
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 3)),
                Operator::Divide,
                Arc::new(Column::new("e", 5)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 3)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 4)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(ScalarUDF::new_from_impl(DummyUDF::new())),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 0)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 2))),
                vec![
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 2)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 5)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 5)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 2)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 3)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 5)),
                ))),
            )?),
        ];
        let projected_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 3)), "a".to_owned()),
            (Arc::new(Column::new("b", 1)), "b_new".to_owned()),
            (Arc::new(Column::new("c", 0)), "c".to_owned()),
            (Arc::new(Column::new("d", 2)), "d_new".to_owned()),
            (Arc::new(Column::new("e", 5)), "e".to_owned()),
            (Arc::new(Column::new("f", 4)), "f_new".to_owned()),
        ];

        let expected_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Divide,
                Arc::new(Column::new("e", 4)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 0)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f_new", 5)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(ScalarUDF::new_from_impl(DummyUDF::new())),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_new", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 2)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Divide,
                        Arc::new(Column::new("b_new", 1)),
                    )),
                ],
                DataType::Int32,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d_new", 3))),
                vec![
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d_new", 3)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 4)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 4)),
                            Operator::Plus,
                            Arc::new(Column::new("d_new", 3)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 4)),
                ))),
            )?),
        ];

        for (expr, expected_expr) in exprs.into_iter().zip(expected_exprs.into_iter()) {
            assert!(update_expr(&expr, &projected_exprs, false)?
                .unwrap()
                .eq(&expected_expr));
        }

        Ok(())
    }

    fn create_simple_csv_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
        ]));
        Arc::new(
            CsvExec::builder(
                FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema)
                    .with_file(PartitionedFile::new("x".to_string(), 100))
                    .with_projection(Some(vec![0, 1, 2, 3, 4])),
            )
            .with_has_header(false)
            .with_delimeter(0)
            .with_quote(0)
            .with_escape(None)
            .with_comment(None)
            .with_newlines_in_values(false)
            .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
            .build(),
        )
    }

    fn create_projecting_csv_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]));
        Arc::new(
            CsvExec::builder(
                FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema)
                    .with_file(PartitionedFile::new("x".to_string(), 100))
                    .with_projection(Some(vec![3, 2, 1])),
            )
            .with_has_header(false)
            .with_delimeter(0)
            .with_quote(0)
            .with_escape(None)
            .with_comment(None)
            .with_newlines_in_values(false)
            .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
            .build(),
        )
    }

    fn create_projecting_memory_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
        ]));

        Arc::new(MemoryExec::try_new(&[], schema, Some(vec![2, 0, 3, 4])).unwrap())
    }

    #[test]
    fn test_csv_after_projection() -> Result<()> {
        let csv = create_projecting_csv_exec();
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 2)), "b".to_string()),
                (Arc::new(Column::new("d", 0)), "d".to_string()),
            ],
            csv.clone(),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@2 as b, d@0 as d]",
                "  CsvExec: file_groups={1 group: [[x]]}, projection=[d, c, b], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "CsvExec: file_groups={1 group: [[x]]}, projection=[b, d], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_memory_after_projection() -> Result<()> {
        let memory = create_projecting_memory_exec();
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("d", 2)), "d".to_string()),
                (Arc::new(Column::new("e", 3)), "e".to_string()),
                (Arc::new(Column::new("a", 1)), "a".to_string()),
            ],
            memory.clone(),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[d@2 as d, e@3 as e, a@1 as a]",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = ["MemoryExec: partitions=0, partition_sizes=[]"];
        assert_eq!(get_plan_string(&after_optimize), expected);
        assert_eq!(
            after_optimize
                .clone()
                .as_any()
                .downcast_ref::<MemoryExec>()
                .unwrap()
                .projection()
                .clone()
                .unwrap(),
            vec![3, 4, 0]
        );

        Ok(())
    }

    #[test]
    fn test_streaming_table_after_projection() -> Result<()> {
        #[derive(Debug)]
        struct DummyStreamPartition {
            schema: SchemaRef,
        }
        impl PartitionStream for DummyStreamPartition {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }
            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                unreachable!()
            }
        }

        let streaming_table = StreamingTableExec::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, true),
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
            ])),
            vec![Arc::new(DummyStreamPartition {
                schema: Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, true),
                    Field::new("c", DataType::Int32, true),
                    Field::new("d", DataType::Int32, true),
                    Field::new("e", DataType::Int32, true),
                ])),
            }) as _],
            Some(&vec![0_usize, 2, 4, 3]),
            vec![
                LexOrdering::new(vec![
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("e", 2)),
                        options: SortOptions::default(),
                    },
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("a", 0)),
                        options: SortOptions::default(),
                    },
                ]),
                LexOrdering::new(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options: SortOptions::default(),
                }]),
            ]
            .into_iter(),
            true,
            None,
        )?;
        let projection = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("d", 3)), "d".to_string()),
                (Arc::new(Column::new("e", 2)), "e".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
            ],
            Arc::new(streaming_table) as _,
        )?) as _;

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let result = after_optimize
            .as_any()
            .downcast_ref::<StreamingTableExec>()
            .unwrap();
        assert_eq!(
            result.partition_schema(),
            &Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, true),
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
            ]))
        );
        assert_eq!(
            result.projection().clone().unwrap().to_vec(),
            vec![3_usize, 4, 0]
        );
        assert_eq!(
            result.projected_schema(),
            &Schema::new(vec![
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ])
        );
        assert_eq!(
            result.projected_output_ordering().into_iter().collect_vec(),
            vec![
                LexOrdering::new(vec![
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("e", 1)),
                        options: SortOptions::default(),
                    },
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("a", 2)),
                        options: SortOptions::default(),
                    },
                ]),
                LexOrdering::new(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 0)),
                    options: SortOptions::default(),
                }]),
            ]
        );
        assert!(result.is_infinite());

        Ok(())
    }

    #[test]
    fn test_projection_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let child_projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("e", 4)), "new_e".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("b", 1)), "new_b".to_string()),
            ],
            csv.clone(),
        )?);
        let top_projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("new_b", 3)), "new_b".to_string()),
                (
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Plus,
                        Arc::new(Column::new("new_e", 1)),
                    )),
                    "binary".to_string(),
                ),
                (Arc::new(Column::new("new_b", 3)), "newest_b".to_string()),
            ],
            child_projection.clone(),
        )?);

        let initial = get_plan_string(&top_projection);
        let expected_initial = [
            "ProjectionExec: expr=[new_b@3 as new_b, c@0 + new_e@1 as binary, new_b@3 as newest_b]",
            "  ProjectionExec: expr=[c@2 as c, e@4 as new_e, a@0 as a, b@1 as new_b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(top_projection, &ConfigOptions::new())?;

        let expected = [
            "ProjectionExec: expr=[b@1 as new_b, c@2 + e@4 as binary, b@1 as newest_b]",
            "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_output_req_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            csv.clone(),
            Some(LexRequirement::new(vec![
                PhysicalSortRequirement {
                    expr: Arc::new(Column::new("b", 1)),
                    options: Some(SortOptions::default()),
                },
                PhysicalSortRequirement {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: Some(SortOptions::default()),
                },
            ])),
            Distribution::HashPartitioned(vec![
                Arc::new(Column::new("a", 0)),
                Arc::new(Column::new("b", 1)),
            ]),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  OutputRequirementExec",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected: [&str; 3] = [
            "OutputRequirementExec",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];

        assert_eq!(get_plan_string(&after_optimize), expected);
        let expected_reqs = LexRequirement::new(vec![
            PhysicalSortRequirement {
                expr: Arc::new(Column::new("b", 2)),
                options: Some(SortOptions::default()),
            },
            PhysicalSortRequirement {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("c", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("new_a", 1)),
                )),
                options: Some(SortOptions::default()),
            },
        ]);
        assert_eq!(
            after_optimize
                .as_any()
                .downcast_ref::<OutputRequirementExec>()
                .unwrap()
                .required_input_ordering()[0]
                .clone()
                .unwrap(),
            expected_reqs
        );
        let expected_distribution: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("new_a", 1)),
            Arc::new(Column::new("b", 2)),
        ];
        if let Distribution::HashPartitioned(vec) = after_optimize
            .as_any()
            .downcast_ref::<OutputRequirementExec>()
            .unwrap()
            .required_input_distribution()[0]
            .clone()
        {
            assert!(vec
                .iter()
                .zip(expected_distribution)
                .all(|(actual, expected)| actual.eq(&expected)));
        } else {
            panic!("Expected HashPartitioned distribution!");
        };

        Ok(())
    }

    #[test]
    fn test_coalesce_partitions_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let coalesce_partitions: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(csv));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("a", 0)), "a_new".to_string()),
                (Arc::new(Column::new("d", 3)), "d".to_string()),
            ],
            coalesce_partitions,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@1 as b, a@0 as a_new, d@3 as d]",
                "  CoalescePartitionsExec",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "CoalescePartitionsExec",
                "  ProjectionExec: expr=[b@1 as b, a@0 as a_new, d@3 as d]",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_filter_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("b", 1)),
                Operator::Minus,
                Arc::new(Column::new("a", 0)),
            )),
            Operator::Gt,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("d", 3)),
                Operator::Minus,
                Arc::new(Column::new("a", 0)),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, csv)?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("a", 0)), "a_new".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("d", 3)), "d".to_string()),
            ],
            filter.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[a@0 as a_new, b@1 as b, d@3 as d]",
                "  FilterExec: b@1 - a@0 > d@3 - a@0",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "FilterExec: b@1 - a_new@0 > d@2 - a_new@0",
                "  ProjectionExec: expr=[a@0 as a_new, b@1 as b, d@3 as d]",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_join_after_projection() -> Result<()> {
        let left_csv = create_simple_csv_exec();
        let right_csv = create_simple_csv_exec();

        let join: Arc<dyn ExecutionPlan> = Arc::new(SymmetricHashJoinExec::try_new(
            left_csv,
            right_csv,
            vec![(Arc::new(Column::new("b", 1)), Arc::new(Column::new("c", 2)))],
            // b_left-(1+a_right)<=a_right+c_left
            Some(JoinFilter::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_left_inter", 0)),
                        Operator::Minus,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                            Operator::Plus,
                            Arc::new(Column::new("a_right_inter", 1)),
                        )),
                    )),
                    Operator::LtEq,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a_right_inter", 1)),
                        Operator::Plus,
                        Arc::new(Column::new("c_left_inter", 2)),
                    )),
                )),
                vec![
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Left,
                    },
                    ColumnIndex {
                        index: 0,
                        side: JoinSide::Right,
                    },
                    ColumnIndex {
                        index: 2,
                        side: JoinSide::Left,
                    },
                ],
                Arc::new(Schema::new(vec![
                    Field::new("b_left_inter", DataType::Int32, true),
                    Field::new("a_right_inter", DataType::Int32, true),
                    Field::new("c_left_inter", DataType::Int32, true),
                ])),
            )),
            &JoinType::Inner,
            true,
            None,
            None,
            StreamJoinPartitionMode::SinglePartition,
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c_from_left".to_string()),
                (Arc::new(Column::new("b", 1)), "b_from_left".to_string()),
                (Arc::new(Column::new("a", 0)), "a_from_left".to_string()),
                (Arc::new(Column::new("a", 5)), "a_from_right".to_string()),
                (Arc::new(Column::new("c", 7)), "c_from_right".to_string()),
            ],
            join,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, a@5 as a_from_right, c@7 as c_from_right]",
            "  SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b_from_left@1, c_from_right@1)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "  ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "  ProjectionExec: expr=[a@0 as a_from_right, c@2 as c_from_right]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        let expected_filter_col_ind = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
        ];

        assert_eq!(
            expected_filter_col_ind,
            after_optimize
                .as_any()
                .downcast_ref::<SymmetricHashJoinExec>()
                .unwrap()
                .filter()
                .unwrap()
                .column_indices()
        );

        Ok(())
    }

    #[test]
    fn test_join_after_required_projection() -> Result<()> {
        let left_csv = create_simple_csv_exec();
        let right_csv = create_simple_csv_exec();

        let join: Arc<dyn ExecutionPlan> = Arc::new(SymmetricHashJoinExec::try_new(
            left_csv,
            right_csv,
            vec![(Arc::new(Column::new("b", 1)), Arc::new(Column::new("c", 2)))],
            // b_left-(1+a_right)<=a_right+c_left
            Some(JoinFilter::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_left_inter", 0)),
                        Operator::Minus,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                            Operator::Plus,
                            Arc::new(Column::new("a_right_inter", 1)),
                        )),
                    )),
                    Operator::LtEq,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a_right_inter", 1)),
                        Operator::Plus,
                        Arc::new(Column::new("c_left_inter", 2)),
                    )),
                )),
                vec![
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Left,
                    },
                    ColumnIndex {
                        index: 0,
                        side: JoinSide::Right,
                    },
                    ColumnIndex {
                        index: 2,
                        side: JoinSide::Left,
                    },
                ],
                Arc::new(Schema::new(vec![
                    Field::new("b_left_inter", DataType::Int32, true),
                    Field::new("a_right_inter", DataType::Int32, true),
                    Field::new("c_left_inter", DataType::Int32, true),
                ])),
            )),
            &JoinType::Inner,
            true,
            None,
            None,
            StreamJoinPartitionMode::SinglePartition,
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("a", 5)), "a".to_string()),
                (Arc::new(Column::new("b", 6)), "b".to_string()),
                (Arc::new(Column::new("c", 7)), "c".to_string()),
                (Arc::new(Column::new("d", 8)), "d".to_string()),
                (Arc::new(Column::new("e", 9)), "e".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("d", 3)), "d".to_string()),
                (Arc::new(Column::new("e", 4)), "e".to_string()),
            ],
            join,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[a@5 as a, b@6 as b, c@7 as c, d@8 as d, e@9 as e, a@0 as a, b@1 as b, c@2 as c, d@3 as d, e@4 as e]",
            "  SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "ProjectionExec: expr=[a@5 as a, b@6 as b, c@7 as c, d@8 as d, e@9 as e, a@0 as a, b@1 as b, c@2 as c, d@3 as d, e@4 as e]",
            "  SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(get_plan_string(&after_optimize), expected);
        Ok(())
    }

    #[test]
    fn test_nested_loop_join_after_projection() -> Result<()> {
        let left_csv = create_simple_csv_exec();
        let right_csv = create_simple_csv_exec();

        let col_left_a = col("a", &left_csv.schema())?;
        let col_right_b = col("b", &right_csv.schema())?;
        let col_left_c = col("c", &left_csv.schema())?;
        // left_a < right_b
        let filter_expr =
            binary(col_left_a, Operator::Lt, col_right_b, &Schema::empty())?;
        let filter_column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];
        let filter_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);

        let join: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
            left_csv,
            right_csv,
            Some(JoinFilter::new(
                filter_expr,
                filter_column_indices,
                Arc::new(filter_schema),
            )),
            &JoinType::Inner,
            None,
        )?);

        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![(col_left_c, "c".to_string())],
            Arc::clone(&join),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c]",
            "  NestedLoopJoinExec: join_type=Inner, filter=a@0 < b@1",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;
        let expected = [
            "NestedLoopJoinExec: join_type=Inner, filter=a@0 < b@1, projection=[c@2]",
            "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);
        Ok(())
    }

    #[test]
    fn test_hash_join_after_projection() -> Result<()> {
        // sql like
        // SELECT t1.c as c_from_left, t1.b as b_from_left, t1.a as a_from_left, t2.c as c_from_right FROM t1 JOIN t2 ON t1.b = t2.c WHERE t1.b - (1 + t2.a) <= t2.a + t1.c
        let left_csv = create_simple_csv_exec();
        let right_csv = create_simple_csv_exec();

        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left_csv,
            right_csv,
            vec![(Arc::new(Column::new("b", 1)), Arc::new(Column::new("c", 2)))],
            // b_left-(1+a_right)<=a_right+c_left
            Some(JoinFilter::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_left_inter", 0)),
                        Operator::Minus,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                            Operator::Plus,
                            Arc::new(Column::new("a_right_inter", 1)),
                        )),
                    )),
                    Operator::LtEq,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a_right_inter", 1)),
                        Operator::Plus,
                        Arc::new(Column::new("c_left_inter", 2)),
                    )),
                )),
                vec![
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Left,
                    },
                    ColumnIndex {
                        index: 0,
                        side: JoinSide::Right,
                    },
                    ColumnIndex {
                        index: 2,
                        side: JoinSide::Left,
                    },
                ],
                Arc::new(Schema::new(vec![
                    Field::new("b_left_inter", DataType::Int32, true),
                    Field::new("a_right_inter", DataType::Int32, true),
                    Field::new("c_left_inter", DataType::Int32, true),
                ])),
            )),
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            true,
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c_from_left".to_string()),
                (Arc::new(Column::new("b", 1)), "b_from_left".to_string()),
                (Arc::new(Column::new("a", 0)), "a_from_left".to_string()),
                (Arc::new(Column::new("c", 7)), "c_from_right".to_string()),
            ],
            join.clone(),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
			"ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, c@7 as c_from_right]", "  HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2", "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        // HashJoinExec only returns result after projection. Because there are some alias columns in the projection, the ProjectionExec is not removed.
        let expected = ["ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, c@3 as c_from_right]", "  HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2, projection=[a@0, b@1, c@2, c@7]", "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("c", 7)), "c".to_string()),
            ],
            join.clone(),
        )?);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        // Comparing to the previous result, this projection don't have alias columns either change the order of output fields. So the ProjectionExec is removed.
        let expected = ["HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2, projection=[a@0, b@1, c@2, c@7]", "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_repartition_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            csv,
            Partitioning::Hash(
                vec![
                    Arc::new(Column::new("a", 0)),
                    Arc::new(Column::new("b", 1)),
                    Arc::new(Column::new("d", 3)),
                ],
                6,
            ),
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 1)), "b_new".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("d", 3)), "d_new".to_string()),
            ],
            repartition,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@1 as b_new, a@0 as a, d@3 as d_new]",
                "  RepartitionExec: partitioning=Hash([a@0, b@1, d@3], 6), input_partitions=1",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "RepartitionExec: partitioning=Hash([a@1, b_new@0, d_new@2], 6), input_partitions=1",
                "  ProjectionExec: expr=[b@1 as b_new, a@0 as a, d@3 as d_new]",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        assert_eq!(
            after_optimize
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .unwrap()
                .partitioning()
                .clone(),
            Partitioning::Hash(
                vec![
                    Arc::new(Column::new("a", 1)),
                    Arc::new(Column::new("b_new", 0)),
                    Arc::new(Column::new("d_new", 2)),
                ],
                6,
            ),
        );

        Ok(())
    }

    #[test]
    fn test_sort_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: SortOptions::default(),
                },
            ]),
            csv.clone(),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  SortExec: expr=[b@1 ASC, c@2 + a@0 ASC], preserve_partitioning=[false]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SortExec: expr=[b@2 ASC, c@0 + new_a@1 ASC], preserve_partitioning=[false]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_sort_preserving_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: SortOptions::default(),
                },
            ]),
            csv.clone(),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  SortPreservingMergeExec: [b@1 ASC, c@2 + a@0 ASC]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SortPreservingMergeExec: [b@2 ASC, c@0 + new_a@1 ASC]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_union_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let union: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![csv.clone(), csv.clone(), csv]));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            union.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  UnionExec",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "UnionExec",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }
}
