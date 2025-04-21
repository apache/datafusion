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

use std::any::Any;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::CsvSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_common::{JoinSide, JoinType, ScalarValue};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::expressions::{
    binary, cast, col, BinaryExpr, CaseExpr, CastExpr, Column, Literal, NegativeExpr,
};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::{
    Distribution, Partitioning, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
use datafusion_physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion_physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, StreamJoinPartitionMode,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::projection::{update_expr, ProjectionExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::streaming::StreamingTableExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{get_plan_string, ExecutionPlan};

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_expr_common::columnar_value::ColumnarValue;
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("dummy - not implemented")
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
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema,
        Arc::new(CsvSource::new(false, 0, 0)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_projection(Some(vec![0, 1, 2, 3, 4]))
    .build();

    DataSourceExec::from_data_source(config)
}

fn create_projecting_csv_exec() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
        Field::new("d", DataType::Int32, true),
    ]));
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema,
        Arc::new(CsvSource::new(false, 0, 0)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_projection(Some(vec![3, 2, 1]))
    .build();

    DataSourceExec::from_data_source(config)
}

fn create_projecting_memory_exec() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
        Field::new("d", DataType::Int32, true),
        Field::new("e", DataType::Int32, true),
    ]));

    MemorySourceConfig::try_new_exec(&[], schema, Some(vec![2, 0, 3, 4])).unwrap()
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
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[d, c, b], file_type=csv, has_header=false",
    ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected =
        ["DataSourceExec: file_groups={1 group: [[x]]}, projection=[b, d], file_type=csv, has_header=false"];
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
        "  DataSourceExec: partitions=0, partition_sizes=[]",
    ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = ["DataSourceExec: partitions=0, partition_sizes=[]"];
    assert_eq!(get_plan_string(&after_optimize), expected);
    assert_eq!(
        after_optimize
            .clone()
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .as_any()
            .downcast_ref::<MemorySourceConfig>()
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(top_projection, &ConfigOptions::new())?;

    let expected = [
            "ProjectionExec: expr=[b@1 as new_b, c@2 + e@4 as binary, b@1 as newest_b]",
            "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected: [&str; 3] = [
            "OutputRequirementExec",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
                "CoalescePartitionsExec",
                "  ProjectionExec: expr=[b@1 as b, a@0 as a_new, d@3 as d]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
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
    let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, csv)?);
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
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
                "FilterExec: b@1 - a_new@0 > d@2 - a_new@0",
                "  ProjectionExec: expr=[a@0 as a_new, b@1 as b, d@3 as d]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
            "SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b_from_left@1, c_from_right@1)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "  ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "  ProjectionExec: expr=[a@0 as a_from_right, c@2 as c_from_right]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
            "ProjectionExec: expr=[a@5 as a, b@6 as b, c@7 as c, d@8 as d, e@9 as e, a@0 as a, b@1 as b, c@2 as c, d@3 as d, e@4 as e]",
            "  SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
    let filter_expr = binary(col_left_a, Operator::Lt, col_right_b, &Schema::empty())?;
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;
    let expected = [
            "NestedLoopJoinExec: join_type=Inner, filter=a@0 < b@1, projection=[c@2]",
            "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
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
			"ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, c@7 as c_from_right]", "  HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2", "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false", "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    // HashJoinExec only returns result after projection. Because there are some alias columns in the projection, the ProjectionExec is not removed.
    let expected = ["ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, c@3 as c_from_right]", "  HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2, projection=[a@0, b@1, c@2, c@7]", "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false", "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"];
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
    let expected = ["HashJoinExec: mode=Auto, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2, projection=[a@0, b@1, c@2, c@7]", "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false", "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"];
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
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
                "RepartitionExec: partitioning=Hash([a@1, b_new@0, d_new@2], 6), input_partitions=1",
                "  ProjectionExec: expr=[b@1 as b_new, a@0 as a, d@3 as d_new]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
            "SortExec: expr=[b@2 ASC, c@0 + new_a@1 ASC], preserve_partitioning=[false]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
            "SortPreservingMergeExec: [b@2 ASC, c@0 + new_a@1 ASC]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
            ];
    assert_eq!(initial, expected_initial);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
            "UnionExec",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

/// Returns a DataSourceExec that scans a file with (int_col, string_col)
/// and has a partitioning column partition_col (Utf8)
fn partitioned_data_source() -> Arc<DataSourceExec> {
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("int_col", DataType::Int32, true),
        Field::new("string_col", DataType::Utf8, true),
    ]));

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        file_schema.clone(),
        Arc::new(CsvSource::default()),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_table_partition_cols(vec![Field::new("partition_col", DataType::Utf8, true)])
    .with_projection(Some(vec![0, 1, 2]))
    .build();

    DataSourceExec::from_data_source(config)
}

#[test]
fn test_partition_col_projection_pushdown() -> Result<()> {
    let source = partitioned_data_source();
    let partitioned_schema = source.schema();

    let projection = Arc::new(ProjectionExec::try_new(
        vec![
            (
                col("string_col", partitioned_schema.as_ref())?,
                "string_col".to_string(),
            ),
            (
                col("partition_col", partitioned_schema.as_ref())?,
                "partition_col".to_string(),
            ),
            (
                col("int_col", partitioned_schema.as_ref())?,
                "int_col".to_string(),
            ),
        ],
        source,
    )?);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
        "ProjectionExec: expr=[string_col@1 as string_col, partition_col@2 as partition_col, int_col@0 as int_col]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[int_col, string_col, partition_col], file_type=csv, has_header=false"
    ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn test_partition_col_projection_pushdown_expr() -> Result<()> {
    let source = partitioned_data_source();
    let partitioned_schema = source.schema();

    let projection = Arc::new(ProjectionExec::try_new(
        vec![
            (
                col("string_col", partitioned_schema.as_ref())?,
                "string_col".to_string(),
            ),
            (
                // CAST(partition_col, Utf8View)
                cast(
                    col("partition_col", partitioned_schema.as_ref())?,
                    partitioned_schema.as_ref(),
                    DataType::Utf8View,
                )?,
                "partition_col".to_string(),
            ),
            (
                col("int_col", partitioned_schema.as_ref())?,
                "int_col".to_string(),
            ),
        ],
        source,
    )?);

    let after_optimize =
        ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

    let expected = [
        "ProjectionExec: expr=[string_col@1 as string_col, CAST(partition_col@2 AS Utf8View) as partition_col, int_col@0 as int_col]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[int_col, string_col, partition_col], file_type=csv, has_header=false"
    ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}
