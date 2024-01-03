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

use arrow::csv::WriterBuilder;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema};
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileSinkConfig, ParquetExec,
};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{
    create_udf, BuiltinScalarFunction, JoinType, Operator, Volatility,
};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::physical_expr::{PhysicalSortRequirement, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    binary, cast, col, in_list, like, lit, Avg, BinaryExpr, Column, DistinctCount,
    GetFieldAccessExpr, GetIndexedFieldExpr, NotExpr, NthValue, PhysicalSortExpr, Sum,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::insert::FileSinkExec;
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, StreamJoinPartitionMode,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{
    BuiltInWindowExpr, PlainAggregateWindowExpr, WindowAggExec,
};
use datafusion::physical_plan::{
    functions, udaf, AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::file_options::parquet_writer::ParquetWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{FileTypeWriterOptions, Result};
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, Signature, SimpleAggregateUDF,
    WindowFrame, WindowFrameBound,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf;

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
    let _ = roundtrip_test_and_return(exec_plan);
    Ok(())
}

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
///
/// This version of the roundtrip_test method returns the final plan after serde so that it can be inspected
/// farther in tests.
fn roundtrip_test_and_return(
    exec_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto: protobuf::PhysicalPlanNode =
        protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
            .expect("to proto");
    let runtime = ctx.runtime_env();
    let result_exec_plan: Arc<dyn ExecutionPlan> = proto
        .try_into_physical_plan(&ctx, runtime.deref(), &codec)
        .expect("from proto");
    assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
    Ok(result_exec_plan)
}

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
///
/// This version of the roundtrip_test function accepts a SessionContext, which is required when
/// performing serde on some plans.
fn roundtrip_test_with_context(
    exec_plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let proto: protobuf::PhysicalPlanNode =
        protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
            .expect("to proto");
    let runtime = ctx.runtime_env();
    let result_exec_plan: Arc<dyn ExecutionPlan> = proto
        .try_into_physical_plan(&ctx, runtime.deref(), &codec)
        .expect("from proto");
    assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
    Ok(())
}

#[test]
fn roundtrip_empty() -> Result<()> {
    roundtrip_test(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
}

#[test]
fn roundtrip_date_time_interval() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("some_date", DataType::Date32, false),
        Field::new(
            "some_interval",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
    ]);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));
    let date_expr = col("some_date", &schema)?;
    let literal_expr = col("some_interval", &schema)?;
    let date_time_interval_expr =
        binary(date_expr, Operator::Plus, literal_expr, &schema)?;
    let plan = Arc::new(ProjectionExec::try_new(
        vec![(date_time_interval_expr, "result".to_string())],
        input,
    )?);
    roundtrip_test(plan)
}

#[test]
fn roundtrip_local_limit() -> Result<()> {
    roundtrip_test(Arc::new(LocalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        25,
    )))
}

#[test]
fn roundtrip_global_limit() -> Result<()> {
    roundtrip_test(Arc::new(GlobalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        0,
        Some(25),
    )))
}

#[test]
fn roundtrip_global_skip_no_limit() -> Result<()> {
    roundtrip_test(Arc::new(GlobalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        10,
        None, // no limit
    )))
}

#[test]
fn roundtrip_hash_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let on = vec![(
        Column::new("col", schema_left.index_of("col")?),
        Column::new("col", schema_right.index_of("col")?),
    )];

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    for join_type in &[
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
    ] {
        for partition_mode in &[PartitionMode::Partitioned, PartitionMode::CollectLeft] {
            roundtrip_test(Arc::new(HashJoinExec::try_new(
                Arc::new(EmptyExec::new(schema_left.clone())),
                Arc::new(EmptyExec::new(schema_right.clone())),
                on.clone(),
                None,
                join_type,
                *partition_mode,
                false,
            )?))?;
        }
    }
    Ok(())
}

#[test]
fn roundtrip_nested_loop_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    for join_type in &[
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
    ] {
        roundtrip_test(Arc::new(NestedLoopJoinExec::try_new(
            Arc::new(EmptyExec::new(schema_left.clone())),
            Arc::new(EmptyExec::new(schema_right.clone())),
            None,
            join_type,
        )?))?;
    }
    Ok(())
}

#[test]
fn roundtrip_window() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let window_frame = WindowFrame {
        units: datafusion_expr::WindowFrameUnits::Range,
        start_bound: WindowFrameBound::Preceding(ScalarValue::Int64(None)),
        end_bound: WindowFrameBound::CurrentRow,
    };

    let builtin_window_expr = Arc::new(BuiltInWindowExpr::new(
        Arc::new(NthValue::first(
            "FIRST_VALUE(a) PARTITION BY [b] ORDER BY [a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            col("a", &schema)?,
            DataType::Int64,
        )),
        &[col("b", &schema)?],
        &[PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }],
        Arc::new(window_frame),
    ));

    let plain_aggr_window_expr = Arc::new(PlainAggregateWindowExpr::new(
        Arc::new(Avg::new(
            cast(col("b", &schema)?, &schema, DataType::Float64)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        )),
        &[],
        &[],
        Arc::new(WindowFrame::new(false)),
    ));

    let window_frame = WindowFrame {
        units: datafusion_expr::WindowFrameUnits::Range,
        start_bound: WindowFrameBound::CurrentRow,
        end_bound: WindowFrameBound::Preceding(ScalarValue::Int64(None)),
    };

    let sliding_aggr_window_expr = Arc::new(SlidingAggregateWindowExpr::new(
        Arc::new(Sum::new(
            cast(col("a", &schema)?, &schema, DataType::Float64)?,
            "SUM(a) RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEEDING",
            DataType::Float64,
        )),
        &[],
        &[],
        Arc::new(window_frame),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(WindowAggExec::try_new(
        vec![
            builtin_window_expr,
            plain_aggr_window_expr,
            sliding_aggr_window_expr,
        ],
        input,
        vec![col("b", &schema)?],
    )?))
}

#[test]
fn rountrip_aggregate() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
        cast(col("b", &schema)?, &schema, DataType::Float64)?,
        "AVG(b)".to_string(),
        DataType::Float64,
    ))];

    roundtrip_test(Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates.clone(),
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?))
}

#[test]
fn roundtrip_aggregate_udaf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    #[derive(Debug)]
    struct Example;
    impl Accumulator for Example {
        fn state(&self) -> Result<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Int64(Some(0))])
        }

        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int64(Some(0)))
        }

        fn size(&self) -> usize {
            0
        }
    }

    let return_type = DataType::Int64;
    let accumulator: AccumulatorFactoryFunction = Arc::new(|_| Ok(Box::new(Example)));
    let state_type = vec![DataType::Int64];

    // let udaf = AggregateUDF::new(
    //     "example",
    //     &Signature::exact(vec![DataType::Int64], Volatility::Immutable),
    //     &rt_func,
    //     &accumulator,
    //     &st_func,
    // );
    let udaf = AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "example".to_string(),
        Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        return_type,
        accumulator,
        state_type,
    ));

    let ctx = SessionContext::new();
    ctx.register_udaf(udaf.clone());

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![udaf::create_aggregate_expr(
        &udaf,
        &[col("b", &schema)?],
        &schema,
        "example_agg",
    )?];

    roundtrip_test_with_context(
        Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
            aggregates.clone(),
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?),
        ctx,
    )
}

#[test]
fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let field_c = Field::new("c", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
    let not = Arc::new(NotExpr::new(col("a", &schema)?));
    let in_list = in_list(
        col("b", &schema)?,
        vec![
            lit(ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Int64(Some(2))),
        ],
        &false,
        schema.as_ref(),
    )?;
    let and = binary(not, Operator::And, in_list, &schema)?;
    roundtrip_test(Arc::new(FilterExec::try_new(
        and,
        Arc::new(EmptyExec::new(schema.clone())),
    )?))
}

#[test]
fn roundtrip_sort() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs = vec![
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ];
    roundtrip_test(Arc::new(SortExec::new(
        sort_exprs,
        Arc::new(EmptyExec::new(schema)),
    )))
}

#[test]
fn roundtrip_sort_preserve_partitioning() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs = vec![
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ];

    roundtrip_test(Arc::new(SortExec::new(
        sort_exprs.clone(),
        Arc::new(EmptyExec::new(schema.clone())),
    )))?;

    roundtrip_test(Arc::new(
        SortExec::new(sort_exprs, Arc::new(EmptyExec::new(schema)))
            .with_preserve_partitioning(true),
    ))
}

#[test]
fn roundtrip_parquet_exec_with_pruning_predicate() -> Result<()> {
    let scan_config = FileScanConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_schema: Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            false,
        )])),
        file_groups: vec![vec![PartitionedFile::new(
            "/path/to/file.parquet".to_string(),
            1024,
        )]],
        statistics: Statistics {
            num_rows: Precision::Inexact(100),
            total_byte_size: Precision::Inexact(1024),
            column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(vec![
                Field::new("col", DataType::Utf8, false),
            ]))),
        },
        projection: None,
        limit: None,
        table_partition_cols: vec![],
        output_ordering: vec![],
    };

    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("col", 1)),
        Operator::Eq,
        lit("1"),
    ));
    roundtrip_test(Arc::new(ParquetExec::new(
        scan_config,
        Some(predicate),
        None,
    )))
}

#[test]
fn roundtrip_builtin_scalar_function() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    let execution_props = ExecutionProps::new();

    let fun_expr =
        functions::create_physical_fun(&BuiltinScalarFunction::Acos, &execution_props)?;

    let expr = ScalarFunctionExpr::new(
        "acos",
        fun_expr,
        vec![col("a", &schema)?],
        DataType::Int64,
        None,
    );

    let project =
        ProjectionExec::try_new(vec![(Arc::new(expr), "a".to_string())], input)?;

    roundtrip_test(Arc::new(project))
}

#[test]
fn roundtrip_scalar_udf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

    let scalar_fn = make_scalar_function(fn_impl);

    let udf = create_udf(
        "dummy",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        scalar_fn.clone(),
    );

    let expr = ScalarFunctionExpr::new(
        "dummy",
        scalar_fn,
        vec![col("a", &schema)?],
        DataType::Int64,
        None,
    );

    let project =
        ProjectionExec::try_new(vec![(Arc::new(expr), "a".to_string())], input)?;

    let ctx = SessionContext::new();

    ctx.register_udf(udf);

    roundtrip_test_with_context(Arc::new(project), ctx)
}

#[test]
fn roundtrip_distinct_count() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(DistinctCount::new(
        DataType::Int64,
        col("b", &schema)?,
        "COUNT(DISTINCT b)".to_string(),
    ))];

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    roundtrip_test(Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups),
        aggregates.clone(),
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?))
}

#[test]
fn roundtrip_like() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));
    let like_expr = like(
        false,
        false,
        col("a", &schema)?,
        col("b", &schema)?,
        &schema,
    )?;
    let plan = Arc::new(ProjectionExec::try_new(
        vec![(like_expr, "result".to_string())],
        input,
    )?);
    roundtrip_test(plan)
}

#[test]
fn roundtrip_get_indexed_field_named_struct_field() -> Result<()> {
    let fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new_struct(
            "arg",
            Fields::from(vec![Field::new("name", DataType::Float64, true)]),
            true,
        ),
    ];

    let schema = Schema::new(fields);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));

    let col_arg = col("arg", &schema)?;
    let get_indexed_field_expr = Arc::new(GetIndexedFieldExpr::new(
        col_arg,
        GetFieldAccessExpr::NamedStructField {
            name: ScalarValue::from("name"),
        },
    ));

    let plan = Arc::new(ProjectionExec::try_new(
        vec![(get_indexed_field_expr, "result".to_string())],
        input,
    )?);

    roundtrip_test(plan)
}

#[test]
fn roundtrip_get_indexed_field_list_index() -> Result<()> {
    let fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new_list("arg", Field::new("item", DataType::Float64, true), true),
        Field::new("key", DataType::Int64, true),
    ];

    let schema = Schema::new(fields);
    let input = Arc::new(PlaceholderRowExec::new(Arc::new(schema.clone())));

    let col_arg = col("arg", &schema)?;
    let col_key = col("key", &schema)?;
    let get_indexed_field_expr = Arc::new(GetIndexedFieldExpr::new(
        col_arg,
        GetFieldAccessExpr::ListIndex { key: col_key },
    ));

    let plan = Arc::new(ProjectionExec::try_new(
        vec![(get_indexed_field_expr, "result".to_string())],
        input,
    )?);

    roundtrip_test(plan)
}

#[test]
fn roundtrip_get_indexed_field_list_range() -> Result<()> {
    let fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new_list("arg", Field::new("item", DataType::Float64, true), true),
        Field::new("start", DataType::Int64, true),
        Field::new("stop", DataType::Int64, true),
    ];

    let schema = Schema::new(fields);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));

    let col_arg = col("arg", &schema)?;
    let col_start = col("start", &schema)?;
    let col_stop = col("stop", &schema)?;
    let get_indexed_field_expr = Arc::new(GetIndexedFieldExpr::new(
        col_arg,
        GetFieldAccessExpr::ListRange {
            start: col_start,
            stop: col_stop,
        },
    ));

    let plan = Arc::new(ProjectionExec::try_new(
        vec![(get_indexed_field_expr, "result".to_string())],
        input,
    )?);

    roundtrip_test(plan)
}

#[test]
fn roundtrip_analyze() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Schema::new(vec![field_a, field_b]);
    let input = Arc::new(PlaceholderRowExec::new(Arc::new(schema.clone())));

    roundtrip_test(Arc::new(AnalyzeExec::new(
        false,
        false,
        input,
        Arc::new(schema),
    )))
}

#[test]
fn roundtrip_json_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        single_file_output: true,
        overwrite: true,
        file_type_writer_options: FileTypeWriterOptions::JSON(JsonWriterOptions::new(
            CompressionTypeVariant::UNCOMPRESSED,
        )),
    };
    let data_sink = Arc::new(JsonSink::new(file_sink_config));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    roundtrip_test(Arc::new(FileSinkExec::new(
        input,
        data_sink,
        schema.clone(),
        Some(sort_order),
    )))
}

#[test]
fn roundtrip_csv_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        single_file_output: true,
        overwrite: true,
        file_type_writer_options: FileTypeWriterOptions::CSV(CsvWriterOptions::new(
            WriterBuilder::default(),
            CompressionTypeVariant::ZSTD,
        )),
    };
    let data_sink = Arc::new(CsvSink::new(file_sink_config));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    let roundtrip_plan = roundtrip_test_and_return(Arc::new(FileSinkExec::new(
        input,
        data_sink,
        schema.clone(),
        Some(sort_order),
    )))
    .unwrap();

    let roundtrip_plan = roundtrip_plan
        .as_any()
        .downcast_ref::<FileSinkExec>()
        .unwrap();
    let csv_sink = roundtrip_plan
        .sink()
        .as_any()
        .downcast_ref::<CsvSink>()
        .unwrap();
    assert_eq!(
        CompressionTypeVariant::ZSTD,
        csv_sink
            .config()
            .file_type_writer_options
            .try_into_csv()
            .unwrap()
            .compression
    );

    Ok(())
}

#[test]
fn roundtrip_parquet_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        single_file_output: true,
        overwrite: true,
        file_type_writer_options: FileTypeWriterOptions::Parquet(
            ParquetWriterOptions::new(WriterProperties::default()),
        ),
    };
    let data_sink = Arc::new(ParquetSink::new(file_sink_config));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    roundtrip_test(Arc::new(FileSinkExec::new(
        input,
        data_sink,
        schema.clone(),
        Some(sort_order),
    )))
}

#[test]
fn roundtrip_sym_hash_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let on = vec![(
        Column::new("col", schema_left.index_of("col")?),
        Column::new("col", schema_right.index_of("col")?),
    )];

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    for join_type in &[
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
    ] {
        for partition_mode in &[
            StreamJoinPartitionMode::Partitioned,
            StreamJoinPartitionMode::SinglePartition,
        ] {
            roundtrip_test(Arc::new(
                datafusion::physical_plan::joins::SymmetricHashJoinExec::try_new(
                    Arc::new(EmptyExec::new(schema_left.clone())),
                    Arc::new(EmptyExec::new(schema_right.clone())),
                    on.clone(),
                    None,
                    join_type,
                    false,
                    *partition_mode,
                )?,
            ))?;
        }
    }
    Ok(())
}

#[test]
fn roundtrip_union() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let left = EmptyExec::new(Arc::new(schema_left));
    let right = EmptyExec::new(Arc::new(schema_right));
    let inputs: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::new(left), Arc::new(right)];
    let union = UnionExec::new(inputs);
    roundtrip_test(Arc::new(union))
}

#[test]
fn roundtrip_interleave() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let partition = Partitioning::Hash(vec![], 3);
    let left = RepartitionExec::try_new(
        Arc::new(EmptyExec::new(Arc::new(schema_left))),
        partition.clone(),
    )?;
    let right = RepartitionExec::try_new(
        Arc::new(EmptyExec::new(Arc::new(schema_right))),
        partition.clone(),
    )?;
    let inputs: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::new(left), Arc::new(right)];
    let interleave = InterleaveExec::try_new(inputs)?;
    roundtrip_test(Arc::new(interleave))
}
