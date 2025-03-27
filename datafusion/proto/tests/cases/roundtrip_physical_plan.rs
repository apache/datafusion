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
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use std::vec;

use crate::cases::{
    CustomUDWF, CustomUDWFNode, MyAggregateUDF, MyAggregateUdfNode, MyRegexUdf,
    MyRegexUdfNode,
};

use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder;
use arrow::datatypes::{Fields, TimeUnit};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_expr::dml::InsertOp;
use datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion_functions_aggregate::array_agg::array_agg_udaf;
use datafusion_functions_aggregate::min_max::max_udaf;
use prost::Message;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfig,
    FileSinkConfig, FileSource, ParquetSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::nth_value::nth_value_udwf;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::{create_udf, JoinType, Operator, Volatility};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalSortRequirement, ScalarFunctionExpr,
};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    binary, cast, col, in_list, like, lit, BinaryExpr, Column, NotExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, StreamJoinPartitionMode,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion::physical_plan::windows::{
    create_udwf_window_expr, BoundedWindowAggExec, PlainAggregateWindowExpr,
    WindowAggExec,
};
use datafusion::physical_plan::{
    displayable, ExecutionPlan, InputOrderMode, Partitioning, PhysicalExpr, Statistics,
};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{
    internal_err, not_impl_err, DataFusionError, Result, UnnestOptions,
};
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, ColumnarValue, ScalarUDF,
    Signature, SimpleAggregateUDF, WindowFrame, WindowFrameBound, WindowUDF,
};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::nth_value::nth_value_udaf;
use datafusion_functions_aggregate::string_agg::string_agg_udaf;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    roundtrip_test_and_return(exec_plan, &ctx, &codec)?;
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
    ctx: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let proto: protobuf::PhysicalPlanNode =
        protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), codec)
            .expect("to proto");
    let runtime = ctx.runtime_env();
    let result_exec_plan: Arc<dyn ExecutionPlan> = proto
        .try_into_physical_plan(ctx, runtime.deref(), codec)
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
    ctx: &SessionContext,
) -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    roundtrip_test_and_return(exec_plan, ctx, &codec)?;
    Ok(())
}

/// Perform a serde roundtrip for the specified sql query, and  assert that
/// query results are identical.
async fn roundtrip_test_sql_with_context(sql: &str, ctx: &SessionContext) -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let initial_plan = ctx.sql(sql).await?.create_physical_plan().await?;

    roundtrip_test_and_return(initial_plan, ctx, &codec)?;
    Ok(())
}

/// returns a SessionContext with `alltypes_plain` registered
async fn all_types_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    Ok(ctx)
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
        Arc::new(Column::new("col", schema_left.index_of("col")?)) as _,
        Arc::new(Column::new("col", schema_right.index_of("col")?)) as _,
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
                None,
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
            Some(vec![0]),
        )?))?;
    }
    Ok(())
}

#[test]
fn roundtrip_udwf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let udwf_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &schema,
            "row_number() PARTITION BY [a] ORDER BY [b] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
            false,
        )?,
        &[
            col("a", &schema)?
        ],
        &LexOrdering::new(vec![
            PhysicalSortExpr::new(col("b", &schema)?, SortOptions::new(true, true)),
        ]),
        Arc::new(WindowFrame::new(None)),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(BoundedWindowAggExec::try_new(
        vec![udwf_expr],
        input,
        InputOrderMode::Sorted,
        true,
    )?))
}

#[test]
fn roundtrip_window() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
        WindowFrameBound::CurrentRow,
    );

    let nth_value_window =
        create_udwf_window_expr(
            &nth_value_udwf(),
            &[col("a", &schema)?,
                lit(2)], schema.as_ref(),
            "NTH_VALUE(a, 2) PARTITION BY [b] ORDER BY [a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
            false,
        )?;
    let udwf_expr = Arc::new(StandardWindowExpr::new(
        nth_value_window,
        &[col("b", &schema)?],
        &LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]),
        Arc::new(window_frame),
    ));

    let plain_aggr_window_expr = Arc::new(PlainAggregateWindowExpr::new(
        AggregateExprBuilder::new(
            avg_udaf(),
            vec![cast(col("b", &schema)?, &schema, DataType::Float64)?],
        )
        .schema(Arc::clone(&schema))
        .alias("avg(b)")
        .build()
        .map(Arc::new)?,
        &[],
        &LexOrdering::default(),
        Arc::new(WindowFrame::new(None)),
    ));

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
    );

    let args = vec![cast(col("a", &schema)?, &schema, DataType::Float64)?];
    let sum_expr = AggregateExprBuilder::new(sum_udaf(), args)
        .schema(Arc::clone(&schema))
        .alias("SUM(a) RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEEDING")
        .build()
        .map(Arc::new)?;

    let sliding_aggr_window_expr = Arc::new(SlidingAggregateWindowExpr::new(
        sum_expr,
        &[],
        &LexOrdering::default(),
        Arc::new(window_frame),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(WindowAggExec::try_new(
        vec![plain_aggr_window_expr, sliding_aggr_window_expr, udwf_expr],
        input,
        false,
    )?))
}

#[test]
fn rountrip_aggregate() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let avg_expr = AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
        .schema(Arc::clone(&schema))
        .alias("AVG(b)")
        .build()?;
    let nth_expr =
        AggregateExprBuilder::new(nth_value_udaf(), vec![col("b", &schema)?, lit(1u64)])
            .schema(Arc::clone(&schema))
            .alias("NTH_VALUE(b, 1)")
            .build()?;
    let str_agg_expr =
        AggregateExprBuilder::new(string_agg_udaf(), vec![col("b", &schema)?, lit(1u64)])
            .schema(Arc::clone(&schema))
            .alias("NTH_VALUE(b, 1)")
            .build()?;

    let test_cases = vec![
        // AVG
        vec![Arc::new(avg_expr)],
        // NTH_VALUE
        vec![Arc::new(nth_expr)],
        // STRING_AGG
        vec![Arc::new(str_agg_expr)],
    ];

    for aggregates in test_cases {
        let schema = schema.clone();
        roundtrip_test(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
            aggregates,
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?))?;
    }

    Ok(())
}

#[test]
fn rountrip_aggregate_with_limit() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates =
        vec![
            AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("AVG(b)")
                .build()
                .map(Arc::new)?,
        ];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    let agg = agg.with_limit(Some(12));
    roundtrip_test(Arc::new(agg))
}

#[test]
fn rountrip_aggregate_with_approx_pencentile_cont() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates = vec![AggregateExprBuilder::new(
        approx_percentile_cont_udaf(),
        vec![col("b", &schema)?, lit(0.5)],
    )
    .schema(Arc::clone(&schema))
    .alias("APPROX_PERCENTILE_CONT(b, 0.5)")
    .build()
    .map(Arc::new)?];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    roundtrip_test(Arc::new(agg))
}

#[test]
fn rountrip_aggregate_with_sort() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];
    let sort_exprs = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("b", &schema)?,
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    }]);

    let aggregates =
        vec![
            AggregateExprBuilder::new(array_agg_udaf(), vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("ARRAY_AGG(b)")
                .order_by(sort_exprs)
                .build()
                .map(Arc::new)?,
        ];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    roundtrip_test(Arc::new(agg))
}

#[test]
fn roundtrip_aggregate_udaf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    #[derive(Debug)]
    struct Example;
    impl Accumulator for Example {
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Int64(Some(0))])
        }

        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int64(Some(0)))
        }

        fn size(&self) -> usize {
            0
        }
    }

    let return_type = DataType::Int64;
    let accumulator: AccumulatorFactoryFunction = Arc::new(|_| Ok(Box::new(Example)));

    let udaf = AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "example",
        Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        return_type,
        accumulator,
        vec![Field::new("value", DataType::Int64, true)],
    ));

    let ctx = SessionContext::new();
    ctx.register_udaf(udaf.clone());

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates =
        vec![
            AggregateExprBuilder::new(Arc::new(udaf), vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("example_agg")
                .build()
                .map(Arc::new)?,
        ];

    roundtrip_test_with_context(
        Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
            aggregates,
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?),
        &ctx,
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
    let sort_exprs = LexOrdering::new(vec![
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
    ]);
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
    let sort_exprs = LexOrdering::new(vec![
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
    ]);

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
fn roundtrip_coalesce_with_fetch() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    roundtrip_test(Arc::new(CoalesceBatchesExec::new(
        Arc::new(EmptyExec::new(schema.clone())),
        8096,
    )))?;

    roundtrip_test(Arc::new(
        CoalesceBatchesExec::new(Arc::new(EmptyExec::new(schema)), 8096)
            .with_fetch(Some(10)),
    ))
}

#[test]
fn roundtrip_parquet_exec_with_pruning_predicate() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("col", 1)),
        Operator::Eq,
        lit("1"),
    ));

    let mut options = TableParquetOptions::new();
    options.global.pushdown_filters = true;

    let file_source = Arc::new(
        ParquetSource::new(options).with_predicate(Arc::clone(&file_schema), predicate),
    );

    let scan_config =
        FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema, file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            });

    roundtrip_test(scan_config.build())
}

#[tokio::test]
async fn roundtrip_parquet_exec_with_table_partition_cols() -> Result<()> {
    let mut file_group =
        PartitionedFile::new("/path/to/part=0/file.parquet".to_string(), 1024);
    file_group.partition_values =
        vec![wrap_partition_value_in_dict(ScalarValue::Int64(Some(0)))];
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let file_source = Arc::new(ParquetSource::default());
    let scan_config =
        FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema, file_source)
            .with_projection(Some(vec![0, 1]))
            .with_file_group(FileGroup::new(vec![file_group]))
            .with_table_partition_cols(vec![Field::new(
                "part".to_string(),
                wrap_partition_type_in_dict(DataType::Int16),
                false,
            )])
            .with_newlines_in_values(false);

    roundtrip_test(scan_config.build())
}

#[test]
fn roundtrip_parquet_exec_with_custom_predicate_expr() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let custom_predicate_expr = Arc::new(CustomPredicateExpr {
        inner: Arc::new(Column::new("col", 1)),
    });

    let file_source = Arc::new(
        ParquetSource::default()
            .with_predicate(Arc::clone(&file_schema), custom_predicate_expr),
    );

    let scan_config =
        FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema, file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            });

    #[derive(Debug, Clone, Eq)]
    struct CustomPredicateExpr {
        inner: Arc<dyn PhysicalExpr>,
    }

    // Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
    impl PartialEq for CustomPredicateExpr {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner)
        }
    }

    impl std::hash::Hash for CustomPredicateExpr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Display for CustomPredicateExpr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CustomPredicateExpr")
        }
    }

    impl PhysicalExpr for CustomPredicateExpr {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            unreachable!()
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            unreachable!()
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            unreachable!()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            todo!()
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, f)
        }
    }

    #[derive(Debug)]
    struct CustomPhysicalExtensionCodec;
    impl PhysicalExtensionCodec for CustomPhysicalExtensionCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _registry: &dyn FunctionRegistry,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unreachable!()
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
        ) -> Result<()> {
            unreachable!()
        }

        fn try_decode_expr(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn PhysicalExpr>],
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if buf == "CustomPredicateExpr".as_bytes() {
                Ok(Arc::new(CustomPredicateExpr {
                    inner: inputs[0].clone(),
                }))
            } else {
                internal_err!("Not supported")
            }
        }

        fn try_encode_expr(
            &self,
            node: &Arc<dyn PhysicalExpr>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            if node
                .as_any()
                .downcast_ref::<CustomPredicateExpr>()
                .is_some()
            {
                buf.extend_from_slice("CustomPredicateExpr".as_bytes());
                Ok(())
            } else {
                internal_err!("Not supported")
            }
        }
    }

    let exec_plan = scan_config.build();

    let ctx = SessionContext::new();
    roundtrip_test_and_return(exec_plan, &ctx, &CustomPhysicalExtensionCodec {})?;
    Ok(())
}

#[test]
fn roundtrip_scalar_udf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    let scalar_fn = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::new(array.clone()) as ArrayRef))
    });

    let udf = create_udf(
        "dummy",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        scalar_fn.clone(),
    );

    let fun_def = Arc::new(udf.clone());

    let expr = ScalarFunctionExpr::new(
        "dummy",
        fun_def,
        vec![col("a", &schema)?],
        DataType::Int64,
    );

    let project =
        ProjectionExec::try_new(vec![(Arc::new(expr), "a".to_string())], input)?;

    let ctx = SessionContext::new();

    ctx.register_udf(udf);

    roundtrip_test_with_context(Arc::new(project), &ctx)
}

#[derive(Debug)]
struct UDFExtensionCodec;

impl PhysicalExtensionCodec for UDFExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("No extension codec provided")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("No extension codec provided")
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == "regex_udf" {
            let proto = MyRegexUdfNode::decode(buf).map_err(|err| {
                DataFusionError::Internal(format!("failed to decode regex_udf: {err}"))
            })?;

            Ok(Arc::new(ScalarUDF::from(MyRegexUdf::new(proto.pattern))))
        } else {
            not_impl_err!("unrecognized scalar UDF implementation, cannot decode")
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let binding = node.inner();
        if let Some(udf) = binding.as_any().downcast_ref::<MyRegexUdf>() {
            let proto = MyRegexUdfNode {
                pattern: udf.pattern.clone(),
            };
            proto.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("failed to encode udf: {err}"))
            })?;
        }
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if name == "aggregate_udf" {
            let proto = MyAggregateUdfNode::decode(buf).map_err(|err| {
                DataFusionError::Internal(format!(
                    "failed to decode aggregate_udf: {err}"
                ))
            })?;

            Ok(Arc::new(AggregateUDF::from(MyAggregateUDF::new(
                proto.result,
            ))))
        } else {
            not_impl_err!("unrecognized scalar UDF implementation, cannot decode")
        }
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let binding = node.inner();
        if let Some(udf) = binding.as_any().downcast_ref::<MyAggregateUDF>() {
            let proto = MyAggregateUdfNode {
                result: udf.result.clone(),
            };
            proto.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("failed to encode udf: {err:?}"))
            })?;
        }
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if name == "custom_udwf" {
            let proto = CustomUDWFNode::decode(buf).map_err(|err| {
                DataFusionError::Internal(format!("failed to decode custom_udwf: {err}"))
            })?;

            Ok(Arc::new(WindowUDF::from(CustomUDWF::new(proto.payload))))
        } else {
            not_impl_err!(
                "unrecognized user-defined window function implementation, cannot decode"
            )
        }
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        let binding = node.inner();
        if let Some(udwf) = binding.as_any().downcast_ref::<CustomUDWF>() {
            let proto = CustomUDWFNode {
                payload: udwf.payload.clone(),
            };
            proto.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("failed to encode udwf: {err:?}"))
            })?;
        }
        Ok(())
    }
}

#[test]
fn roundtrip_scalar_udf_extension_codec() -> Result<()> {
    let field_text = Field::new("text", DataType::Utf8, true);
    let field_published = Field::new("published", DataType::Boolean, false);
    let field_author = Field::new("author", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_text, field_published, field_author]));
    let input = Arc::new(EmptyExec::new(schema.clone()));

    let udf_expr = Arc::new(ScalarFunctionExpr::new(
        "regex_udf",
        Arc::new(ScalarUDF::from(MyRegexUdf::new(".*".to_string()))),
        vec![col("text", &schema)?],
        DataType::Int64,
    ));

    let filter = Arc::new(FilterExec::try_new(
        Arc::new(BinaryExpr::new(
            col("published", &schema)?,
            Operator::And,
            Arc::new(BinaryExpr::new(udf_expr.clone(), Operator::Gt, lit(0))),
        )),
        input,
    )?);
    let aggr_expr =
        AggregateExprBuilder::new(max_udaf(), vec![udf_expr as Arc<dyn PhysicalExpr>])
            .schema(schema.clone())
            .alias("max")
            .build()
            .map(Arc::new)?;

    let window = Arc::new(WindowAggExec::try_new(
        vec![Arc::new(PlainAggregateWindowExpr::new(
            aggr_expr.clone(),
            &[col("author", &schema)?],
            &LexOrdering::default(),
            Arc::new(WindowFrame::new(None)),
        ))],
        filter,
        true,
    )?);

    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![aggr_expr],
        vec![None],
        window,
        schema,
    )?);

    let ctx = SessionContext::new();
    roundtrip_test_and_return(aggregate, &ctx, &UDFExtensionCodec)?;
    Ok(())
}

#[test]
fn roundtrip_udwf_extension_codec() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let custom_udwf = Arc::new(WindowUDF::from(CustomUDWF::new("payload".to_string())));
    let udwf = create_udwf_window_expr(
        &custom_udwf,
        &[col("a", &schema)?],
        schema.as_ref(),
        "custom_udwf(a) PARTITION BY [b] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
        false,
    )?;

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
        WindowFrameBound::CurrentRow,
    );

    let udwf_expr = Arc::new(StandardWindowExpr::new(
        udwf,
        &[col("b", &schema)?],
        &LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]),
        Arc::new(window_frame),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));
    let window = Arc::new(BoundedWindowAggExec::try_new(
        vec![udwf_expr],
        input,
        InputOrderMode::Sorted,
        true,
    )?);

    let ctx = SessionContext::new();
    roundtrip_test_and_return(window, &ctx, &UDFExtensionCodec)?;
    Ok(())
}

#[test]
fn roundtrip_aggregate_udf_extension_codec() -> Result<()> {
    let field_text = Field::new("text", DataType::Utf8, true);
    let field_published = Field::new("published", DataType::Boolean, false);
    let field_author = Field::new("author", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_text, field_published, field_author]));
    let input = Arc::new(EmptyExec::new(schema.clone()));

    let udf_expr = Arc::new(ScalarFunctionExpr::new(
        "regex_udf",
        Arc::new(ScalarUDF::from(MyRegexUdf::new(".*".to_string()))),
        vec![col("text", &schema)?],
        DataType::Int64,
    ));

    let udaf = Arc::new(AggregateUDF::from(MyAggregateUDF::new(
        "result".to_string(),
    )));
    let aggr_args: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(Literal::new(ScalarValue::from(42)))];

    let aggr_expr = AggregateExprBuilder::new(Arc::clone(&udaf), aggr_args.clone())
        .schema(Arc::clone(&schema))
        .alias("aggregate_udf")
        .build()
        .map(Arc::new)?;

    let filter = Arc::new(FilterExec::try_new(
        Arc::new(BinaryExpr::new(
            col("published", &schema)?,
            Operator::And,
            Arc::new(BinaryExpr::new(udf_expr, Operator::Gt, lit(0))),
        )),
        input,
    )?);

    let window = Arc::new(WindowAggExec::try_new(
        vec![Arc::new(PlainAggregateWindowExpr::new(
            aggr_expr,
            &[col("author", &schema)?],
            &LexOrdering::default(),
            Arc::new(WindowFrame::new(None)),
        ))],
        filter,
        true,
    )?);

    let aggr_expr = AggregateExprBuilder::new(udaf, aggr_args.clone())
        .schema(Arc::clone(&schema))
        .alias("aggregate_udf")
        .distinct()
        .ignore_nulls()
        .build()
        .map(Arc::new)?;

    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![aggr_expr],
        vec![None],
        window,
        schema,
    )?);

    let ctx = SessionContext::new();
    roundtrip_test_and_return(aggregate, &ctx, &UDFExtensionCodec)?;
    Ok(())
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

#[tokio::test]
async fn roundtrip_json_source() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_json("t1", "../core/tests/data/1.json", Default::default())
        .await?;
    let plan = ctx.table("t1").await?.create_physical_plan().await?;
    roundtrip_test(plan)
}

#[test]
fn roundtrip_json_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "json".into(),
    };
    let data_sink = Arc::new(JsonSink::new(
        file_sink_config,
        JsonWriterOptions::new(CompressionTypeVariant::UNCOMPRESSED),
    ));
    let sort_order = LexRequirement::new(vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]);

    roundtrip_test(Arc::new(DataSinkExec::new(
        input,
        data_sink,
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
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "csv".into(),
    };
    let data_sink = Arc::new(CsvSink::new(
        file_sink_config,
        CsvWriterOptions::new(WriterBuilder::default(), CompressionTypeVariant::ZSTD),
    ));
    let sort_order = LexRequirement::new(vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]);

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let roundtrip_plan = roundtrip_test_and_return(
        Arc::new(DataSinkExec::new(input, data_sink, Some(sort_order))),
        &ctx,
        &codec,
    )
    .unwrap();

    let roundtrip_plan = roundtrip_plan
        .as_any()
        .downcast_ref::<DataSinkExec>()
        .unwrap();
    let csv_sink = roundtrip_plan
        .sink()
        .as_any()
        .downcast_ref::<CsvSink>()
        .unwrap();
    assert_eq!(
        CompressionTypeVariant::ZSTD,
        csv_sink.writer_options().compression
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
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "parquet".into(),
    };
    let data_sink = Arc::new(ParquetSink::new(
        file_sink_config,
        TableParquetOptions::default(),
    ));
    let sort_order = LexRequirement::new(vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]);

    roundtrip_test(Arc::new(DataSinkExec::new(
        input,
        data_sink,
        Some(sort_order),
    )))
}

#[test]
fn roundtrip_sym_hash_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let on = vec![(
        Arc::new(Column::new("col", schema_left.index_of("col")?)) as _,
        Arc::new(Column::new("col", schema_right.index_of("col")?)) as _,
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
            for left_order in &[
                None,
                Some(LexOrdering::new(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("col", schema_left.index_of("col")?)),
                    options: Default::default(),
                }])),
            ] {
                for right_order in &[
                    None,
                    Some(LexOrdering::new(vec![PhysicalSortExpr {
                        expr: Arc::new(Column::new("col", schema_right.index_of("col")?)),
                        options: Default::default(),
                    }])),
                ] {
                    roundtrip_test(Arc::new(
                        datafusion::physical_plan::joins::SymmetricHashJoinExec::try_new(
                            Arc::new(EmptyExec::new(schema_left.clone())),
                            Arc::new(EmptyExec::new(schema_right.clone())),
                            on.clone(),
                            None,
                            join_type,
                            false,
                            left_order.clone(),
                            right_order.clone(),
                            *partition_mode,
                        )?,
                    ))?;
                }
            }
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
        partition,
    )?;
    let inputs: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::new(left), Arc::new(right)];
    let interleave = InterleaveExec::try_new(inputs)?;
    roundtrip_test(Arc::new(interleave))
}

#[test]
fn roundtrip_unnest() -> Result<()> {
    let fa = Field::new("a", DataType::Int64, true);
    let fb0 = Field::new_list_field(DataType::Utf8, true);
    let fb = Field::new_list("b", fb0.clone(), false);
    let fc1 = Field::new("c1", DataType::Boolean, false);
    let fc2 = Field::new("c2", DataType::Date64, true);
    let fc = Field::new_struct("c", Fields::from(vec![fc1.clone(), fc2.clone()]), true);
    let fd0 = Field::new_list_field(DataType::Float32, false);
    let fd = Field::new_list("d", fd0.clone(), true);
    let fe1 = Field::new("e1", DataType::UInt16, false);
    let fe2 = Field::new("e2", DataType::Duration(TimeUnit::Millisecond), true);
    let fe3 = Field::new("e3", DataType::Timestamp(TimeUnit::Millisecond, None), true);
    let fe_fields = Fields::from(vec![fe1.clone(), fe2.clone(), fe3.clone()]);
    let fe = Field::new_struct("e", fe_fields, false);

    let fb0 = fb0.with_name("b");
    let fd0 = fd0.with_name("d");
    let input_schema = Arc::new(Schema::new(vec![fa.clone(), fb, fc, fd, fe]));
    let output_schema =
        Arc::new(Schema::new(vec![fa, fb0, fc1, fc2, fd0, fe1, fe2, fe3]));
    let input = Arc::new(EmptyExec::new(input_schema));
    let options = UnnestOptions::default();
    let unnest = UnnestExec::new(
        input,
        vec![
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 2,
            },
            ListUnnest {
                index_in_input_schema: 3,
                depth: 2,
            },
        ],
        vec![2, 4],
        output_schema,
        options,
    );
    roundtrip_test(Arc::new(unnest))
}

#[tokio::test]
async fn roundtrip_coalesce() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(Fields::from([
            Arc::new(Field::new("f", DataType::Int64, false)),
        ]))))),
    )?;
    let df = ctx.sql("select coalesce(f) as f from t").await?;
    let plan = df.create_physical_plan().await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let restored = node.try_into_physical_plan(
        &ctx,
        ctx.runtime_env().as_ref(),
        &DefaultPhysicalExtensionCodec {},
    )?;

    assert_eq!(
        plan.schema(),
        restored.schema(),
        "Schema mismatch for plans:\n>> initial:\n{}>> final: \n{}",
        displayable(plan.as_ref())
            .set_show_schema(true)
            .indent(true),
        displayable(restored.as_ref())
            .set_show_schema(true)
            .indent(true),
    );

    Ok(())
}

#[tokio::test]
async fn roundtrip_projection_source() -> Result<()> {
    let schema = Arc::new(Schema::new(Fields::from([
        Arc::new(Field::new("a", DataType::Utf8, false)),
        Arc::new(Field::new("b", DataType::Utf8, false)),
        Arc::new(Field::new("c", DataType::Int32, false)),
        Arc::new(Field::new("d", DataType::Int32, false)),
    ])));

    let statistics = Statistics::new_unknown(&schema);

    let file_source = ParquetSource::default().with_statistics(statistics.clone());
    let scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        schema.clone(),
        file_source,
    )
    .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
        "/path/to/file.parquet".to_string(),
        1024,
    )])])
    .with_statistics(statistics)
    .with_projection(Some(vec![0, 1, 2]));

    let filter = Arc::new(
        FilterExec::try_new(
            Arc::new(BinaryExpr::new(col("c", &schema)?, Operator::Eq, lit(1))),
            scan_config.build(),
        )?
        .with_projection(Some(vec![0, 1]))?,
    );

    roundtrip_test(filter)
}

#[tokio::test]
async fn roundtrip_parquet_select_star() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_star_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_empty_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select 1 from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}
