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
use std::ops::Deref;
use std::sync::Arc;
use std::vec;

use arrow::csv::WriterBuilder;
use datafusion::functions_aggregate::sum::sum_udaf;
use prost::Message;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema};
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
    FileSinkConfig, ParquetExec,
};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{create_udf, JoinType, Operator, Volatility};
use datafusion::physical_expr::expressions::{Max, NthValueAgg};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::physical_expr::{PhysicalSortRequirement, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    binary, cast, col, in_list, like, lit, BinaryExpr, Column, NotExpr, NthValue,
    PhysicalSortExpr,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::insert::DataSinkExec;
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, StreamJoinPartitionMode,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::udaf::create_aggregate_expr;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{
    BuiltInWindowExpr, PlainAggregateWindowExpr, WindowAggExec,
};
use datafusion::physical_plan::{
    udaf, AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, ColumnarValue, ScalarUDF,
    ScalarUDFImpl, Signature, SimpleAggregateUDF, WindowFrame, WindowFrameBound,
};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::string_agg::StringAgg;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;

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
        )?))?;
    }
    Ok(())
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

    let builtin_window_expr = Arc::new(BuiltInWindowExpr::new(
        Arc::new(NthValue::first(
            "FIRST_VALUE(a) PARTITION BY [b] ORDER BY [a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            col("a", &schema)?,
            DataType::Int64,
            false,
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
        create_aggregate_expr(
            &avg_udaf(),
            &[cast(col("b", &schema)?, &schema, DataType::Float64)?],
            &[],
            &[],
            &[],
            &schema,
            "avg(b)",
            false,
            false,
        )?,
        &[],
        &[],
        Arc::new(WindowFrame::new(None)),
    ));

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
    );

    let args = vec![cast(col("a", &schema)?, &schema, DataType::Float64)?];
    let sum_expr = udaf::create_aggregate_expr(
        &sum_udaf(),
        &args,
        &[],
        &[],
        &[],
        &schema,
        "SUM(a) RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEEDING",
        false,
        false,
    )?;

    let sliding_aggr_window_expr = Arc::new(SlidingAggregateWindowExpr::new(
        sum_expr,
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

    let test_cases: Vec<Vec<Arc<dyn AggregateExpr>>> = vec![
        // AVG
        vec![create_aggregate_expr(
            &avg_udaf(),
            &[col("b", &schema)?],
            &[],
            &[],
            &[],
            &schema,
            "AVG(b)",
            false,
            false,
        )?],
        // NTH_VALUE
        vec![Arc::new(NthValueAgg::new(
            col("b", &schema)?,
            1,
            "NTH_VALUE(b, 1)".to_string(),
            DataType::Int64,
            false,
            Vec::new(),
            Vec::new(),
        ))],
        // STRING_AGG
        vec![udaf::create_aggregate_expr(
            &AggregateUDF::new_from_impl(StringAgg::new()),
            &[
                cast(col("b", &schema)?, &schema, DataType::Utf8)?,
                lit(ScalarValue::Utf8(Some(",".to_string()))),
            ],
            &[],
            &[],
            &[],
            &schema,
            "STRING_AGG(name, ',')",
            false,
            false,
        )?],
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

    let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![create_aggregate_expr(
        &avg_udaf(),
        &[col("b", &schema)?],
        &[],
        &[],
        &[],
        &schema,
        "AVG(b)",
        false,
        false,
    )?];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates.clone(),
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    let agg = agg.with_limit(Some(12));
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

    let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![udaf::create_aggregate_expr(
        &udaf,
        &[col("b", &schema)?],
        &[],
        &[],
        &[],
        &schema,
        "example_agg",
        false,
        false,
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
    roundtrip_test(
        ParquetExec::builder(scan_config)
            .with_predicate(predicate)
            .build_arc(),
    )
}

#[tokio::test]
async fn roundtrip_parquet_exec_with_table_partition_cols() -> Result<()> {
    let mut file_group =
        PartitionedFile::new("/path/to/part=0/file.parquet".to_string(), 1024);
    file_group.partition_values =
        vec![wrap_partition_value_in_dict(ScalarValue::Int64(Some(0)))];
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let scan_config = FileScanConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_groups: vec![vec![file_group]],
        statistics: Statistics::new_unknown(&schema),
        file_schema: schema,
        projection: Some(vec![0, 1]),
        limit: None,
        table_partition_cols: vec![Field::new(
            "part".to_string(),
            wrap_partition_type_in_dict(DataType::Int16),
            false,
        )],
        output_ordering: vec![],
    };

    roundtrip_test(ParquetExec::builder(scan_config).build_arc())
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
        Arc::new(DataType::Int64),
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

#[test]
fn roundtrip_scalar_udf_extension_codec() -> Result<()> {
    #[derive(Debug)]
    struct MyRegexUdf {
        signature: Signature,
        // regex as original string
        pattern: String,
    }

    impl MyRegexUdf {
        fn new(pattern: String) -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
                pattern,
            }
        }
    }

    /// Implement the ScalarUDFImpl trait for MyRegexUdf
    impl ScalarUDFImpl for MyRegexUdf {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "regex_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, args: &[DataType]) -> Result<DataType> {
            if !matches!(args.first(), Some(&DataType::Utf8)) {
                return plan_err!("regex_udf only accepts Utf8 arguments");
            }
            Ok(DataType::Int64)
        }

        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct MyRegexUdfNode {
        #[prost(string, tag = "1")]
        pub pattern: String,
    }

    #[derive(Debug)]
    pub struct ScalarUDFExtensionCodec {}

    impl PhysicalExtensionCodec for ScalarUDFExtensionCodec {
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
                    DataFusionError::Internal(format!(
                        "failed to decode regex_udf: {}",
                        err
                    ))
                })?;

                Ok(Arc::new(ScalarUDF::new_from_impl(MyRegexUdf::new(
                    proto.pattern,
                ))))
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
                proto.encode(buf).map_err(|e| {
                    DataFusionError::Internal(format!("failed to encode udf: {e:?}"))
                })?;
            }
            Ok(())
        }
    }

    let field_text = Field::new("text", DataType::Utf8, true);
    let field_published = Field::new("published", DataType::Boolean, false);
    let field_author = Field::new("author", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_text, field_published, field_author]));
    let input = Arc::new(EmptyExec::new(schema.clone()));

    let pattern = ".*";
    let udf = ScalarUDF::from(MyRegexUdf::new(pattern.to_string()));
    let udf_expr = Arc::new(ScalarFunctionExpr::new(
        udf.name(),
        Arc::new(udf.clone()),
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

    let window = Arc::new(WindowAggExec::try_new(
        vec![Arc::new(PlainAggregateWindowExpr::new(
            Arc::new(Max::new(udf_expr.clone(), "max", DataType::Int64)),
            &[col("author", &schema)?],
            &[],
            Arc::new(WindowFrame::new(None)),
        ))],
        filter,
        vec![col("author", &schema)?],
    )?);

    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![Arc::new(Max::new(udf_expr, "max", DataType::Int64))],
        vec![None],
        window,
        schema.clone(),
    )?);

    let ctx = SessionContext::new();
    let codec = ScalarUDFExtensionCodec {};
    roundtrip_test_and_return(aggregate, &ctx, &codec)?;
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
        overwrite: true,
        keep_partition_by_columns: true,
    };
    let data_sink = Arc::new(JsonSink::new(
        file_sink_config,
        JsonWriterOptions::new(CompressionTypeVariant::UNCOMPRESSED),
    ));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    roundtrip_test(Arc::new(DataSinkExec::new(
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
        overwrite: true,
        keep_partition_by_columns: true,
    };
    let data_sink = Arc::new(CsvSink::new(
        file_sink_config,
        CsvWriterOptions::new(WriterBuilder::default(), CompressionTypeVariant::ZSTD),
    ));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let roundtrip_plan = roundtrip_test_and_return(
        Arc::new(DataSinkExec::new(
            input,
            data_sink,
            schema.clone(),
            Some(sort_order),
        )),
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
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        overwrite: true,
        keep_partition_by_columns: true,
    };
    let data_sink = Arc::new(ParquetSink::new(
        file_sink_config,
        TableParquetOptions::default(),
    ));
    let sort_order = vec![PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )];

    roundtrip_test(Arc::new(DataSinkExec::new(
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
                Some(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("col", schema_left.index_of("col")?)),
                    options: Default::default(),
                }]),
            ] {
                for right_order in &[
                    None,
                    Some(vec![PhysicalSortExpr {
                        expr: Arc::new(Column::new("col", schema_right.index_of("col")?)),
                        options: Default::default(),
                    }]),
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
        partition.clone(),
    )?;
    let inputs: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::new(left), Arc::new(right)];
    let interleave = InterleaveExec::try_new(inputs)?;
    roundtrip_test(Arc::new(interleave))
}
