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

//! Plans that do not (yet) warrant a file of their own: unions, unnest,
//! repartitioning and the analyze/explain execs.

use super::{roundtrip_test, roundtrip_test_and_return};
use arrow::datatypes::{Fields, TimeUnit};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, col, lit};
use datafusion::physical_plan::metrics::{MetricCategory, MetricType};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RangePartitioning, SplitPoint,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::display::{PlanType, StringifiedPlan};
use datafusion_common::format::ExplainFormat;
use datafusion_common::{DataFusionError, Result, UnnestOptions};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    PhysicalPlanDecodeContext,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_analyze() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]));
    let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
    let metric_categories = vec![MetricCategory::Rows, MetricCategory::Timing];
    let analyze = Arc::new(
        AnalyzeExec::builder(true, true, input, Arc::clone(&schema))
            .with_metric_categories(Some(metric_categories.clone()))
            .with_format(ExplainFormat::Tree)
            .build(),
    );

    let ctx = SessionContext::new();
    let roundtripped = roundtrip_test_and_return(
        analyze,
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let roundtripped = roundtripped.downcast_ref::<AnalyzeExec>().unwrap();

    assert_eq!(roundtripped.schema(), schema);
    assert!(roundtripped.verbose());
    assert!(roundtripped.show_statistics());
    assert_eq!(
        roundtripped.metric_categories(),
        Some(metric_categories.as_slice())
    );
    assert_eq!(roundtripped.format(), &ExplainFormat::Tree);
    assert!(
        roundtripped
            .input()
            .downcast_ref::<PlaceholderRowExec>()
            .is_some()
    );
    Ok(())
}

#[test]
fn roundtrip_analyze_metric_types() -> Result<()> {
    use protobuf::MetricType as ProtoMetricType;

    let codec = DefaultPhysicalExtensionCodec {};
    let ctx = SessionContext::new();

    for (metric_types, expected) in [
        (
            Some(vec![MetricType::Summary]),
            vec![ProtoMetricType::Summary as i32],
        ),
        (
            Some(vec![MetricType::Dev]),
            vec![ProtoMetricType::Dev as i32],
        ),
        (Some(vec![]), vec![]),
        (
            None,
            vec![ProtoMetricType::Summary as i32, ProtoMetricType::Dev as i32],
        ),
    ] {
        let legacy = metric_types.is_none();
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
        let builder = AnalyzeExec::builder(false, false, input, schema);
        let analyze = Arc::new(match metric_types {
            Some(metric_types) => builder.with_metric_types(metric_types).build(),
            None => builder.build(),
        });
        let mut node = PhysicalPlanNode::try_from_physical_plan(analyze, &codec)?;

        let Some(protobuf::physical_plan_node::PhysicalPlanType::Analyze(analyze)) =
            node.physical_plan_type.as_mut()
        else {
            unreachable!("expected AnalyzeExecNode")
        };
        if legacy {
            analyze.has_metric_types = false;
            analyze.metric_types.clear();
        }

        let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        #[cfg(feature = "json")]
        let node: PhysicalPlanNode =
            serde_json::from_str(&serde_json::to_string(&node).unwrap()).unwrap();
        let roundtripped = node.try_into_physical_plan(&ctx.task_ctx(), &codec)?;
        let node = PhysicalPlanNode::try_from_physical_plan(roundtripped, &codec)?;
        let Some(protobuf::physical_plan_node::PhysicalPlanType::Analyze(analyze)) =
            node.physical_plan_type.as_ref()
        else {
            unreachable!("expected AnalyzeExecNode")
        };
        assert!(analyze.has_metric_types);
        assert_eq!(analyze.metric_types, expected);
    }
    Ok(())
}

#[test]
fn decode_malformed_analyze_exec_node() -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]));
    let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
    let analyze = Arc::new(
        AnalyzeExec::builder(false, false, input, schema)
            .with_metric_types(vec![MetricType::Summary])
            .build(),
    );
    let mut node = PhysicalPlanNode::try_from_physical_plan(analyze, &codec)?;

    let mut invalid_schema_node = node.clone();
    let Some(protobuf::physical_plan_node::PhysicalPlanType::Analyze(analyze)) =
        invalid_schema_node.physical_plan_type.as_mut()
    else {
        unreachable!("expected AnalyzeExecNode")
    };
    analyze
        .schema
        .as_mut()
        .unwrap()
        .columns
        .push(protobuf::Field::default());
    let error = invalid_schema_node
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .unwrap_err();
    assert!(error.strip_backtrace().contains("arrow_type"));

    let Some(protobuf::physical_plan_node::PhysicalPlanType::Analyze(analyze)) =
        node.physical_plan_type.as_mut()
    else {
        unreachable!("expected AnalyzeExecNode")
    };
    analyze.metric_types = vec![i32::MAX];
    let error = node
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .unwrap_err();
    assert!(error.strip_backtrace().contains("unknown MetricType"));

    Ok(())
}

#[cfg(feature = "json")]
#[test]
fn analyze_metric_types_json() {
    use protobuf::{AnalyzeExecNode, MetricType as ProtoMetricType};

    struct FailAfterFirstByte(bool);

    impl std::io::Write for FailAfterFirstByte {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            if self.0 {
                return Err(std::io::Error::other("expected write failure"));
            }
            self.0 = true;
            Ok(1)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let node = AnalyzeExecNode {
        has_metric_types: true,
        metric_types: vec![ProtoMetricType::Summary as i32, ProtoMetricType::Dev as i32],
        ..Default::default()
    };
    let json = serde_json::to_string(&node).unwrap();
    assert_eq!(
        json,
        r#"{"hasMetricTypes":true,"metricTypes":["METRIC_TYPE_SUMMARY","METRIC_TYPE_DEV"]}"#
    );

    let node: AnalyzeExecNode = serde_json::from_str(
        r#"{"has_metric_types":true,"metric_types":["METRIC_TYPE_DEV"]}"#,
    )
    .unwrap();
    assert!(node.has_metric_types);
    assert_eq!(node.metric_types, [ProtoMetricType::Dev as i32]);

    for json in [
        r#"{"hasMetricTypes":true,"has_metric_types":false}"#,
        r#"{"metricTypes":[],"metric_types":[]}"#,
    ] {
        let error = serde_json::from_str::<AnalyzeExecNode>(json).unwrap_err();
        assert!(error.to_string().contains("duplicate field"));
    }

    for json in [
        r#"{"hasMetricTypes":"true"}"#,
        r#"{"metricTypes":true}"#,
        r#"{"metricTypes":["UNKNOWN"]}"#,
    ] {
        assert!(serde_json::from_str::<AnalyzeExecNode>(json).is_err());
    }

    let invalid = AnalyzeExecNode {
        metric_types: vec![i32::MAX],
        ..Default::default()
    };
    let error = serde_json::to_string(&invalid).unwrap_err();
    assert!(error.to_string().contains("Invalid variant"));

    for node in [
        AnalyzeExecNode {
            has_metric_types: true,
            ..Default::default()
        },
        AnalyzeExecNode {
            metric_types: vec![ProtoMetricType::Summary as i32],
            ..Default::default()
        },
    ] {
        let error = serde_json::to_writer(FailAfterFirstByte(false), &node).unwrap_err();
        assert!(error.to_string().contains("expected write failure"));
    }
}

#[test]
fn roundtrip_explain() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]));
    let stringified_plans = vec![
        StringifiedPlan::new(PlanType::InitialLogicalPlan, "initial logical"),
        StringifiedPlan::new(
            PlanType::AnalyzedLogicalPlan {
                analyzer_name: "analyzer".to_string(),
            },
            "analyzed logical",
        ),
        StringifiedPlan::new(PlanType::FinalAnalyzedLogicalPlan, "final analyzed"),
        StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "logical optimizer".to_string(),
            },
            "optimized logical",
        ),
        StringifiedPlan::new(PlanType::FinalLogicalPlan, "final logical"),
        StringifiedPlan::new(PlanType::InitialPhysicalPlan, "initial physical"),
        StringifiedPlan::new(
            PlanType::InitialPhysicalPlanWithStats,
            "initial physical with stats",
        ),
        StringifiedPlan::new(
            PlanType::InitialPhysicalPlanWithSchema,
            "initial physical with schema",
        ),
        StringifiedPlan::new(
            PlanType::OptimizedPhysicalPlan {
                optimizer_name: "physical optimizer".to_string(),
            },
            "optimized physical",
        ),
        StringifiedPlan::new(PlanType::FinalPhysicalPlan, "final physical"),
        StringifiedPlan::new(
            PlanType::FinalPhysicalPlanWithStats,
            "final physical with stats",
        ),
        StringifiedPlan::new(
            PlanType::FinalPhysicalPlanWithSchema,
            "final physical with schema",
        ),
        StringifiedPlan::new(PlanType::PhysicalPlanError, "physical plan error"),
    ];
    let explain = Arc::new(ExplainExec::new(
        Arc::clone(&schema),
        stringified_plans.clone(),
        true,
    ));

    let ctx = SessionContext::new();
    let roundtripped = roundtrip_test_and_return(
        explain,
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let roundtripped = roundtripped.downcast_ref::<ExplainExec>().unwrap();

    assert_eq!(roundtripped.schema(), schema);
    assert_eq!(roundtripped.stringified_plans(), stringified_plans);
    assert!(roundtripped.verbose());
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
    let union = UnionExec::try_new(inputs)?;
    roundtrip_test(union)
}

/// `UnionExec::try_new` coerces a nullability-mismatched leg by wrapping it
/// in a `ProjectionExec` with a same-type `CastExpr` (see `coerce_schema` in
/// `datafusion-physical-plan`'s `union` module) -- a zero-copy relabeling,
/// not a real cast. `ProjectionExec` has an ordinary protobuf message, so
/// unlike the node this replaced, there's no wrapper-erasure trick to verify;
/// just that the decoded plan still contains the coercion and that its
/// emitted batches expose the union's nullable schema.
#[tokio::test]
async fn roundtrip_union_with_mismatched_nullability_executes() -> Result<()> {
    let literal_leg = |value: ScalarValue| -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: lit(value),
                alias: "a".to_string(),
            }],
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))),
        )?))
    };
    let non_nullable_leg = literal_leg(ScalarValue::Int64(Some(1)))?;
    let nullable_leg = literal_leg(ScalarValue::Int64(None))?;

    let union: Arc<dyn ExecutionPlan> =
        UnionExec::try_new(vec![non_nullable_leg, nullable_leg])?;
    assert!(union.schema().field(0).is_nullable());
    assert!(
        format!("{union:?}").contains("CastExpr"),
        "expected a coercing CastExpr in plan:\n{union:?}"
    );

    let ctx = SessionContext::new();
    let bytes = datafusion_proto::bytes::physical_plan_to_bytes(Arc::clone(&union))?;
    let roundtripped = datafusion_proto::bytes::physical_plan_from_bytes(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
    )?;
    assert!(roundtripped.schema().field(0).is_nullable());
    assert!(
        format!("{roundtripped:?}").contains("CastExpr"),
        "expected a coercing CastExpr after roundtrip:\n{roundtripped:?}"
    );

    let batches =
        datafusion::physical_plan::collect(roundtripped, ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for batch in &batches {
        assert!(batch.schema().field(0).is_nullable());
    }

    Ok(())
}

#[test]
fn roundtrip_repartition_preserve_order() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a]));
    let sort_exprs: LexOrdering = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

    // Create two sorted single-partition inputs, then union them to get
    // a sorted input with 2 partitions.
    let source1 = SortExec::new(
        sort_exprs.clone(),
        Arc::new(EmptyExec::new(Arc::clone(&schema))),
    );
    let source2 = SortExec::new(sort_exprs, Arc::new(EmptyExec::new(schema)));
    let union = UnionExec::try_new(vec![
        Arc::new(source1) as Arc<dyn ExecutionPlan>,
        Arc::new(source2) as Arc<dyn ExecutionPlan>,
    ])?;

    let repartition = RepartitionExec::try_new(union, Partitioning::RoundRobinBatch(10))?
        .with_preserve_order();
    assert!(repartition.preserve_order());

    roundtrip_test(Arc::new(repartition))
}

#[test]
fn roundtrip_range_partitioning() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let range_partitioning = Partitioning::Range(RangePartitioning::new(
        [PhysicalSortExpr::new_default(col("a", &schema)?)].into(),
        vec![SplitPoint::new(vec![ScalarValue::Int64(Some(10))])],
    ));
    // RepartitionExec is used only to carry the partitioning through proto.
    // Executing range repartitioning is intentionally unsupported.
    let repartition = RepartitionExec::try_new(input, range_partitioning)?;

    roundtrip_test(Arc::new(repartition))
}

/// `parse_protobuf_hash_partitioning` has no in-tree callers left; it delegates
/// to the shared `Partitioning::try_from_proto`, so pin that it still decodes
/// the hash message it is handed.
#[test]
fn parse_hash_partitioning_delegates_to_shared_decoder() -> Result<()> {
    use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;

    let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let codec = DefaultPhysicalExtensionCodec {};
    let decode_ctx = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
    let proto_converter = DefaultPhysicalProtoConverter {};

    let hash_expr = serialize_physical_expr_with_converter(
        &col("a", &schema)?,
        &codec,
        &proto_converter,
    )?;
    let hash = protobuf::PhysicalHashRepartition {
        hash_expr: vec![hash_expr],
        partition_count: 4,
    };

    let partitioning = parse_protobuf_hash_partitioning(
        Some(&hash),
        &decode_ctx,
        &schema,
        &proto_converter,
    )?;
    let Some(Partitioning::Hash(exprs, count)) = partitioning else {
        panic!("expected hash partitioning, got {partitioning:?}");
    };
    assert_eq!(count, 4);
    assert_eq!(exprs.len(), 1);
    assert_eq!(exprs[0].to_string(), col("a", &schema)?.to_string());

    // No message means no partitioning, as before.
    assert!(
        parse_protobuf_hash_partitioning(None, &decode_ctx, &schema, &proto_converter)?
            .is_none()
    );

    // The count is a `u64` on the wire and a `usize` in memory, so decoding
    // narrows it. A count that does not fit is the case that motivated routing
    // this through the shared decoder: it used to `unwrap()` and panic, and now
    // reports an error. Only a target narrower than 64 bits can reach that arm
    // -- on a 64-bit target every `u64` fits, and the assertion there is that
    // the largest possible count survives whole rather than being truncated.
    let oversized = protobuf::PhysicalHashRepartition {
        hash_expr: vec![serialize_physical_expr_with_converter(
            &col("a", &schema)?,
            &codec,
            &proto_converter,
        )?],
        partition_count: u64::MAX,
    };
    let decoded = parse_protobuf_hash_partitioning(
        Some(&oversized),
        &decode_ctx,
        &schema,
        &proto_converter,
    );

    #[cfg(target_pointer_width = "64")]
    {
        let Some(Partitioning::Hash(_, count)) = decoded? else {
            panic!("expected hash partitioning");
        };
        assert_eq!(count, usize::MAX);
    }

    #[cfg(not(target_pointer_width = "64"))]
    assert!(decoded.unwrap_err().to_string().contains(
        "Partitioning: partition_count wire value 18446744073709551615 is out of range for usize"
    ));

    Ok(())
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

/// See [`roundtrip_union_with_mismatched_nullability_executes`]: the same
/// wrapper-reinsertion behavior applies to `InterleaveExec::try_from_proto`.
#[tokio::test]
async fn roundtrip_interleave_with_mismatched_nullability_executes() -> Result<()> {
    let partition = Partitioning::Hash(vec![], 3);
    let literal_leg = |value: ScalarValue| -> Result<Arc<dyn ExecutionPlan>> {
        let projection = ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: lit(value),
                alias: "a".to_string(),
            }],
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))),
        )?;
        Ok(Arc::new(RepartitionExec::try_new(
            Arc::new(projection),
            partition.clone(),
        )?))
    };
    let non_nullable_leg = literal_leg(ScalarValue::Int64(Some(1)))?;
    let nullable_leg = literal_leg(ScalarValue::Int64(None))?;

    let interleave: Arc<dyn ExecutionPlan> = Arc::new(InterleaveExec::try_new(vec![
        non_nullable_leg,
        nullable_leg,
    ])?);
    assert!(interleave.schema().field(0).is_nullable());
    assert!(
        format!("{interleave:?}").contains("CastExpr"),
        "expected a coercing CastExpr in plan:\n{interleave:?}"
    );

    let ctx = SessionContext::new();
    let bytes = datafusion_proto::bytes::physical_plan_to_bytes(Arc::clone(&interleave))?;
    let roundtripped = datafusion_proto::bytes::physical_plan_from_bytes(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
    )?;
    assert!(roundtripped.schema().field(0).is_nullable());
    assert!(
        format!("{roundtripped:?}").contains("CastExpr"),
        "expected a coercing CastExpr after roundtrip:\n{roundtripped:?}"
    );

    let batches =
        datafusion::physical_plan::collect(roundtripped, ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for batch in &batches {
        assert!(batch.schema().field(0).is_nullable());
    }

    Ok(())
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
    let options = UnnestOptions {
        null_handling: datafusion_common::NullHandling::Drop,
        recursions: vec![datafusion_common::RecursionUnnestOption {
            input_column: datafusion_common::Column::new_unqualified("b"),
            output_column: datafusion_common::Column::new_unqualified("b"),
            depth: 2,
        }],
    };
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
        options.clone(),
    )?;
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let result =
        roundtrip_test_and_return(Arc::new(unnest), &ctx, &codec, &proto_converter)?;
    let result = result.downcast_ref::<UnnestExec>().unwrap();
    assert_eq!(result.options(), &options);

    Ok(())
}

#[tokio::test]
/// Tests that we can serialize an unoptimized "analyze" plan and it will work on the other end
async fn analyze_roundtrip_unoptimized() -> Result<()> {
    let ctx = SessionContext::new();

    // No optimizations
    let session_state =
        datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state())
            .with_physical_optimizer_rules(vec![])
            .build();

    let logical_plan = session_state
        .create_logical_plan("explain analyze select 1")
        .await?;
    let plan = session_state.create_physical_plan(&logical_plan).await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;

    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let unoptimized =
        node.try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;

    let physical_planner =
        datafusion::physical_planner::DefaultPhysicalPlanner::default();
    physical_planner.optimize_physical_plan(unoptimized, &session_state, |_, _| {})?;
    Ok(())
}
