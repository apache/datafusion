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

//! Plans carrying user defined functions, and the extension codec that
//! (de)serializes them.

use super::{roundtrip_test_and_return, roundtrip_test_with_context};
use crate::cases::{
    CustomUDWF, CustomUDWFNode, MyAggregateUDF, MyAggregateUdfNode, MyHigherOrderUDF,
    MyHigherOrderUdfNode, MyRegexUdf, MyRegexUdfNode,
};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Operator, Volatility, create_udf};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::window::StandardWindowExpr;
use datafusion::physical_expr::{HigherOrderFunctionExpr, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{BinaryExpr, PhysicalSortExpr, col, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, PlainAggregateWindowExpr, WindowAggExec,
    create_udwf_window_expr,
};
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, internal_datafusion_err, not_impl_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, HigherOrderUDF, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, WindowFrame, WindowFrameBound, WindowUDF,
};
use datafusion_functions_aggregate::min_max::max_udaf;
use datafusion_physical_expr::expressions::{LambdaVariable, is_not_null, lambda};
use datafusion_proto::physical_plan::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use prost::Message;
use std::sync::Arc;
use std::vec;

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
        Field::new("f", DataType::Int64, true).into(),
        Arc::new(ConfigOptions::default()),
    );

    let project = ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: Arc::new(expr),
            alias: "a".to_string(),
        }],
        input,
    )?;

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
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("No extension codec provided")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        not_impl_err!("No extension codec provided")
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == "regex_udf" {
            let proto = MyRegexUdfNode::decode(buf).map_err(|err| {
                internal_datafusion_err!("failed to decode regex_udf: {err}")
            })?;

            Ok(Arc::new(ScalarUDF::from(MyRegexUdf::new(proto.pattern))))
        } else {
            not_impl_err!("unrecognized scalar UDF implementation, cannot decode")
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let binding = node.inner();
        if let Some(udf) = binding.downcast_ref::<MyRegexUdf>() {
            let proto = MyRegexUdfNode {
                pattern: udf.pattern.clone(),
            };
            proto
                .encode(buf)
                .map_err(|err| internal_datafusion_err!("failed to encode udf: {err}"))?;
        }
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if name == "aggregate_udf" {
            let proto = MyAggregateUdfNode::decode(buf).map_err(|err| {
                internal_datafusion_err!("failed to decode aggregate_udf: {err}")
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
        if let Some(udf) = binding.downcast_ref::<MyAggregateUDF>() {
            let proto = MyAggregateUdfNode {
                result: udf.result.clone(),
            };
            proto.encode(buf).map_err(|err| {
                internal_datafusion_err!("failed to encode udf: {err:?}")
            })?;
        }
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if name == "custom_udwf" {
            let proto = CustomUDWFNode::decode(buf).map_err(|err| {
                internal_datafusion_err!("failed to decode custom_udwf: {err}")
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
        if let Some(udwf) = binding.downcast_ref::<CustomUDWF>() {
            let proto = CustomUDWFNode {
                payload: udwf.payload.clone(),
            };
            proto.encode(buf).map_err(|err| {
                internal_datafusion_err!("failed to encode udwf: {err:?}")
            })?;
        }
        Ok(())
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        if name == "higher_order_udf" {
            let proto = MyHigherOrderUdfNode::decode(buf).map_err(|err| {
                internal_datafusion_err!("failed to decode higher_order_udf: {err}")
            })?;

            Ok(Arc::new(HigherOrderUDF::new_from_impl(
                MyHigherOrderUDF::new(proto.payload),
            )))
        } else {
            not_impl_err!("unrecognized higher order UDF implementation, cannot decode")
        }
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(hof) = (node.inner().as_ref() as &dyn std::any::Any)
            .downcast_ref::<MyHigherOrderUDF>()
        {
            let proto = MyHigherOrderUdfNode {
                payload: hof.payload.clone(),
            };
            proto.encode(buf).map_err(|err| {
                internal_datafusion_err!("failed to encode hof: {err:?}")
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
        Field::new("f", DataType::Int64, true).into(),
        Arc::new(ConfigOptions::default()),
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
            &[],
            Arc::new(WindowFrame::new(None)),
            None,
        ))],
        filter,
        true,
    )?);

    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new(vec![], vec![], vec![], false),
        vec![aggr_expr],
        vec![None],
        window,
        schema,
    )?);

    let ctx = SessionContext::new();
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(aggregate, &ctx, &UDFExtensionCodec, &proto_converter)?;
    Ok(())
}

#[test]
fn roundtrip_higher_order_udf() -> Result<()> {
    let element_field = Arc::new(Field::new("v", DataType::Int32, true));
    let list_field = Field::new(
        "list_col",
        DataType::List(Arc::clone(&element_field)),
        false,
    );
    let schema = Arc::new(Schema::new(vec![list_field]));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    let hof = Arc::new(HigherOrderUDF::new_from_impl(MyHigherOrderUDF::new(
        "payload".to_string(),
    )));

    let expr = HigherOrderFunctionExpr::try_new_with_schema(
        Arc::clone(&hof),
        vec![
            col("list_col", &schema)?,
            lambda(
                ["v"],
                is_not_null(Arc::new(LambdaVariable::new(1, element_field)))?,
            )?,
        ],
        &schema,
        Arc::new(ConfigOptions::default()),
    )?;

    let project = ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: Arc::new(expr),
            alias: "a".to_string(),
        }],
        input,
    )?;

    let ctx = SessionContext::new();
    ctx.register_higher_order_function(hof);

    roundtrip_test_with_context(Arc::new(project), &ctx)
}

#[test]
fn roundtrip_higher_order_udf_extension_codec() -> Result<()> {
    let element_field = Arc::new(Field::new("v", DataType::Int32, true));
    let list_field = Field::new(
        "list_col",
        DataType::List(Arc::clone(&element_field)),
        false,
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let input = Arc::new(EmptyExec::new(schema.clone()));

    let lambda_body = Arc::new(LambdaVariable::new(1, Arc::clone(&element_field)));
    let lambda_expr = lambda(["v"], lambda_body)?;

    let hof = Arc::new(HigherOrderUDF::new_from_impl(MyHigherOrderUDF::new(
        "payload".to_string(),
    )));
    let hof_expr = Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
        hof,
        vec![col("list_col", &schema)?, lambda_expr],
        &schema,
        Arc::new(ConfigOptions::default()),
    )?);

    let project = ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: hof_expr,
            alias: "out".to_string(),
        }],
        input,
    )?;

    let ctx = SessionContext::new();
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(
        Arc::new(project),
        &ctx,
        &UDFExtensionCodec,
        &proto_converter,
    )?;
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
        &[PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }],
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
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(window, &ctx, &UDFExtensionCodec, &proto_converter)?;
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
        Field::new("f", DataType::Int64, true).into(),
        Arc::new(ConfigOptions::default()),
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
            &[],
            Arc::new(WindowFrame::new(None)),
            None,
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
        PhysicalGroupBy::new(vec![], vec![], vec![], false),
        vec![aggr_expr],
        vec![None],
        window,
        schema,
    )?);

    let ctx = SessionContext::new();
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(aggregate, &ctx, &UDFExtensionCodec, &proto_converter)?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_async_func_exec() -> Result<()> {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestAsyncUDF {
        signature: Signature,
    }

    impl TestAsyncUDF {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Int64], Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for TestAsyncUDF {
        fn name(&self) -> &str {
            "test_async_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            not_impl_err!("Must call from `invoke_async_with_args`")
        }
    }

    #[async_trait::async_trait]
    impl AsyncScalarUDFImpl for TestAsyncUDF {
        async fn invoke_async_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            Ok(args.args[0].clone())
        }
    }

    let ctx = SessionContext::new();
    let async_udf = AsyncScalarUDF::new(Arc::new(TestAsyncUDF::new()));
    ctx.register_udf(async_udf.into_scalar_udf());

    let physical_plan = ctx
        .sql("select test_async_udf(1)")
        .await?
        .create_physical_plan()
        .await?;

    roundtrip_test_with_context(physical_plan, &ctx)?;

    Ok(())
}
