// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::array::{AsArray, RecordBatch};
    use arrow::datatypes::{DataType, Int64Type, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_execution::TaskContextProvider;
    use datafusion_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion_ffi::execution::FFI_TaskContextProvider;
    use datafusion_ffi::execution_plan::ForeignExecutionPlan;
    use datafusion_ffi::proto::physical_extension_codec::ForeignPhysicalExtensionCodec;
    use datafusion_ffi::proto::physical_extension_codec::fixtures::{
        ScalarSubqueryExprExec, ScalarSubqueryExprExecCodec,
    };
    use datafusion_ffi::tests::utils::get_scalar_subquery_expr_exec_codec;
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    };

    /// A plan decoded by a genuinely foreign codec must read the scalar
    /// subquery results the host populated.
    ///
    /// The codec comes from a separately `dlopen`'d copy of this crate, so
    /// the decode takes the same path a real foreign codec takes rather than
    /// a test-only one.
    #[tokio::test]
    async fn test_ffi_physical_extension_codec_scalar_subquery_cross_library()
    -> Result<(), DataFusionError> {
        let results = ScalarSubqueryResults::new(1);
        results.set(SubqueryIndex::new(0), ScalarValue::Int64(Some(42)))?;

        let sq_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarSubqueryExpr::new(
            DataType::Int64,
            true,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let extension_plan: Arc<dyn ExecutionPlan> =
            Arc::new(ScalarSubqueryExprExec::new(
                sq_expr,
                Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
            ));

        let mut bytes = Vec::new();
        ScalarSubqueryExprExecCodec.try_encode(
            Arc::clone(&extension_plan),
            &mut bytes,
            &DefaultPhysicalProtoConverter {},
        )?;

        let ctx = Arc::new(SessionContext::default());
        let host_task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let ffi_task_ctx_provider =
            FFI_TaskContextProvider::from(&host_task_ctx_provider);

        let foreign_ffi_codec =
            get_scalar_subquery_expr_exec_codec(ffi_task_ctx_provider)?;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&foreign_ffi_codec).into();
        assert!(
            (Arc::clone(&foreign_codec) as Arc<dyn Any>)
                .downcast_ref::<ForeignPhysicalExtensionCodec>()
                .is_some(),
            "codec built inside the dlopen'd cdylib must be seen as foreign"
        );

        let task_ctx = ctx.task_ctx();
        let decode_ctx =
            PhysicalPlanDecodeContext::new(task_ctx.as_ref(), foreign_codec.as_ref())
                .with_scalar_subquery_results(results.clone());
        // `ScalarSubqueryExprExecCodec::try_decode_with_ctx` only uses this
        // to read a schema; `ScalarSubqueryExpr` carries no column
        // references of its own.
        let dummy_input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let decoded = foreign_codec.try_decode_with_ctx(
            bytes.as_ref(),
            &[dummy_input],
            &decode_ctx,
            &DefaultPhysicalProtoConverter {},
        )?;
        assert!(
            decoded.downcast_ref::<ScalarSubqueryExprExec>().is_none(),
            "a plan decoded by a genuinely foreign codec must stay opaque, not downcast locally"
        );
        assert!(
            decoded.downcast_ref::<ForeignExecutionPlan>().is_some(),
            "expected the decoded plan to be a ForeignExecutionPlan"
        );

        let stream = decoded.execute(0, task_ctx)?;
        let batches: Vec<RecordBatch> =
            datafusion_physical_plan::common::collect(stream).await?;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let value = batch.column(0).as_primitive::<Int64Type>().value(0);
        assert_eq!(value, 42);

        Ok(())
    }
}
