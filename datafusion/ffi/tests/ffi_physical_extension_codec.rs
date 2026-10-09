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

#[cfg(feature = "integration-tests")]
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

    /// The decode context's active scalar-subquery results scope must reach
    /// a `ScalarSubqueryExpr` decoded by the real, production
    /// `PhysicalExtensionCodec::try_decode_with_ctx` entry point when the
    /// codec is foreign *for real*.
    ///
    /// The codec here is built inside a separately `dlopen`'d copy of this
    /// crate's cdylib (see [`get_scalar_subquery_expr_exec_codec`]), so
    /// converting it to `Arc<dyn PhysicalExtensionCodec>` takes the real
    /// `ForeignPhysicalExtensionCodec` branch, and calling
    /// `try_decode_with_ctx` on it dispatches through the same
    /// `try_decode_with_ctx_fn_wrapper` any foreign codec goes through in
    /// production - not a bespoke test-only entry point. That makes
    /// `ForeignPhysicalExtensionCodec::try_decode_with_ctx` wrap this test's
    /// live `ScalarSubqueryResults` in a genuinely foreign
    /// `FFI_ScalarSubqueryResults`, so the decoded expression reads through
    /// `ForeignScalarSubqueryResultsBackend`.
    ///
    /// The decoded plan comes back as a `ForeignExecutionPlan`, opaque to
    /// this side - there is no generic `FFI_PhysicalExpr` wrapper to pull the
    /// embedded `ScalarSubqueryExpr` back out directly. Executing it instead
    /// (through the real `FFI_ExecutionPlan`/`FFI_RecordBatchStream`
    /// machinery every `ExecutionPlan` already has) carries the value back
    /// out as an ordinary `RecordBatch`, which is where the host-value
    /// assertion happens.
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

        // The codec itself is produced inside a genuinely separate copy of
        // this crate's cdylib, loaded through `libloading`.
        let foreign_ffi_codec =
            get_scalar_subquery_expr_exec_codec(ffi_task_ctx_provider)?;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&foreign_ffi_codec).into();
        assert!(
            (Arc::clone(&foreign_codec) as Arc<dyn Any>)
                .downcast_ref::<ForeignPhysicalExtensionCodec>()
                .is_some(),
            "codec built inside the dlopen'd cdylib must be seen as foreign"
        );

        // The production entry point: calling `try_decode_with_ctx` directly
        // on the foreign `Arc<dyn PhysicalExtensionCodec>` is exactly what
        // `PhysicalPlanDecodeContext`-aware decoding does for any extension
        // plan, foreign or not - there is no separate test-only path here.
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

        // Executing the foreign plan crosses back into the cdylib, where the
        // decoded ScalarSubqueryExpr reads through
        // ForeignScalarSubqueryResultsBackend into this test's `results`,
        // and the value crosses back out as a RecordBatch.
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
