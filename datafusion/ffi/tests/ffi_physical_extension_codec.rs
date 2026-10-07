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
    use std::sync::Arc;

    use arrow_schema::{DataType, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_execution::TaskContextProvider;
    use datafusion_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion_ffi::execution::FFI_TaskContextProvider;
    use datafusion_ffi::proto::physical_extension_codec::fixtures::{
        ScalarSubqueryExprExec, ScalarSubqueryExprExecCodec,
    };
    use datafusion_ffi::proto::scalar_subquery_results::FFI_ScalarSubqueryResults;
    use datafusion_ffi::tests::utils::decode_scalar_subquery_expr_exec_result;
    use datafusion_ffi::util::FFI_Option;
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    };

    /// The scalar-subquery results scope a `PhysicalExtensionCodec` receives
    /// through `try_decode_with_ctx` must reach a `ScalarSubqueryExpr`
    /// decoded by a codec that is foreign *for real*: the decode in this
    /// test runs inside a separately `dlopen`'d copy of the
    /// `datafusion-ffi` cdylib (see
    /// [`decode_scalar_subquery_expr_exec_result`]), a genuinely different
    /// compiled image from this test binary, which links the crate as an
    /// ordinary (statically-linked) rlib dependency instead.
    ///
    /// That image difference means the `FFI_ScalarSubqueryResults` handle
    /// this test builds and passes across is not recognized as local on the
    /// far side, so decoding there must go through
    /// `ForeignScalarSubqueryResultsBackend`, round-tripping every
    /// `get`/`set` back across the boundary - not the local-bypass fast path
    /// the in-process, marker-mocked unit test
    /// `ffi_physical_extension_codec_forced_foreign_scalar_subquery_roundtrip`
    /// (in `datafusion/ffi/src/proto/physical_extension_codec.rs`) takes.
    #[test]
    fn test_ffi_physical_extension_codec_scalar_subquery_cross_library()
    -> Result<(), DataFusionError> {
        let results = ScalarSubqueryResults::new(1);
        let sq_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarSubqueryExpr::new(
            DataType::Int64,
            true,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let extension_plan: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExprExec {
            expr: sq_expr,
            child: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        });

        let mut bytes = Vec::new();
        ScalarSubqueryExprExecCodec.try_encode(
            Arc::clone(&extension_plan),
            &mut bytes,
            &DefaultPhysicalProtoConverter {},
        )?;

        let ctx = Arc::new(SessionContext::default());
        let host_task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;

        // Nothing has been written to `results` yet.
        let read = decode_scalar_subquery_expr_exec_result(
            FFI_TaskContextProvider::from(&host_task_ctx_provider),
            &bytes,
            FFI_Option::Some(FFI_ScalarSubqueryResults::new(results.clone())),
            0,
        )?;
        assert_eq!(read, None);

        // The host writes a value into its own results container. A fresh
        // decode inside the foreign image must observe it - this only
        // happens if the handle genuinely round-tripped through
        // `ForeignScalarSubqueryResultsBackend` into *this* container,
        // rather than a disconnected one private to the foreign side.
        results.set(SubqueryIndex::new(0), ScalarValue::Int64(Some(42)))?;

        let read = decode_scalar_subquery_expr_exec_result(
            FFI_TaskContextProvider::from(&host_task_ctx_provider),
            &bytes,
            FFI_Option::Some(FFI_ScalarSubqueryResults::new(results.clone())),
            0,
        )?;
        assert_eq!(read, Some(42));

        Ok(())
    }
}
