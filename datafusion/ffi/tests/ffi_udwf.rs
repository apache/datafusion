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

/// Add an additional module here for convenience to scope this to only
/// when the feature integration-tests is built
#[cfg(feature = "integration-tests")]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, create_array};
    use datafusion::error::Result;
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{ExprFunctionExt, WindowUDF, WindowUDFImpl, col};
    use datafusion::prelude::SessionContext;
    use datafusion_ffi::tests::create_record_batch;
    use datafusion_ffi::tests::utils::get_module;

    #[tokio::test]
    async fn test_rank_udwf() -> Result<()> {
        let module = get_module()?;

        let ffi_rank_func = (module.create_rank_udwf)();
        let foreign_rank_func: Arc<dyn WindowUDFImpl> = (&ffi_rank_func).into();

        let udwf = WindowUDF::new_from_shared_impl(foreign_rank_func);

        let ctx = SessionContext::default();
        let df = ctx.read_batch(create_record_batch(-5, 5))?;

        let df = df.select(vec![
            col("a"),
            udwf.call(vec![])
                .order_by(vec![Sort::new(col("a"), true, true)])
                .build()
                .unwrap()
                .alias("rank_a"),
        ])?;

        df.clone().show().await?;

        let result = df.collect().await?;
        let expected = create_array!(UInt64, [1, 2, 3, 4, 5]) as ArrayRef;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column(1), &expected);

        Ok(())
    }

    #[test]
    fn test_udwf_documentation() -> Result<()> {
        use datafusion::logical_expr::{DocSection, Documentation};
        use datafusion_ffi::udwf::FFI_WindowUDF;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFWithDoc {
            signature: datafusion::logical_expr::Signature,
            doc: Documentation,
        }

        impl WindowUDFImpl for MockUDWFWithDoc {
            fn name(&self) -> &str {
                "mock_doc"
            }
            fn signature(&self) -> &datafusion::logical_expr::Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: datafusion_expr::function::PartitionEvaluatorArgs,
            ) -> Result<Box<dyn datafusion::logical_expr::PartitionEvaluator>>
            {
                unimplemented!()
            }
            fn field(
                &self,
                _: datafusion_expr::function::WindowUDFFieldArgs,
            ) -> Result<arrow::datatypes::FieldRef> {
                unimplemented!()
            }
            fn documentation(&self) -> Option<&Documentation> {
                Some(&self.doc)
            }
        }

        let doc = Documentation::builder(DocSection::default(), "description", "syntax")
            .build();
        let original_udwf = Arc::new(WindowUDF::from(MockUDWFWithDoc {
            signature: datafusion::logical_expr::Signature::any(
                0,
                datafusion::logical_expr::Volatility::Immutable,
            ),
            doc: doc.clone(),
        }));

        let mut ffi_udwf = FFI_WindowUDF::from(original_udwf);
        extern "C" fn mock_marker() -> usize {
            0xdeadbeef
        }
        ffi_udwf.library_marker_id = mock_marker;

        let foreign_udwf_impl: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        let foreign_udwf = WindowUDF::new_from_shared_impl(foreign_udwf_impl);

        assert_eq!(foreign_udwf.documentation(), Some(&doc));

        Ok(())
    }
}
