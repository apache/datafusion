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
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{ExprFunctionExt, WindowUDF, WindowUDFImpl, col};
    use datafusion::prelude::SessionContext;
    use datafusion_ffi::tests::create_record_batch;
    use datafusion_ffi::tests::utils::get_module;

    #[tokio::test]
    async fn test_rank_udwf() -> Result<()> {
        let module = get_module()?;

        let ffi_rank_func =
            module
                .create_rank_udwf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_scalar_udf"
                        .to_string(),
                ))?();
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
}
