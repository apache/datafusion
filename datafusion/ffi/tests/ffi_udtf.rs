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

mod utils;

/// Add an additional module here for convenience to scope this to only
/// when the feature integration-tests is built
#[cfg(feature = "integration-tests")]
mod tests {

    use std::sync::Arc;

    use arrow::array::{ArrayRef, create_array};
    use datafusion::catalog::TableFunctionImpl;
    use datafusion::error::{DataFusionError, Result};

    use datafusion_ffi::tests::utils::get_module;

    /// This test validates that we can load an external module and use a scalar
    /// udf defined in it via the foreign function interface. In this case we are
    /// using the abs() function as our scalar UDF.
    #[tokio::test]
    async fn test_user_defined_table_function() -> Result<()> {
        let module = get_module()?;
        let (ctx, codec) = super::utils::ctx_and_codec();

        let ffi_table_func = module
            .create_table_function()
            .ok_or(DataFusionError::NotImplemented(
            "External table function provider failed to implement create_table_function"
                .to_string(),
        ))?(codec);
        let foreign_table_func: Arc<dyn TableFunctionImpl> = ffi_table_func.into();

        ctx.register_udtf("my_range", foreign_table_func);

        let result = ctx
            .sql("SELECT * FROM my_range(5)")
            .await?
            .collect()
            .await?;
        let expected = create_array!(Int64, [0, 1, 2, 3, 4]) as ArrayRef;

        assert!(result.len() == 1);
        assert!(result[0].column(0) == &expected);

        Ok(())
    }
}
