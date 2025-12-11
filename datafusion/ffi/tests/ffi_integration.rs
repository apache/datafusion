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
    use datafusion::error::{DataFusionError, Result};
    use datafusion::prelude::SessionContext;
    use datafusion_ffi::table_provider::ForeignTableProvider;
    use datafusion_ffi::tests::create_record_batch;
    use datafusion_ffi::tests::utils::get_module;
    use std::sync::Arc;

    /// It is important that this test is in the `tests` directory and not in the
    /// library directory so we can verify we are building a dynamic library and
    /// testing it via a different executable.
    async fn test_table_provider(synchronous: bool) -> Result<()> {
        let table_provider_module = get_module()?;

        // By calling the code below, the table provided will be created within
        // the module's code.
        let ffi_table_provider = table_provider_module.create_table().ok_or(
            DataFusionError::NotImplemented(
                "External table provider failed to implement create_table".to_string(),
            ),
        )?(synchronous);

        // In order to access the table provider within this executable, we need to
        // turn it into a `ForeignTableProvider`.
        let foreign_table_provider: ForeignTableProvider = (&ffi_table_provider).into();

        let ctx = SessionContext::new();

        // Display the data to show the full cycle works.
        ctx.register_table("external_table", Arc::new(foreign_table_provider))?;
        let df = ctx.table("external_table").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], create_record_batch(1, 5));
        assert_eq!(results[1], create_record_batch(6, 1));
        assert_eq!(results[2], create_record_batch(7, 5));

        Ok(())
    }

    #[tokio::test]
    async fn async_test_table_provider() -> Result<()> {
        test_table_provider(false).await
    }

    #[tokio::test]
    async fn sync_test_table_provider() -> Result<()> {
        test_table_provider(true).await
    }
}
