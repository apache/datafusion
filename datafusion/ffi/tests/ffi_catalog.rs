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
    use datafusion::catalog::{CatalogProvider, CatalogProviderList};
    use datafusion::prelude::SessionContext;
    use datafusion_common::DataFusionError;
    use datafusion_ffi::tests::utils::get_module;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() -> datafusion_common::Result<()> {
        let module = get_module()?;

        let ffi_catalog =
            module
                .create_catalog()
                .ok_or(DataFusionError::NotImplemented(
                    "External catalog provider failed to implement create_catalog"
                        .to_string(),
                ))?();
        let foreign_catalog: Arc<dyn CatalogProvider + Send> = (&ffi_catalog).into();

        let ctx = SessionContext::default();
        let _ = ctx.register_catalog("fruit", foreign_catalog);

        let df = ctx.table("fruit.apple.purchases").await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 2);
        let num_rows: usize = results.into_iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(num_rows, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_list() -> datafusion_common::Result<()> {
        let module = get_module()?;

        let ffi_catalog_list =
            module
                .create_catalog_list()
                .ok_or(DataFusionError::NotImplemented(
                    "External catalog provider failed to implement create_catalog_list"
                        .to_string(),
                ))?();
        let foreign_catalog_list: Arc<dyn CatalogProviderList + Send> =
            (&ffi_catalog_list).into();

        let ctx = SessionContext::default();
        ctx.register_catalog_list(foreign_catalog_list);

        let df = ctx.table("blue.apple.purchases").await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 2);
        let num_rows: usize = results.into_iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(num_rows, 5);

        Ok(())
    }
}
