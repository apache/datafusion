// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod tests {
    use std::sync::Arc;

    use datafusion::catalog::{CatalogProvider, CatalogProviderList};
    use datafusion_ffi::tests::utils::get_module;

    #[tokio::test]
    async fn test_catalog() -> datafusion_common::Result<()> {
        let module = get_module()?;
        let (ctx, codec) = crate::utils::ctx_and_codec();

        let ffi_catalog = (module.create_catalog)(codec);
        let foreign_catalog: Arc<dyn CatalogProvider> = (&ffi_catalog).into();

        let _ = ctx.register_catalog("fruit", foreign_catalog);

        let df = ctx.table("fruit.apple.purchases").await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 2);
        let num_rows: usize = results.into_iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(num_rows, 5);

        Ok(())
    }

    /// Exercises the `Result`-returning catalog and schema lookups across the
    /// cdylib boundary.
    #[tokio::test]
    async fn test_catalog_schema_lookups() -> datafusion_common::Result<()> {
        let module = get_module()?;
        let (_ctx, codec) = crate::utils::ctx_and_codec();

        let ffi_catalog = (module.create_catalog)(codec);
        let foreign_catalog: Arc<dyn CatalogProvider> = (&ffi_catalog).into();

        assert_eq!(foreign_catalog.schema_names()?, vec!["apple".to_string()]);
        assert!(foreign_catalog.schema("nope")?.is_none());

        let schema = foreign_catalog
            .schema("apple")?
            .expect("fixture catalog should contain schema apple");
        assert_eq!(schema.table_names()?, vec!["purchases".to_string()]);
        assert!(schema.table_exist("purchases")?);
        assert!(!schema.table_exist("nope")?);

        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_list() -> datafusion_common::Result<()> {
        let module = get_module()?;
        let (ctx, codec) = crate::utils::ctx_and_codec();

        let ffi_catalog_list = (module.create_catalog_list)(codec);
        let foreign_catalog_list: Arc<dyn CatalogProviderList> =
            (&ffi_catalog_list).into();

        ctx.register_catalog_list(foreign_catalog_list);

        let df = ctx.table("blue.apple.purchases").await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 2);
        let num_rows: usize = results.into_iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(num_rows, 5);

        Ok(())
    }
}
