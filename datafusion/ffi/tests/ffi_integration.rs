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
/// when the feature integtation-tests is built
#[cfg(feature = "integration-tests")]
mod tests {

    use abi_stable::library::RootModule;
    use arrow::array::Float64Array;
    use datafusion::common::record_batch;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
    use datafusion::prelude::{col, SessionContext};
    use datafusion_ffi::catalog_provider::ForeignCatalogProvider;
    use datafusion_ffi::table_provider::ForeignTableProvider;
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_ffi::tests::{create_record_batch, ForeignLibraryModuleRef};
    use datafusion_ffi::udaf::ForeignAggregateUDF;
    use datafusion_ffi::udf::ForeignScalarUDF;
    use std::path::Path;
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

    #[tokio::test]
    async fn test_catalog() -> Result<()> {
        let module = get_module()?;

        let ffi_catalog =
            module
                .create_catalog()
                .ok_or(DataFusionError::NotImplemented(
                    "External catalog provider failed to implement create_catalog"
                        .to_string(),
                ))?();
        let foreign_catalog: ForeignCatalogProvider = (&ffi_catalog).into();

        let ctx = SessionContext::default();
        let _ = ctx.register_catalog("fruit", Arc::new(foreign_catalog));

        let df = ctx.table("fruit.apple.purchases").await?;

        let results = df.collect().await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() != 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_udaf() -> Result<()> {
        let module = get_module()?;

        let ffi_avg_func =
            module
                .create_sum_udaf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_udaf".to_string(),
                ))?();
        let foreign_avg_func: ForeignAggregateUDF = (&ffi_avg_func).try_into()?;

        let udaf: AggregateUDF = foreign_avg_func.into();

        let ctx = SessionContext::default();
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 2, 4, 4, 4, 4]),
            ("b", Float64, vec![1.0, 2.0, 2.0, 4.0, 4.0, 4.0, 4.0])
        )
        .unwrap();

        let df = ctx.read_batch(record_batch)?;

        let df = df
            .aggregate(
                vec![col("a")],
                vec![udaf.call(vec![col("b")]).alias("sum_b")],
            )?
            .sort_by(vec![col("a")])?;

        let result = df.collect().await?;

        let expected = record_batch!(
            ("a", Int32, vec![1, 2, 4]),
            ("sum_b", Float64, vec![1.0, 4.0, 16.0])
        )?;

        assert_eq!(result[0], expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_grouping_udaf() -> Result<()> {
        let module = get_module()?;

        let ffi_stddev_func =
            module
                .create_stddev_udaf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_udaf".to_string(),
                ))?();
        let foreign_stddev_func: ForeignAggregateUDF = (&ffi_stddev_func).try_into()?;

        let udaf: AggregateUDF = foreign_stddev_func.into();

        let ctx = SessionContext::default();
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 2, 4, 4, 4, 4]),
            (
                "b",
                Float64,
                vec![
                    1.0,
                    2.0,
                    2.0 + 2.0_f64.sqrt(),
                    4.0,
                    4.0,
                    4.0 + 3.0_f64.sqrt(),
                    4.0 + 3.0_f64.sqrt()
                ]
            )
        )
        .unwrap();

        let df = ctx.read_batch(record_batch)?;

        let df = df
            .aggregate(
                vec![col("a")],
                vec![udaf.call(vec![col("b")]).alias("stddev_b")],
            )?
            .sort_by(vec![col("a")])?;

        let result = df.collect().await?;
        let result = result[0].column_by_name("stddev_b").unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values();

        assert!(result.first().unwrap().is_nan());
        assert!(result.get(1).unwrap() - 1.0 < 0.00001);
        assert!(result.get(2).unwrap() - 1.0 < 0.00001);

        Ok(())
    }
}
