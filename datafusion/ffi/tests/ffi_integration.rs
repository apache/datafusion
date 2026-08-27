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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use datafusion::catalog::{TableProvider, TableProviderFactory};
    use datafusion::error::Result;
    use datafusion::prelude::{col, lit};
    use datafusion_common::TableReference;
    use datafusion_common::ToDFSchema;
    use datafusion_expr::CreateExternalTable;
    use datafusion_ffi::tests::create_record_batch;
    use datafusion_ffi::tests::utils::get_module;

    /// It is important that this test is in the `tests` directory and not in the
    /// library directory so we can verify we are building a dynamic library and
    /// testing it via a different executable.
    async fn test_table_provider(synchronous: bool) -> Result<()> {
        let table_provider_module = get_module()?;
        let (ctx, codec) = super::utils::ctx_and_codec();

        // By calling the code below, the table provided will be created within
        // the module's code.
        let ffi_table_provider = (table_provider_module.create_table)(synchronous, codec);

        // In order to access the table provider within this executable, we need to
        // turn it into a `TableProvider`.
        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_table_provider).into();

        // Display the data to show the full cycle works.
        ctx.register_table("external_table", foreign_table_provider)?;
        let df = ctx.table("external_table").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 3);
        assert!(results.contains(&create_record_batch(1, 5)));
        assert!(results.contains(&create_record_batch(6, 1)));
        assert!(results.contains(&create_record_batch(7, 5)));

        if !synchronous {
            assert!(
                ctx.state()
                    .catalog_list()
                    .catalog("ffi_registered")
                    .is_some()
            );
        }

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

    #[test]
    fn test_ffi_table_provider_statistics_cross_library() -> Result<()> {
        let module = get_module()?;
        let (_, codec) = super::utils::ctx_and_codec();

        let expected = datafusion_ffi::tests::make_test_statistics();

        let ffi_provider = (module.create_table_with_statistics)(codec);
        let foreign: Arc<dyn TableProvider> = (&ffi_provider).into();

        assert_eq!(foreign.statistics().as_ref(), Some(&expected));

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_table_provider_dml_cross_library() -> Result<()> {
        let module = get_module()?;
        let (ctx, codec) = super::utils::ctx_and_codec();

        let ffi_provider = (module.create_table_with_statistics)(codec);
        let foreign: Arc<dyn TableProvider> = (&ffi_provider).into();
        let state = ctx.state();

        let delete_plan = foreign
            .delete_from(
                &state,
                vec![col("a").gt(lit(10_i32)), col("b").lt(lit(2.5_f64))],
            )
            .await?;
        assert_eq!(delete_plan.schema().field(0).name(), "count");
        let delete_all_plan = foreign.delete_from(&state, vec![]).await?;
        assert_eq!(delete_all_plan.schema().field(0).name(), "count");

        let update_assignments = vec![
            ("b".to_string(), lit(42_f64)),
            ("a".to_string(), lit(7_i32)),
        ];
        let update_plan = foreign
            .update(
                &state,
                update_assignments.clone(),
                vec![col("a").eq(lit(7_i32)), col("b").gt(lit(1.5_f64))],
            )
            .await?;
        assert_eq!(update_plan.schema().field(0).name(), "count");

        let update_all_plan = foreign.update(&state, update_assignments, vec![]).await?;
        assert_eq!(update_all_plan.schema().field(0).name(), "count");

        let truncate_plan = foreign.truncate(&state).await?;
        assert_eq!(truncate_plan.schema().field(0).name(), "count");

        Ok(())
    }

    /// A table provider that builds its own object store, registers it on the
    /// session during planning, and reads it back during execution.
    ///
    /// Planning happens in the loaded module and execution is driven from this
    /// executable, so the session's `RuntimeEnv` has to cross the FFI boundary
    /// intact for the store to be found at execution time.
    ///
    /// The same scan reports the memory pool limit it observes, checking that
    /// `datafusion.execution.memory_limit` reaches a foreign plan.
    #[tokio::test]
    async fn test_object_store_crosses_ffi_boundary() -> Result<()> {
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_ffi::tests::object_store_provider::{
            EXPECTED_VALUES, UNLIMITED_MEMORY,
        };
        use std::sync::Arc;

        const MEMORY_LIMIT: usize = 64 * 1024 * 1024;

        let module = get_module()?;

        // Build a context with a distinctive memory limit so the value observed
        // inside the module identifies which pool it actually reached.
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(MEMORY_LIMIT)))
            .build_arc()?;
        let ctx = Arc::new(SessionContext::new_with_config_rt(
            SessionConfig::new(),
            runtime_env,
        ));
        let codec = super::utils::codec_for(&ctx);

        let ffi_provider = (module.create_object_store_table)(codec);
        let foreign: Arc<dyn TableProvider> = (&ffi_provider).into();

        ctx.register_table("remote_table", foreign)?;
        let batches = ctx.table("remote_table").await?.collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows,
            EXPECTED_VALUES.len(),
            "scan should read every value back out of the registered store"
        );

        let values: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .expect("column a should be Int32")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, EXPECTED_VALUES.to_vec());

        let observed_limit = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("column mem_limit should be UInt64")
            .value(0);
        assert_ne!(
            observed_limit, UNLIMITED_MEMORY,
            "the foreign plan saw an unbounded pool, so the host's memory limit \
             did not cross the boundary"
        );
        assert_eq!(
            observed_limit, MEMORY_LIMIT as u64,
            "the foreign plan should see the host's configured memory limit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_provider_factory() -> Result<()> {
        let table_provider_module = get_module()?;
        let (ctx, codec) = super::utils::ctx_and_codec();

        let ffi_table_provider_factory =
            (table_provider_module.create_table_factory)(codec);

        let foreign_table_provider_factory: Arc<dyn TableProviderFactory> =
            (&ffi_table_provider_factory).into();

        let cmd = CreateExternalTable {
            schema: Schema::empty().to_dfschema_ref()?,
            name: TableReference::bare("cloned_test"),
            locations: vec!["test".to_string()],
            file_type: "test".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Default::default(),
            column_defaults: HashMap::new(),
        };

        let provider = foreign_table_provider_factory
            .create(&ctx.state(), &cmd)
            .await?;
        assert_eq!(provider.schema().fields().len(), 2);

        Ok(())
    }
}
