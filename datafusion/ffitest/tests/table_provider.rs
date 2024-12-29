use abi_stable::library::development_utils::compute_library_path;
use abi_stable::library::RootModule;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion_ffi::table_provider::ForeignTableProvider;
#[allow(clippy::single_component_path_imports)]
use datafusion_ffi_test;
use datafusion_ffi_test::TableProviderModuleRef;
use std::path::Path;
use std::sync::Arc;

/// It is important that this test is in the `tests` directory and not in the
/// library directory so we can verify we are building a dynamic library and
/// testing it via a different executable.
async fn test_table_provider(synchronous: bool) -> Result<()> {
    let expected_version = datafusion_ffi::version();

    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_dir = crate_root
        .parent()
        .expect("Failed to find crate parent")
        .parent()
        .expect("Failed to find workspace root")
        .join("target");

    // Find the location of the library. This is specific to the build environment,
    // so you will need to change the approach here based on your use case.
    // let target: &std::path::Path = "../../../../target/".as_ref();
    let library_path =
        compute_library_path::<TableProviderModuleRef>(target_dir.as_path())
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .join("deps");

    // Load the module
    let table_provider_module =
        TableProviderModuleRef::load_from_directory(&library_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

    assert_eq!(
        table_provider_module
            .version()
            .expect("Unable to call version on FFI module")(),
        expected_version
    );

    // By calling the code below, the table provided will be created within
    // the module's code.
    let ffi_table_provider =
        table_provider_module
            .create_table()
            .ok_or(DataFusionError::NotImplemented(
                "External table provider failed to implement create_table".to_string(),
            ))?(synchronous);

    // In order to access the table provider within this executable, we need to
    // turn it into a `ForeignTableProvider`.
    let foreign_table_provider: ForeignTableProvider = (&ffi_table_provider).into();

    let ctx = SessionContext::new();

    // Display the data to show the full cycle works.
    ctx.register_table("external_table", Arc::new(foreign_table_provider))?;
    let df = ctx.table("external_table").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], datafusion_ffi_test::create_record_batch(1, 5));
    assert_eq!(results[1], datafusion_ffi_test::create_record_batch(6, 1));
    assert_eq!(results[2], datafusion_ffi_test::create_record_batch(7, 5));

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
