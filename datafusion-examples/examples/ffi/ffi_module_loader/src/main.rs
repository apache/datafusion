use std::sync::Arc;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};

use abi_stable::library::{development_utils::compute_library_path, RootModule};
use datafusion_ffi::table_provider::ForeignTableProvider;
use ffi_module_interface::TableProviderModuleRef;

#[tokio::main]
async fn main() -> Result<()> {
    let target: &std::path::Path = "../../../../target/".as_ref();
    let library_path = compute_library_path::<TableProviderModuleRef>(target).unwrap();

    let table_provider_module =
        TableProviderModuleRef::load_from_directory(&library_path)
            .unwrap_or_else(|e| panic!("{}", e));

    let ffi_table_provider =
        table_provider_module
            .create_table()
            .ok_or(DataFusionError::NotImplemented(
                "External table provider failed to implement create_table".to_string(),
            ))?();

    let foreign_table_provider: ForeignTableProvider = (&ffi_table_provider).into();

    let ctx = SessionContext::new();

    ctx.register_table("external_table", Arc::new(foreign_table_provider))?;

    let df = ctx.table("external_table").await?;

    df.show().await?;

    Ok(())
}
