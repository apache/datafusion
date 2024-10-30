use abi_stable::{
    declare_root_module_statics,
    library::{LibraryError, RootModule},
    package_version_strings,
    sabi_types::VersionStrings,
    StableAbi,
};
use datafusion_ffi::table_provider::FFI_TableProvider;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = TableProviderModuleRef)))]
pub struct TableProviderModule {
    /// Constructs the table provider
    pub create_table: extern "C" fn() -> FFI_TableProvider,
}

impl RootModule for TableProviderModuleRef {
    declare_root_module_statics! {TableProviderModuleRef}
    const BASE_NAME: &'static str = "ffi_example_table_provider";
    const NAME: &'static str = "ffi_example_table_provider";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    fn initialization(self) -> Result<Self, LibraryError> {
        Ok(self)
    }
}
