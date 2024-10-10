use datafusion::{datasource::TableType, logical_expr::TableProviderFilterPushDown};

// TODO Should we just define TableProviderFilterPushDown as repr(C)?
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum FFI_TableProviderFilterPushDown {
    Unsupported,
    Inexact,
    Exact,
}

impl From<FFI_TableProviderFilterPushDown> for TableProviderFilterPushDown {
    fn from(value: FFI_TableProviderFilterPushDown) -> Self {
        match value {
            FFI_TableProviderFilterPushDown::Unsupported => TableProviderFilterPushDown::Unsupported,
            FFI_TableProviderFilterPushDown::Inexact => TableProviderFilterPushDown::Inexact,
            FFI_TableProviderFilterPushDown::Exact => TableProviderFilterPushDown::Exact,
        }
    }
}

impl From<TableProviderFilterPushDown> for FFI_TableProviderFilterPushDown {
    fn from(value: TableProviderFilterPushDown) -> Self {
        match value {
            TableProviderFilterPushDown::Unsupported => FFI_TableProviderFilterPushDown::Unsupported,
            TableProviderFilterPushDown::Inexact => FFI_TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Exact => FFI_TableProviderFilterPushDown::Exact,
        }
    }
}

// TODO Should we just define FFI_TableType as repr(C)?
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_TableType {
    Base,
    View,
    Temporary,
}

impl From<FFI_TableType> for TableType {
    fn from(value: FFI_TableType) -> Self {
        match value {
            FFI_TableType::Base => TableType::Base,
            FFI_TableType::View => TableType::View,
            FFI_TableType::Temporary => TableType::Temporary,
        }
    }
}

impl From<TableType> for FFI_TableType {
    fn from(value: TableType) -> Self {
        match value {
            TableType::Base => FFI_TableType::Base,
            TableType::View => FFI_TableType::View,
            TableType::Temporary => FFI_TableType::Temporary,
        }
    }
}