use abi_stable::StableAbi;
use datafusion_expr::sort_properties::ExprProperties;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ExprProperties {
}

impl From<ExprProperties> for FFI_ExprProperties {
    fn from(value: ExprProperties) -> Self {
        todo!()
    }
}

impl From<FFI_ExprProperties> for ExprProperties {
    fn from(value: FFI_ExprProperties) -> Self {
        todo!()
    }
}