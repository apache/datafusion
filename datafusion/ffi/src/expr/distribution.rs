use abi_stable::StableAbi;
use datafusion_expr::statistics::Distribution;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Distribution {
}

impl From<Distribution> for FFI_Distribution {
    fn from(value: Distribution) -> Self {
        todo!()
    }
}

impl From<FFI_Distribution> for Distribution {
    fn from(value: FFI_Distribution) -> Self {
        todo!()
    }
}