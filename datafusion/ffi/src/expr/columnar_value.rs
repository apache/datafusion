use abi_stable::StableAbi;
use datafusion_expr::ColumnarValue;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ColumnarValue {
}

impl From<ColumnarValue> for FFI_ColumnarValue {
    fn from(value: ColumnarValue) -> Self {
        todo!()
    }
}

impl From<FFI_ColumnarValue> for ColumnarValue {
    fn from(value: FFI_ColumnarValue) -> Self {
        todo!()
    }
}