use crate::arrow_wrappers::WrappedArray;
use crate::expr::util::{rvec_u8_to_scalar_value, scalar_value_to_rvec_u8};
use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_ColumnarValue {
    Array(WrappedArray),
    Scalar(RVec<u8>),
}

impl TryFrom<ColumnarValue> for FFI_ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnarValue::Array(v) => {
                FFI_ColumnarValue::Array(WrappedArray::try_from(&v)?)
            }
            ColumnarValue::Scalar(v) => {
                FFI_ColumnarValue::Scalar(scalar_value_to_rvec_u8(&v)?)
            }
        })
    }
}

impl TryFrom<FFI_ColumnarValue> for ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: FFI_ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FFI_ColumnarValue::Array(v) => ColumnarValue::Array(v.try_into()?),
            FFI_ColumnarValue::Scalar(v) => {
                ColumnarValue::Scalar(rvec_u8_to_scalar_value(&v)?)
            }
        })
    }
}
