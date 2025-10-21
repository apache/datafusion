use crate::arrow_wrappers::WrappedArray;
use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::ArrowError;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use prost::Message;

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
                FFI_ColumnarValue::Array({ WrappedArray::try_from(&v)? })
            }
            ColumnarValue::Scalar(v) => FFI_ColumnarValue::Scalar({
                let v: datafusion_proto_common::ScalarValue = (&v).try_into()?;
                v.encode_to_vec().into()
            }),
        })
    }
}

impl TryFrom<FFI_ColumnarValue> for ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: FFI_ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FFI_ColumnarValue::Array(v) => ColumnarValue::Array({ v.try_into()? }),
            FFI_ColumnarValue::Scalar(v) => ColumnarValue::Scalar({
                let v = datafusion_proto_common::ScalarValue::decode(v.as_ref())
                    .map_err(|err| DataFusionError::Execution(err.to_string()))?;
                (&v).try_into()?
            }),
        })
    }
}
