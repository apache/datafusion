use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::ArrowError;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion_common::DataFusionError;
use prost::Message;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Interval {
    lower: RVec<u8>,
    upper: RVec<u8>,
}

impl TryFrom<&Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: &Interval) -> Result<Self, Self::Error> {
        let upper: datafusion_proto_common::ScalarValue = value.upper().try_into()?;
        let lower: datafusion_proto_common::ScalarValue = value.lower().try_into()?;

        let upper = upper.encode_to_vec().into();
        let lower = lower.encode_to_vec().into();

        Ok(FFI_Interval { upper, lower })
    }
}
impl TryFrom<Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: Interval) -> Result<Self, Self::Error> {
        FFI_Interval::try_from(&value)
    }
}

impl TryFrom<&FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: &FFI_Interval) -> Result<Self, Self::Error> {
        let upper = datafusion_proto_common::ScalarValue::decode(value.upper.as_ref())
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let lower = datafusion_proto_common::ScalarValue::decode(value.lower.as_ref())
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;

        let upper = (&upper).try_into()?;
        let lower = (&lower).try_into()?;

        Interval::try_new(lower, upper)
    }
}

impl TryFrom<FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: FFI_Interval) -> Result<Self, Self::Error> {
        Interval::try_from(&value)
    }
}
