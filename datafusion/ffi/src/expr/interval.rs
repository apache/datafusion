use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::ArrowError;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion_common::DataFusionError;
use prost::Message;
use crate::expr::util::{rvec_u8_to_scalar_value, scalar_value_to_rvec_u8};

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
        let upper = scalar_value_to_rvec_u8(value.upper())?;
        let lower = scalar_value_to_rvec_u8(value.lower())?;

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
        let upper = rvec_u8_to_scalar_value(&value.upper)?;
        let lower = rvec_u8_to_scalar_value(&value.lower)?;

        Interval::try_new(lower, upper)
    }
}

impl TryFrom<FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: FFI_Interval) -> Result<Self, Self::Error> {
        Interval::try_from(&value)
    }
}
