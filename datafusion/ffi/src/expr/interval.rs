use abi_stable::StableAbi;
use datafusion::logical_expr::interval_arithmetic::Interval;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Interval {
}

impl From<&Interval> for FFI_Interval {
    fn from(value: &Interval) -> Self {
        todo!()
    }
}
impl From<Interval> for FFI_Interval {
    fn from(value: Interval) -> Self {
        FFI_Interval::from(&value)
    }
}

impl From<&FFI_Interval> for Interval {
    fn from(value: &FFI_Interval) -> Self {
        todo!()
    }
}

impl From<FFI_Interval> for Interval {
    fn from(value: FFI_Interval) -> Self {
        Interval::from(&value)
    }
}