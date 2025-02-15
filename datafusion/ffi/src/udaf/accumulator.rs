use std::{
    ffi::c_void,
    sync::{Arc, Mutex},
};

use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use arrow::{array::ArrayRef, error::ArrowError};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::Accumulator,
    scalar::ScalarValue,
};
use prost::Message;

use crate::{
    arrow_wrappers::WrappedArray,
    df_result, rresult, rresult_return,
};

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Accumulator {
    pub update_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
    ) -> RResult<(), RString>,

    // Evaluate and return a ScalarValues as protobuf bytes
    pub evaluate: unsafe extern "C" fn(accumulator: &Self) -> RResult<RVec<u8>, RString>,

    pub size: unsafe extern "C" fn(accumulator: &Self) -> usize,

    pub state:
        unsafe extern "C" fn(accumulator: &Self) -> RResult<RVec<RVec<u8>>, RString>,

    pub merge_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        states: RVec<WrappedArray>,
    ) -> RResult<(), RString>,

    pub retract_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
    ) -> RResult<(), RString>,

    pub supports_retract_batch: unsafe extern "C" fn(accumulator: &Self) -> bool,

    /// Used to create a clone on the provider of the accumulator. This should
    /// only need to be called by the receiver of the accumulator.
    pub clone: unsafe extern "C" fn(accumulator: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(accumulator: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the accumulator.
    /// A [`ForeignAccumulator`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_Accumulator {}
unsafe impl Sync for FFI_Accumulator {}

pub struct AccumulatorPrivateData {
    pub accumulator: Arc<Mutex<dyn Accumulator>>,
}

unsafe extern "C" fn update_batch_fn_wrapper(
    accumulator: &mut FFI_Accumulator,
    values: RVec<WrappedArray>,
) -> RResult<(), RString> {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    let values_arrays = values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>();
    let values_arrays = rresult_return!(values_arrays);

    let mut accumulator = rresult_return!(accum_data
        .accumulator
        .lock()
        .map_err(|e| DataFusionError::Execution(e.to_string())));

    rresult!(accumulator.update_batch(&values_arrays))
}

unsafe extern "C" fn evaluate_fn_wrapper(
    accumulator: &FFI_Accumulator,
) -> RResult<RVec<u8>, RString> {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    let mut accumulator_internal = rresult_return!(accum_data
        .accumulator
        .lock()
        .map_err(|_| DataFusionError::Execution(
            "Unable to aquire lock on FFI Accumulator".to_string()
        )));

    let scalar_result = rresult_return!(accumulator_internal.evaluate());
    let proto_result: datafusion_proto::protobuf::ScalarValue =
        rresult_return!((&scalar_result).try_into());

    RResult::ROk(proto_result.encode_to_vec().into())
}

unsafe extern "C" fn size_fn_wrapper(accumulator: &FFI_Accumulator) -> usize {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    accum_data
        .accumulator
        .lock()
        .map(|accum| accum.size())
        .unwrap_or_default()
}

unsafe extern "C" fn state_fn_wrapper(
    accumulator: &FFI_Accumulator,
) -> RResult<RVec<RVec<u8>>, RString> {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    let mut accumulator_internal = rresult_return!(accum_data
        .accumulator
        .lock()
        .map_err(|_| DataFusionError::Execution(
            "Unable to aquire lock on FFI Accumulator".to_string()
        )));

    let state = rresult_return!(accumulator_internal.state());
    let state = state
        .into_iter()
        .map(|state_val| {
            datafusion_proto::protobuf::ScalarValue::try_from(&state_val)
                .map_err(DataFusionError::from)
                .map(|v| RVec::from(v.encode_to_vec()))
        })
        .collect::<Result<Vec<_>>>()
        .map(|state_vec| state_vec.into());

    rresult!(state)
}

unsafe extern "C" fn merge_batch_fn_wrapper(accumulator: &mut FFI_Accumulator,
    states: RVec<WrappedArray>,
) -> RResult<(), RString> {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    let mut accumulator_internal = rresult_return!(accum_data
        .accumulator
        .lock()
        .map_err(|_| DataFusionError::Execution(
            "Unable to aquire lock on FFI Accumulator".to_string()
        )));

    let states = rresult_return!(states.into_iter()
        .map(|state| ArrayRef::try_from(state).map_err(DataFusionError::from))
        .collect::<Result<Vec<_>>>());

    rresult!(accumulator_internal.merge_batch(&states))
}

unsafe extern "C" fn retract_batch_fn_wrapper(accumulator: &mut FFI_Accumulator,
    values: RVec<WrappedArray>,
) -> RResult<(), RString> {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    let mut accumulator_internal = rresult_return!(accum_data
        .accumulator
        .lock()
        .map_err(|_| DataFusionError::Execution(
            "Unable to aquire lock on FFI Accumulator".to_string()
        )));

    let values = rresult_return!(values.into_iter()
        .map(|state| ArrayRef::try_from(state).map_err(DataFusionError::from))
        .collect::<Result<Vec<_>>>());

    rresult!(accumulator_internal.retract_batch(&values))
}

unsafe extern "C" fn supports_retract_batch_fn_wrapper(accumulator: &FFI_Accumulator,
) -> bool {
    let private_data = accumulator.private_data as *mut AccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    accum_data
        .accumulator
        .lock()
        .map(|accum| accum.supports_retract_batch())
        .unwrap_or(false)
}

unsafe extern "C" fn release_fn_wrapper(accumulator: &mut FFI_Accumulator) {
    let private_data =
        Box::from_raw(accumulator.private_data as *mut AccumulatorPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(accumulator: &FFI_Accumulator) -> FFI_Accumulator {
    let private_data = accumulator.private_data as *const AccumulatorPrivateData;
    let accum_data = &(*private_data);

    Arc::clone(&accum_data.accumulator).into()
}

impl Clone for FFI_Accumulator {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<Mutex<dyn Accumulator>>> for FFI_Accumulator {
    fn from(accumulator: Arc<Mutex<dyn Accumulator>>) -> Self {
        let private_data = Box::new(AccumulatorPrivateData { accumulator });

        Self {
            update_batch: update_batch_fn_wrapper,
            evaluate: evaluate_fn_wrapper,
            size: size_fn_wrapper,
            state: state_fn_wrapper,
            merge_batch: merge_batch_fn_wrapper,
            retract_batch: retract_batch_fn_wrapper,
            supports_retract_batch: supports_retract_batch_fn_wrapper,

            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl Drop for FFI_Accumulator {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignAccumulator is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_Accumulator.
#[derive(Debug)]
pub struct ForeignAccumulator {
    accumulator: FFI_Accumulator,
}

unsafe impl Send for ForeignAccumulator {}
unsafe impl Sync for ForeignAccumulator {}

impl From<&FFI_Accumulator> for ForeignAccumulator {
    fn from(accumulator: &FFI_Accumulator) -> Self {
        Self {
            accumulator: accumulator.clone(),
        }
    }
}

impl Accumulator for ForeignAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
            df_result!((self.accumulator.update_batch)(
                &mut self.accumulator,
                values.into()
            ))
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        unsafe {
            let scalar_bytes =
                df_result!((self.accumulator.evaluate)(&self.accumulator))?;

            let proto_scalar =
                datafusion_proto::protobuf::ScalarValue::decode(scalar_bytes.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            ScalarValue::try_from(&proto_scalar).map_err(DataFusionError::from)
        }
    }

    fn size(&self) -> usize {
        unsafe { (self.accumulator.size)(&self.accumulator) }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        unsafe {
            let state_protos = df_result!((self.accumulator.state)(&self.accumulator))?;

            state_protos
                .into_iter()
                .map(|proto_bytes| {
                    datafusion_proto::protobuf::ScalarValue::decode(proto_bytes.as_ref())
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                        .and_then(|proto_value| {
                            ScalarValue::try_from(&proto_value)
                                .map_err(DataFusionError::from)
                        })
                })
                .collect::<Result<Vec<_>>>()
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        unsafe {
            let states = states
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
            df_result!((self.accumulator.merge_batch)(
                &mut self.accumulator,
                states.into()
            ))
        }
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
            df_result!((self.accumulator.retract_batch)(
                &mut self.accumulator,
                values.into()
            ))
        }
    }

    fn supports_retract_batch(&self) -> bool {
        unsafe { (self.accumulator.supports_retract_batch)(&self.accumulator) }
    }
}
