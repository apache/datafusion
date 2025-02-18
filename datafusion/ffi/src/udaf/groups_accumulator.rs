use std::{
    ffi::c_void,
    sync::{Arc, Mutex},
};

use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    error::ArrowError,
    ffi::{from_ffi, to_ffi, FFI_ArrowArray},
};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{EmitTo, GroupsAccumulator},
    scalar::ScalarValue,
};
use prost::Message;

use crate::{
    arrow_wrappers::{WrappedArray, WrappedSchema},
    df_result, rresult, rresult_return,
};

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_GroupsAccumulator {
    pub update_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
        group_indices: RVec<usize>,
        opt_filter: ROption<WrappedArray>,
        total_num_groups: usize,
    ) -> RResult<(), RString>,

    // Evaluate and return a ScalarValues as protobuf bytes
    pub evaluate: unsafe extern "C" fn(
        accumulator: &Self,
        emit_to: FFI_EmitTo,
    ) -> RResult<WrappedArray, RString>,

    pub size: unsafe extern "C" fn(accumulator: &Self) -> usize,

    pub state: unsafe extern "C" fn(
        accumulator: &Self,
        emit_to: FFI_EmitTo,
    ) -> RResult<RVec<WrappedArray>, RString>,

    pub merge_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
        group_indices: RVec<usize>,
        opt_filter: ROption<WrappedArray>,
        total_num_groups: usize,
    ) -> RResult<(), RString>,

    pub convert_to_state: unsafe extern "C" fn(
        accumulator: &Self,
        values: RVec<WrappedArray>,
        opt_filter: ROption<WrappedArray>,
    )
        -> RResult<RVec<WrappedArray>, RString>,

    pub supports_convert_to_state: bool,

    /// Used to create a clone on the provider of the accumulator. This should
    /// only need to be called by the receiver of the accumulator.
    // pub clone: unsafe extern "C" fn(accumulator: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(accumulator: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the accumulator.
    /// A [`ForeignGroupsAccumulator`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_GroupsAccumulator {}
unsafe impl Sync for FFI_GroupsAccumulator {}

pub struct GroupsAccumulatorPrivateData {
    pub accumulator: Box<dyn GroupsAccumulator>,
}

unsafe extern "C" fn update_batch_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    group_indices: RVec<usize>,
    opt_filter: ROption<WrappedArray>,
    total_num_groups: usize,
) -> RResult<(), RString> {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    let values_arrays = values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>();
    let values_arrays = rresult_return!(values_arrays);

    let group_indices: Vec<usize> = group_indices.into_iter().collect();

    let maybe_filter = opt_filter.into_option().and_then(|filter| {
        match ArrayRef::try_from(filter) {
            Ok(v) => Some(v),
            Err(e) => {
                log::warn!("Error during FFI array conversion. Ignoring optional filter in groups accumulator. {}", e);
                None
            }
        }
    }).map(|arr| arr.into_data());
    let opt_filter = maybe_filter.map(|arr| BooleanArray::from(arr));

    rresult!(accum_data.accumulator.update_batch(
        &values_arrays,
        &group_indices,
        opt_filter.as_ref(),
        total_num_groups
    ))
}

unsafe extern "C" fn evaluate_fn_wrapper(
    accumulator: &FFI_GroupsAccumulator,
    emit_to: FFI_EmitTo,
) -> RResult<WrappedArray, RString> {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    let result = rresult_return!(accum_data.accumulator.evaluate(emit_to.into()));

    rresult!(WrappedArray::try_from(&result))
}

unsafe extern "C" fn size_fn_wrapper(accumulator: &FFI_GroupsAccumulator) -> usize {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    accum_data.accumulator.size()
}

unsafe extern "C" fn state_fn_wrapper(
    accumulator: &FFI_GroupsAccumulator,
    emit_to: FFI_EmitTo,
) -> RResult<RVec<WrappedArray>, RString> {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    let state = rresult_return!(accum_data.accumulator.state(emit_to.into()));
    rresult!(state
        .into_iter()
        .map(|arr| WrappedArray::try_from(&arr).map_err(DataFusionError::from))
        .collect::<Result<RVec<_>>>())
}

unsafe extern "C" fn merge_batch_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    group_indices: RVec<usize>,
    opt_filter: ROption<WrappedArray>,
    total_num_groups: usize,
) -> RResult<(), RString> {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);
    let values_arrays = values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>();
    let values_arrays = rresult_return!(values_arrays);

    let group_indices: Vec<usize> = group_indices.into_iter().collect();

    let maybe_filter = opt_filter.into_option().and_then(|filter| {
        match ArrayRef::try_from(filter) {
            Ok(v) => Some(v),
            Err(e) => {
                log::warn!("Error during FFI array conversion. Ignoring optional filter in groups accumulator. {}", e);
                None
            }
        }
    }).map(|arr| arr.into_data());
    let opt_filter = maybe_filter.map(|arr| BooleanArray::from(arr));

    rresult!(accum_data.accumulator.merge_batch(
        &values_arrays,
        &group_indices,
        opt_filter.as_ref(),
        total_num_groups
    ))
}

unsafe extern "C" fn convert_to_state_fn_wrapper(
    accumulator: &FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    opt_filter: ROption<WrappedArray>,
) -> RResult<RVec<WrappedArray>, RString> {
    let private_data = accumulator.private_data as *mut GroupsAccumulatorPrivateData;
    let accum_data = &mut (*private_data);

    let values = rresult_return!(values
        .into_iter()
        .map(|v| ArrayRef::try_from(v).map_err(DataFusionError::from))
        .collect::<Result<Vec<_>>>());

    let opt_filter = opt_filter.into_option().and_then(|filter| {
        match ArrayRef::try_from(filter) {
            Ok(v) => Some(v),
            Err(e) => {
                log::warn!("Error during FFI array conversion. Ignoring optional filter in groups accumulator. {}", e);
                None
            }
        }
    }).map(|arr| arr.into_data()).map(|arr| BooleanArray::from(arr));

    let state = rresult_return!(accum_data
        .accumulator
        .convert_to_state(&values, opt_filter.as_ref()));

    rresult!(state
        .iter()
        .map(|arr| WrappedArray::try_from(arr).map_err(DataFusionError::from))
        .collect::<Result<RVec<_>>>())
}

unsafe extern "C" fn release_fn_wrapper(accumulator: &mut FFI_GroupsAccumulator) {
    let private_data =
        Box::from_raw(accumulator.private_data as *mut GroupsAccumulatorPrivateData);
    drop(private_data);
}

// unsafe extern "C" fn clone_fn_wrapper(accumulator: &FFI_GroupsAccumulator) -> FFI_GroupsAccumulator {
//     let private_data = accumulator.private_data as *const GroupsAccumulatorPrivateData;
//     let accum_data = &(*private_data);

//     Box::new(accum_data.accumulator).into()
// }

// impl Clone for FFI_GroupsAccumulator {
//     fn clone(&self) -> Self {
//         unsafe { (self.clone)(self) }
//     }
// }

impl From<Box<dyn GroupsAccumulator>> for FFI_GroupsAccumulator {
    fn from(accumulator: Box<dyn GroupsAccumulator>) -> Self {
        Self {
            update_batch: update_batch_fn_wrapper,
            evaluate: evaluate_fn_wrapper,
            size: size_fn_wrapper,
            state: state_fn_wrapper,
            merge_batch: merge_batch_fn_wrapper,
            convert_to_state: convert_to_state_fn_wrapper,
            supports_convert_to_state: accumulator.supports_convert_to_state(),

            release: release_fn_wrapper,
            private_data: Box::into_raw(accumulator) as *mut c_void,
        }
    }
}

impl Drop for FFI_GroupsAccumulator {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignGroupsAccumulator is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_GroupsAccumulator.
#[derive(Debug)]
pub struct ForeignGroupsAccumulator {
    accumulator: FFI_GroupsAccumulator,
}

unsafe impl Send for ForeignGroupsAccumulator {}
unsafe impl Sync for ForeignGroupsAccumulator {}

impl From<FFI_GroupsAccumulator> for ForeignGroupsAccumulator {
    fn from(accumulator: FFI_GroupsAccumulator) -> Self {
        Self { accumulator }
    }
}

impl GroupsAccumulator for ForeignGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
            let group_indices = group_indices.iter().cloned().collect();
            let opt_filter = opt_filter
                .map(|bool_array| to_ffi(&bool_array.to_data()))
                .transpose()?
                .map(|(array, schema)| WrappedArray {
                    array,
                    schema: WrappedSchema(schema),
                })
                .into();

            df_result!((self.accumulator.update_batch)(
                &mut self.accumulator,
                values.into(),
                group_indices,
                opt_filter,
                total_num_groups
            ))
        }
    }

    fn size(&self) -> usize {
        unsafe { (self.accumulator.size)(&self.accumulator) }
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        unsafe {
            let return_array = df_result!((self.accumulator.evaluate)(
                &self.accumulator,
                emit_to.into()
            ))?;

            return_array.try_into().map_err(DataFusionError::from)
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        unsafe {
            let returned_arrays =
                df_result!((self.accumulator.state)(&self.accumulator, emit_to.into()))?;

            returned_arrays
                .into_iter()
                .map(|wrapped_array| {
                    wrapped_array.try_into().map_err(DataFusionError::from)
                })
                .collect::<Result<Vec<_>>>()
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
            let group_indices = group_indices.iter().cloned().collect();
            let opt_filter = opt_filter
                .map(|bool_array| to_ffi(&bool_array.to_data()))
                .transpose()?
                .map(|(array, schema)| WrappedArray {
                    array,
                    schema: WrappedSchema(schema),
                })
                .into();

            df_result!((self.accumulator.merge_batch)(
                &mut self.accumulator,
                values.into(),
                group_indices,
                opt_filter,
                total_num_groups
            ))
        }
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<RVec<_>, ArrowError>>()?;

            let opt_filter = opt_filter
                .map(|bool_array| to_ffi(&bool_array.to_data()))
                .transpose()?
                .map(|(array, schema)| WrappedArray {
                    array,
                    schema: WrappedSchema(schema),
                })
                .into();

            let returned_array = df_result!((self.accumulator.convert_to_state)(
                &self.accumulator,
                values,
                opt_filter
            ))?;

            returned_array
                .into_iter()
                .map(|arr| arr.try_into().map_err(DataFusionError::from))
                .collect()
        }
    }

    fn supports_convert_to_state(&self) -> bool {
        self.accumulator.supports_convert_to_state
    }
}

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_EmitTo {
    All,
    First(usize),
}

impl From<EmitTo> for FFI_EmitTo {
    fn from(value: EmitTo) -> Self {
        match value {
            EmitTo::All => Self::All,
            EmitTo::First(v) => Self::First(v),
        }
    }
}

impl From<FFI_EmitTo> for EmitTo {
    fn from(value: FFI_EmitTo) -> Self {
        match value {
            FFI_EmitTo::All => Self::All,
            FFI_EmitTo::First(v) => Self::First(v),
        }
    }
}
