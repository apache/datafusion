// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ffi::c_void;

use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    error::ArrowError,
    ffi::to_ffi,
};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{EmitTo, GroupsAccumulator},
};

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
    let opt_filter = maybe_filter.map(BooleanArray::from);

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
    let opt_filter = maybe_filter.map(BooleanArray::from);

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
    }).map(|arr| arr.into_data()).map(BooleanArray::from);

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
        let supports_convert_to_state = accumulator.supports_convert_to_state();
        let private_data = GroupsAccumulatorPrivateData { accumulator };

        Self {
            update_batch: update_batch_fn_wrapper,
            evaluate: evaluate_fn_wrapper,
            size: size_fn_wrapper,
            state: state_fn_wrapper,
            merge_batch: merge_batch_fn_wrapper,
            convert_to_state: convert_to_state_fn_wrapper,
            supports_convert_to_state,

            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(private_data)) as *mut c_void,
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

#[cfg(test)]
mod tests {
    use arrow::array::{make_array, Array, BooleanArray};
    use datafusion::{
        common::create_array,
        error::Result,
        logical_expr::{EmitTo, GroupsAccumulator},
    };
    use datafusion_functions_aggregate_common::aggregate::groups_accumulator::bool_op::BooleanGroupsAccumulator;

    use super::{FFI_EmitTo, FFI_GroupsAccumulator, ForeignGroupsAccumulator};

    #[test]
    fn test_foreign_avg_accumulator() -> Result<()> {
        let boxed_accum: Box<dyn GroupsAccumulator> =
            Box::new(BooleanGroupsAccumulator::new(|a, b| a && b, true));
        let ffi_accum: FFI_GroupsAccumulator = boxed_accum.into();
        let mut foreign_accum: ForeignGroupsAccumulator = ffi_accum.into();

        // Send in an array to evaluate. We want a mean of 30 and standard deviation of 4.
        let values = create_array!(Boolean, vec![true, true, true, false, true, true]);
        let opt_filter =
            create_array!(Boolean, vec![true, true, true, true, false, false]);
        foreign_accum.update_batch(
            &[values],
            &[0, 0, 1, 1, 2, 2],
            Some(opt_filter.as_ref()),
            3,
        )?;

        let groups_bool = foreign_accum.evaluate(EmitTo::All)?;
        let groups_bool = groups_bool.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(
            groups_bool,
            create_array!(Boolean, vec![Some(true), Some(false), None]).as_ref()
        );

        let state = foreign_accum.state(EmitTo::All)?;
        assert_eq!(state.len(), 1);

        // To verify merging batches works, create a second state to add in
        // This should cause our average to go down to 25.0
        let second_states =
            vec![make_array(create_array!(Boolean, vec![false]).to_data())];

        let opt_filter = create_array!(Boolean, vec![true]);
        foreign_accum.merge_batch(&second_states, &[0], Some(opt_filter.as_ref()), 1)?;
        let groups_bool = foreign_accum.evaluate(EmitTo::All)?;
        assert_eq!(groups_bool.len(), 1);
        assert_eq!(
            groups_bool.as_ref(),
            make_array(create_array!(Boolean, vec![false]).to_data()).as_ref()
        );

        let values = create_array!(Boolean, vec![false]);
        let opt_filter = create_array!(Boolean, vec![true]);
        let groups_bool =
            foreign_accum.convert_to_state(&[values], Some(opt_filter.as_ref()))?;

        assert_eq!(
            groups_bool[0].as_ref(),
            make_array(create_array!(Boolean, vec![false]).to_data()).as_ref()
        );

        Ok(())
    }

    fn test_emit_to_round_trip(value: EmitTo) -> Result<()> {
        let ffi_value: FFI_EmitTo = value.into();
        let round_trip_value: EmitTo = ffi_value.into();

        assert_eq!(value, round_trip_value);
        Ok(())
    }

    /// This test ensures all enum values are properly translated
    #[test]
    fn test_all_emit_to_round_trip() -> Result<()> {
        test_emit_to_round_trip(EmitTo::All)?;
        test_emit_to_round_trip(EmitTo::First(10))?;

        Ok(())
    }
}
