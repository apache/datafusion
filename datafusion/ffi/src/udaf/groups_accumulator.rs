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
use std::ops::Deref;
use std::ptr::null_mut;
use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RVec};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::error::ArrowError;
use arrow::ffi::to_ffi;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_expr::{EmitTo, GroupsAccumulator};

use crate::arrow_wrappers::{WrappedArray, WrappedSchema};
use crate::util::FFIResult;
use crate::{df_result, rresult, rresult_return};

/// A stable struct for sharing [`GroupsAccumulator`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`GroupsAccumulator`].
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_GroupsAccumulator {
    pub update_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
        group_indices: RVec<usize>,
        opt_filter: ROption<WrappedArray>,
        total_num_groups: usize,
    ) -> FFIResult<()>,

    // Evaluate and return a ScalarValues as protobuf bytes
    pub evaluate: unsafe extern "C" fn(
        accumulator: &mut Self,
        emit_to: FFI_EmitTo,
    ) -> FFIResult<WrappedArray>,

    pub size: unsafe extern "C" fn(accumulator: &Self) -> usize,

    pub state: unsafe extern "C" fn(
        accumulator: &mut Self,
        emit_to: FFI_EmitTo,
    ) -> FFIResult<RVec<WrappedArray>>,

    pub merge_batch: unsafe extern "C" fn(
        accumulator: &mut Self,
        values: RVec<WrappedArray>,
        group_indices: RVec<usize>,
        opt_filter: ROption<WrappedArray>,
        total_num_groups: usize,
    ) -> FFIResult<()>,

    pub convert_to_state: unsafe extern "C" fn(
        accumulator: &Self,
        values: RVec<WrappedArray>,
        opt_filter: ROption<WrappedArray>,
    ) -> FFIResult<RVec<WrappedArray>>,

    pub supports_convert_to_state: bool,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(accumulator: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the accumulator.
    /// A [`ForeignGroupsAccumulator`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

pub struct GroupsAccumulatorPrivateData {
    pub accumulator: Box<dyn GroupsAccumulator>,
}

impl FFI_GroupsAccumulator {
    #[inline]
    unsafe fn inner_mut(&mut self) -> &mut Box<dyn GroupsAccumulator> {
        unsafe {
            let private_data = self.private_data as *mut GroupsAccumulatorPrivateData;
            &mut (*private_data).accumulator
        }
    }

    #[inline]
    unsafe fn inner(&self) -> &dyn GroupsAccumulator {
        unsafe {
            let private_data = self.private_data as *const GroupsAccumulatorPrivateData;
            (*private_data).accumulator.deref()
        }
    }
}

fn process_values(values: RVec<WrappedArray>) -> Result<Vec<Arc<dyn Array>>> {
    values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>()
}

/// Convert C-typed opt_filter into the internal type.
fn process_opt_filter(opt_filter: ROption<WrappedArray>) -> Result<Option<BooleanArray>> {
    opt_filter
        .into_option()
        .map(|filter| {
            ArrayRef::try_from(filter)
                .map_err(DataFusionError::from)
                .map(|arr| BooleanArray::from(arr.into_data()))
        })
        .transpose()
}

unsafe extern "C" fn update_batch_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    group_indices: RVec<usize>,
    opt_filter: ROption<WrappedArray>,
    total_num_groups: usize,
) -> FFIResult<()> {
    unsafe {
        let accumulator = accumulator.inner_mut();
        let values = rresult_return!(process_values(values));
        let group_indices: Vec<usize> = group_indices.into_iter().collect();
        let opt_filter = rresult_return!(process_opt_filter(opt_filter));

        rresult!(accumulator.update_batch(
            &values,
            &group_indices,
            opt_filter.as_ref(),
            total_num_groups
        ))
    }
}

unsafe extern "C" fn evaluate_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    emit_to: FFI_EmitTo,
) -> FFIResult<WrappedArray> {
    unsafe {
        let accumulator = accumulator.inner_mut();

        let result = rresult_return!(accumulator.evaluate(emit_to.into()));

        rresult!(WrappedArray::try_from(&result))
    }
}

unsafe extern "C" fn size_fn_wrapper(accumulator: &FFI_GroupsAccumulator) -> usize {
    unsafe {
        let accumulator = accumulator.inner();
        accumulator.size()
    }
}

unsafe extern "C" fn state_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    emit_to: FFI_EmitTo,
) -> FFIResult<RVec<WrappedArray>> {
    unsafe {
        let accumulator = accumulator.inner_mut();

        let state = rresult_return!(accumulator.state(emit_to.into()));
        rresult!(
            state
                .into_iter()
                .map(|arr| WrappedArray::try_from(&arr).map_err(DataFusionError::from))
                .collect::<Result<RVec<_>>>()
        )
    }
}

unsafe extern "C" fn merge_batch_fn_wrapper(
    accumulator: &mut FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    group_indices: RVec<usize>,
    opt_filter: ROption<WrappedArray>,
    total_num_groups: usize,
) -> FFIResult<()> {
    unsafe {
        let accumulator = accumulator.inner_mut();
        let values = rresult_return!(process_values(values));
        let group_indices: Vec<usize> = group_indices.into_iter().collect();
        let opt_filter = rresult_return!(process_opt_filter(opt_filter));

        rresult!(accumulator.merge_batch(
            &values,
            &group_indices,
            opt_filter.as_ref(),
            total_num_groups
        ))
    }
}

unsafe extern "C" fn convert_to_state_fn_wrapper(
    accumulator: &FFI_GroupsAccumulator,
    values: RVec<WrappedArray>,
    opt_filter: ROption<WrappedArray>,
) -> FFIResult<RVec<WrappedArray>> {
    unsafe {
        let accumulator = accumulator.inner();
        let values = rresult_return!(process_values(values));
        let opt_filter = rresult_return!(process_opt_filter(opt_filter));
        let state =
            rresult_return!(accumulator.convert_to_state(&values, opt_filter.as_ref()));

        rresult!(
            state
                .iter()
                .map(|arr| WrappedArray::try_from(arr).map_err(DataFusionError::from))
                .collect::<Result<RVec<_>>>()
        )
    }
}

unsafe extern "C" fn release_fn_wrapper(accumulator: &mut FFI_GroupsAccumulator) {
    unsafe {
        if !accumulator.private_data.is_null() {
            let private_data = Box::from_raw(
                accumulator.private_data as *mut GroupsAccumulatorPrivateData,
            );
            drop(private_data);
            accumulator.private_data = null_mut();
        }
    }
}

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
            library_marker_id: crate::get_library_marker_id,
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

impl From<FFI_GroupsAccumulator> for Box<dyn GroupsAccumulator> {
    fn from(mut accumulator: FFI_GroupsAccumulator) -> Self {
        if (accumulator.library_marker_id)() == crate::get_library_marker_id() {
            unsafe {
                let private_data = Box::from_raw(
                    accumulator.private_data as *mut GroupsAccumulatorPrivateData,
                );
                // We must set this to null to avoid a double free
                accumulator.private_data = null_mut();
                private_data.accumulator
            }
        } else {
            Box::new(ForeignGroupsAccumulator { accumulator })
        }
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
                &mut self.accumulator,
                emit_to.into()
            ))?;

            return_array.try_into().map_err(DataFusionError::from)
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        unsafe {
            let returned_arrays = df_result!((self.accumulator.state)(
                &mut self.accumulator,
                emit_to.into()
            ))?;

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
    use arrow::array::{Array, BooleanArray, make_array};
    use datafusion::common::create_array;
    use datafusion::error::Result;
    use datafusion::functions_aggregate::stddev::StddevGroupsAccumulator;
    use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
    use datafusion_functions_aggregate_common::aggregate::groups_accumulator::bool_op::BooleanGroupsAccumulator;
    use datafusion_functions_aggregate_common::stats::StatsType;

    use super::{FFI_EmitTo, FFI_GroupsAccumulator, ForeignGroupsAccumulator};

    #[test]
    fn test_foreign_avg_accumulator() -> Result<()> {
        let boxed_accum: Box<dyn GroupsAccumulator> =
            Box::new(BooleanGroupsAccumulator::new(|a, b| a && b, true));
        let mut ffi_accum: FFI_GroupsAccumulator = boxed_accum.into();
        ffi_accum.library_marker_id = crate::mock_foreign_marker_id;
        let mut foreign_accum: Box<dyn GroupsAccumulator> = ffi_accum.into();

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

    #[test]
    fn test_ffi_groups_accumulator_local_bypass_inner() -> Result<()> {
        let original_accum = StddevGroupsAccumulator::new(StatsType::Population);
        let boxed_accum: Box<dyn GroupsAccumulator> = Box::new(original_accum);
        let original_size = boxed_accum.size();

        let ffi_accum: FFI_GroupsAccumulator = boxed_accum.into();

        // Verify local libraries can be downcast to their original
        let foreign_accum: Box<dyn GroupsAccumulator> = ffi_accum.into();
        unsafe {
            let concrete = &*(foreign_accum.as_ref() as *const dyn GroupsAccumulator
                as *const StddevGroupsAccumulator);
            assert_eq!(original_size, concrete.size());
        }

        // Verify different library markers generate foreign accumulator
        let original_accum = StddevGroupsAccumulator::new(StatsType::Population);
        let boxed_accum: Box<dyn GroupsAccumulator> = Box::new(original_accum);
        let mut ffi_accum: FFI_GroupsAccumulator = boxed_accum.into();
        ffi_accum.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_accum: Box<dyn GroupsAccumulator> = ffi_accum.into();
        unsafe {
            let concrete = &*(foreign_accum.as_ref() as *const dyn GroupsAccumulator
                as *const ForeignGroupsAccumulator);
            assert_eq!(original_size, concrete.size());
        }

        Ok(())
    }
}
