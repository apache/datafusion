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

use std::{ffi::c_void, ops::Range};

use crate::{arrow_wrappers::WrappedArray, df_result, rresult, rresult_return};
use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use arrow::{array::ArrayRef, error::ArrowError};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{window_state::WindowAggState, PartitionEvaluator},
    scalar::ScalarValue,
};
use prost::Message;

use super::range::FFI_Range;

/// A stable struct for sharing [`PartitionEvaluator`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`PartitionEvaluator`].
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PartitionEvaluator {
    pub evaluate_all: unsafe extern "C" fn(
        evaluator: &mut Self,
        values: RVec<WrappedArray>,
        num_rows: usize,
    ) -> RResult<WrappedArray, RString>,

    pub evaluate: unsafe extern "C" fn(
        evaluator: &mut Self,
        values: RVec<WrappedArray>,
        range: FFI_Range,
    ) -> RResult<RVec<u8>, RString>,

    pub evaluate_all_with_rank: unsafe extern "C" fn(
        evaluator: &Self,
        num_rows: usize,
        ranks_in_partition: RVec<FFI_Range>,
    )
        -> RResult<WrappedArray, RString>,

    pub get_range: unsafe extern "C" fn(
        evaluator: &Self,
        idx: usize,
        n_rows: usize,
    ) -> RResult<FFI_Range, RString>,

    pub is_causal: bool,

    pub supports_bounded_execution: bool,
    pub uses_window_frame: bool,
    pub include_rank: bool,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(evaluator: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the evaluator.
    /// A [`ForeignPartitionEvaluator`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_PartitionEvaluator {}
unsafe impl Sync for FFI_PartitionEvaluator {}

pub struct PartitionEvaluatorPrivateData {
    pub evaluator: Box<dyn PartitionEvaluator>,
}

impl FFI_PartitionEvaluator {
    unsafe fn inner_mut(&mut self) -> &mut Box<dyn PartitionEvaluator + 'static> {
        let private_data = self.private_data as *mut PartitionEvaluatorPrivateData;
        &mut (*private_data).evaluator
    }

    unsafe fn inner(&self) -> &(dyn PartitionEvaluator + 'static) {
        let private_data = self.private_data as *mut PartitionEvaluatorPrivateData;
        (*private_data).evaluator.as_ref()
    }
}

unsafe extern "C" fn evaluate_all_fn_wrapper(
    evaluator: &mut FFI_PartitionEvaluator,
    values: RVec<WrappedArray>,
    num_rows: usize,
) -> RResult<WrappedArray, RString> {
    let inner = evaluator.inner_mut();

    let values_arrays = values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>();
    let values_arrays = rresult_return!(values_arrays);

    let return_array = inner
        .evaluate_all(&values_arrays, num_rows)
        .and_then(|array| WrappedArray::try_from(&array).map_err(DataFusionError::from));

    rresult!(return_array)
}

unsafe extern "C" fn evaluate_fn_wrapper(
    evaluator: &mut FFI_PartitionEvaluator,
    values: RVec<WrappedArray>,
    range: FFI_Range,
) -> RResult<RVec<u8>, RString> {
    let inner = evaluator.inner_mut();

    let values_arrays = values
        .into_iter()
        .map(|v| v.try_into().map_err(DataFusionError::from))
        .collect::<Result<Vec<ArrayRef>>>();
    let values_arrays = rresult_return!(values_arrays);

    // let return_array = (inner.evaluate(&values_arrays, &range.into()));
    // .and_then(|array| WrappedArray::try_from(&array).map_err(DataFusionError::from));
    let scalar_result = rresult_return!(inner.evaluate(&values_arrays, &range.into()));
    let proto_result: datafusion_proto::protobuf::ScalarValue =
        rresult_return!((&scalar_result).try_into());

    RResult::ROk(proto_result.encode_to_vec().into())
}

unsafe extern "C" fn evaluate_all_with_rank_fn_wrapper(
    evaluator: &FFI_PartitionEvaluator,
    num_rows: usize,
    ranks_in_partition: RVec<FFI_Range>,
) -> RResult<WrappedArray, RString> {
    let inner = evaluator.inner();

    let ranks_in_partition = ranks_in_partition
        .into_iter()
        .map(Range::from)
        .collect::<Vec<_>>();

    let return_array = inner
        .evaluate_all_with_rank(num_rows, &ranks_in_partition)
        .and_then(|array| WrappedArray::try_from(&array).map_err(DataFusionError::from));

    rresult!(return_array)
}

unsafe extern "C" fn get_range_fn_wrapper(
    evaluator: &FFI_PartitionEvaluator,
    idx: usize,
    n_rows: usize,
) -> RResult<FFI_Range, RString> {
    let inner = evaluator.inner();
    let range = inner.get_range(idx, n_rows).map(FFI_Range::from);

    rresult!(range)
}

unsafe extern "C" fn release_fn_wrapper(evaluator: &mut FFI_PartitionEvaluator) {
    if !evaluator.private_data.is_null() {
        let private_data =
            Box::from_raw(evaluator.private_data as *mut PartitionEvaluatorPrivateData);
        drop(private_data);
    }
}

impl From<Box<dyn PartitionEvaluator>> for FFI_PartitionEvaluator {
    fn from(evaluator: Box<dyn PartitionEvaluator>) -> Self {
        let is_causal = evaluator.is_causal();
        let supports_bounded_execution = evaluator.supports_bounded_execution();
        let include_rank = evaluator.include_rank();
        let uses_window_frame = evaluator.uses_window_frame();

        let private_data = PartitionEvaluatorPrivateData { evaluator };

        Self {
            evaluate: evaluate_fn_wrapper,
            evaluate_all: evaluate_all_fn_wrapper,
            evaluate_all_with_rank: evaluate_all_with_rank_fn_wrapper,
            get_range: get_range_fn_wrapper,
            is_causal,
            supports_bounded_execution,
            include_rank,
            uses_window_frame,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(private_data)) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_PartitionEvaluator {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignPartitionEvaluator is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_PartitionEvaluator.
#[derive(Debug)]
pub struct ForeignPartitionEvaluator {
    evaluator: FFI_PartitionEvaluator,
}

impl From<FFI_PartitionEvaluator> for Box<dyn PartitionEvaluator> {
    fn from(mut evaluator: FFI_PartitionEvaluator) -> Self {
        if (evaluator.library_marker_id)() == crate::get_library_marker_id() {
            unsafe {
                let private_data = Box::from_raw(
                    evaluator.private_data as *mut PartitionEvaluatorPrivateData,
                );
                // We must set this to null to avoid a double free
                evaluator.private_data = std::ptr::null_mut();
                private_data.evaluator
            }
        } else {
            Box::new(ForeignPartitionEvaluator { evaluator })
        }
    }
}

impl PartitionEvaluator for ForeignPartitionEvaluator {
    fn memoize(&mut self, _state: &mut WindowAggState) -> Result<()> {
        // Exposing `memoize` increases the surface are of the FFI work
        // so for now we dot support it.
        Ok(())
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        let range = unsafe { (self.evaluator.get_range)(&self.evaluator, idx, n_rows) };
        df_result!(range).map(Range::from)
    }

    /// Get whether evaluator needs future data for its result (if so returns `false`) or not
    fn is_causal(&self) -> bool {
        self.evaluator.is_causal
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let result = unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<RVec<_>, ArrowError>>()?;
            (self.evaluator.evaluate_all)(&mut self.evaluator, values, num_rows)
        };

        let array = df_result!(result)?;

        Ok(array.try_into()?)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        unsafe {
            let values = values
                .iter()
                .map(WrappedArray::try_from)
                .collect::<std::result::Result<RVec<_>, ArrowError>>()?;

            let scalar_bytes = df_result!((self.evaluator.evaluate)(
                &mut self.evaluator,
                values,
                range.to_owned().into()
            ))?;

            let proto_scalar =
                datafusion_proto::protobuf::ScalarValue::decode(scalar_bytes.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            ScalarValue::try_from(&proto_scalar).map_err(DataFusionError::from)
        }
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let result = unsafe {
            let ranks_in_partition = ranks_in_partition
                .iter()
                .map(|rank| FFI_Range::from(rank.to_owned()))
                .collect();
            (self.evaluator.evaluate_all_with_rank)(
                &self.evaluator,
                num_rows,
                ranks_in_partition,
            )
        };

        let array = df_result!(result)?;

        Ok(array.try_into()?)
    }

    fn supports_bounded_execution(&self) -> bool {
        self.evaluator.supports_bounded_execution
    }

    fn uses_window_frame(&self) -> bool {
        self.evaluator.uses_window_frame
    }

    fn include_rank(&self) -> bool {
        self.evaluator.include_rank
    }
}

#[cfg(test)]
mod tests {
    use crate::udwf::partition_evaluator::{
        FFI_PartitionEvaluator, ForeignPartitionEvaluator,
    };
    use arrow::array::ArrayRef;
    use datafusion::logical_expr::PartitionEvaluator;

    #[derive(Debug)]
    struct TestPartitionEvaluator {}

    impl PartitionEvaluator for TestPartitionEvaluator {
        fn evaluate_all(
            &mut self,
            values: &[ArrayRef],
            _num_rows: usize,
        ) -> datafusion_common::Result<ArrayRef> {
            Ok(values[0].to_owned())
        }
    }

    #[test]
    fn test_ffi_partition_evaluator_local_bypass_inner() -> datafusion_common::Result<()>
    {
        let original_accum = TestPartitionEvaluator {};
        let boxed_accum: Box<dyn PartitionEvaluator> = Box::new(original_accum);

        let ffi_accum: FFI_PartitionEvaluator = boxed_accum.into();

        // Verify local libraries can be downcast to their original
        let foreign_accum: Box<dyn PartitionEvaluator> = ffi_accum.into();
        unsafe {
            let concrete = &*(foreign_accum.as_ref() as *const dyn PartitionEvaluator
                as *const TestPartitionEvaluator);
            assert!(!concrete.uses_window_frame());
        }

        // Verify different library markers generate foreign accumulator
        let original_accum = TestPartitionEvaluator {};
        let boxed_accum: Box<dyn PartitionEvaluator> = Box::new(original_accum);
        let mut ffi_accum: FFI_PartitionEvaluator = boxed_accum.into();
        ffi_accum.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_accum: Box<dyn PartitionEvaluator> = ffi_accum.into();
        unsafe {
            let concrete = &*(foreign_accum.as_ref() as *const dyn PartitionEvaluator
                as *const ForeignPartitionEvaluator);
            assert!(!concrete.uses_window_frame());
        }

        Ok(())
    }
}
