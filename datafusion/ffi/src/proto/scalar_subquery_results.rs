// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::ffi::c_void;
use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::physical_planning_context::{
    ScalarSubqueryResults, ScalarSubqueryResultsBackend, SubqueryIndex,
};
use prost::Message;

use stabby::vec::Vec as SVec;

use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult, sresult_return};

/// Shares a [`ScalarSubqueryResults`] container across an FFI boundary, so a
/// `ScalarSubqueryExpr` decoded on the far side reads the values the
/// `ScalarSubqueryExec` on the near side populates.
///
/// `ScalarValue`s cross as encoded bytes, so this does not rely on their
/// memory layout being stable across the boundary.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ScalarSubqueryResults {
    get: unsafe extern "C" fn(&Self, index: u64) -> FFI_Result<FFI_Option<SVec<u8>>>,

    set: unsafe extern "C" fn(&Self, index: u64, value: SVec<u8>) -> FFI_Result<()>,

    clear: unsafe extern "C" fn(&Self),

    /// Used to create a clone on the provider of the results container. This
    /// should only need to be called by the receiver of the handle.
    clone: unsafe extern "C" fn(&Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(&mut Self),

    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ScalarSubqueryResults {}
unsafe impl Sync for FFI_ScalarSubqueryResults {}

impl FFI_ScalarSubqueryResults {
    fn inner(&self) -> &ScalarSubqueryResults {
        let private_data = self.private_data as *const ScalarSubqueryResults;
        unsafe { &*private_data }
    }

    /// Creates a new [`FFI_ScalarSubqueryResults`] sharing `results`.
    pub fn new(results: ScalarSubqueryResults) -> Self {
        let private_data = Box::new(results);

        Self {
            get: get_fn_wrapper,
            set: set_fn_wrapper,
            clear: clear_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data).cast::<c_void>(),
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

fn encode_scalar_value(value: &ScalarValue) -> Result<SVec<u8>> {
    let proto: datafusion_proto::protobuf::ScalarValue =
        value.try_into().map_err(DataFusionError::from)?;
    Ok(proto.encode_to_vec().into_iter().collect())
}

fn decode_scalar_value(bytes: &[u8]) -> Result<ScalarValue> {
    let proto = datafusion_proto::protobuf::ScalarValue::decode(bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    ScalarValue::try_from(&proto).map_err(DataFusionError::from)
}

unsafe extern "C" fn get_fn_wrapper(
    results: &FFI_ScalarSubqueryResults,
    index: u64,
) -> FFI_Result<FFI_Option<SVec<u8>>> {
    let value = results.inner().get(SubqueryIndex::new(index as usize));

    let encoded = value.as_ref().map(encode_scalar_value).transpose();

    sresult!(encoded.map(FFI_Option::from))
}

unsafe extern "C" fn set_fn_wrapper(
    results: &FFI_ScalarSubqueryResults,
    index: u64,
    value: SVec<u8>,
) -> FFI_Result<()> {
    let value = sresult_return!(decode_scalar_value(value.as_ref()));
    sresult!(
        results
            .inner()
            .set(SubqueryIndex::new(index as usize), value)
    )
}

unsafe extern "C" fn clear_fn_wrapper(results: &FFI_ScalarSubqueryResults) {
    results.inner().clear();
}

unsafe extern "C" fn clone_fn_wrapper(
    results: &FFI_ScalarSubqueryResults,
) -> FFI_ScalarSubqueryResults {
    FFI_ScalarSubqueryResults::new(results.inner().clone())
}

unsafe extern "C" fn release_fn_wrapper(results: &mut FFI_ScalarSubqueryResults) {
    unsafe {
        drop(Box::from_raw(
            results.private_data.cast::<ScalarSubqueryResults>(),
        ));
    }
}

impl Drop for FFI_ScalarSubqueryResults {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Clone for FFI_ScalarSubqueryResults {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it
/// has no guarantees about being able to access the data behind
/// `private_data`. It forwards every [`ScalarSubqueryResultsBackend`] call
/// back across the boundary to the real container owned by the other side.
#[derive(Debug)]
struct ForeignScalarSubqueryResultsBackend(FFI_ScalarSubqueryResults);

unsafe impl Send for ForeignScalarSubqueryResultsBackend {}
unsafe impl Sync for ForeignScalarSubqueryResultsBackend {}

impl ScalarSubqueryResultsBackend for ForeignScalarSubqueryResultsBackend {
    fn get(&self, index: usize) -> Option<ScalarValue> {
        let bytes = match unsafe { (self.0.get)(&self.0, index as u64) } {
            FFI_Result::Ok(bytes) => bytes.into_option()?,
            FFI_Result::Err(_) => return None,
        };
        decode_scalar_value(bytes.as_ref()).ok()
    }

    fn set(&self, index: usize, value: ScalarValue) -> Result<()> {
        let bytes = encode_scalar_value(&value)?;
        df_result!(unsafe { (self.0.set)(&self.0, index as u64, bytes) })
    }

    fn clear(&self) {
        unsafe { (self.0.clear)(&self.0) }
    }
}

impl From<FFI_ScalarSubqueryResults> for ScalarSubqueryResults {
    fn from(results: FFI_ScalarSubqueryResults) -> Self {
        if (results.library_marker_id)() == crate::get_library_marker_id() {
            Self::clone(results.inner())
        } else {
            Self::from_backend(Arc::new(ForeignScalarSubqueryResultsBackend(results)))
        }
    }
}
