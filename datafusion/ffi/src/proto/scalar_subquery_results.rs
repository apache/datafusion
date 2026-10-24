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
use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_common::{DataFusionError, ScalarValue, internal_datafusion_err};
use datafusion_expr::physical_planning_context::{
    ScalarSubqueryResults, ScalarSubqueryResultsBackend, SubqueryIndex,
};
use datafusion_proto::protobuf::ArrowType;
use prost::Message;

use stabby::vec::Vec as SVec;

use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult, sresult_return};

/// A stable struct for sharing the shared results container of a
/// `ScalarSubqueryExec` across FFI boundaries.
///
/// Unlike most FFI wrappers in this crate, which stand in for a whole
/// `dyn Trait` object, this one stands in for a single
/// [`ScalarSubqueryResults`] value: the active scope that a
/// `PhysicalExtensionCodec` on the near side of the boundary passes to
/// `try_decode_with_ctx` so a `ScalarSubqueryExpr` decoded on the far side
/// reads the same populated values as the `ScalarSubqueryExec` that owns
/// them.
///
/// `ScalarValue`s cross the boundary as prost-encoded bytes, the same wire
/// format `PhysicalExtensionCodec::try_decode`/`try_encode` already use, so
/// this struct never assumes a stable memory layout for `ScalarValue` itself.
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

/// The wire form a [`ScalarValue`] crosses the FFI boundary in.
///
/// `datafusion_proto::protobuf::ScalarValue` alone is not a type-preserving
/// transport: it has no dedicated variant for every `DataType` (`Float16`,
/// for example, is encoded as `Float32Value`), so decoding it back can yield
/// a `ScalarValue` of a different, widened type. Carrying the original
/// `DataType` alongside the encoded value lets [`decode_scalar_value`] cast
/// back down to it, so a foreign `ScalarSubqueryExpr` observes the same type
/// it was declared with.
#[derive(Clone, PartialEq, Message)]
struct ScalarValueEnvelope {
    #[prost(message, optional, tag = "1")]
    data_type: Option<ArrowType>,
    #[prost(bytes, tag = "2")]
    value: Vec<u8>,
}

fn encode_scalar_value(value: &ScalarValue) -> Result<SVec<u8>> {
    let data_type: ArrowType = (&value.data_type())
        .try_into()
        .map_err(DataFusionError::from)?;
    let proto: datafusion_proto::protobuf::ScalarValue =
        value.try_into().map_err(DataFusionError::from)?;
    let envelope = ScalarValueEnvelope {
        data_type: Some(data_type),
        value: proto.encode_to_vec(),
    };
    Ok(envelope.encode_to_vec().into_iter().collect())
}

fn decode_scalar_value(bytes: &[u8]) -> Result<ScalarValue> {
    let envelope = ScalarValueEnvelope::decode(bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let data_type_proto = envelope.data_type.ok_or_else(|| {
        internal_datafusion_err!("ScalarValueEnvelope is missing its data_type")
    })?;
    let data_type = (&data_type_proto)
        .try_into()
        .map_err(DataFusionError::from)?;

    let proto =
        datafusion_proto::protobuf::ScalarValue::decode(envelope.value.as_slice())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let value = ScalarValue::try_from(&proto).map_err(DataFusionError::from)?;

    if value.data_type() == data_type {
        Ok(value)
    } else {
        value.cast_to(&data_type)
    }
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

#[cfg(test)]
mod tests {
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };

    use super::FFI_ScalarSubqueryResults;

    #[test]
    fn roundtrip_ffi_scalar_subquery_results() -> Result<()> {
        let results = ScalarSubqueryResults::new(1);
        let mut ffi_results = FFI_ScalarSubqueryResults::new(results.clone());
        ffi_results.library_marker_id = crate::mock_foreign_marker_id;

        let foreign: ScalarSubqueryResults = ffi_results.into();

        assert_eq!(foreign.get(SubqueryIndex::new(0)), None);

        results.set(SubqueryIndex::new(0), ScalarValue::Int64(Some(42)))?;
        assert_eq!(
            foreign.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int64(Some(42)))
        );

        foreign.clear();
        assert_eq!(results.get(SubqueryIndex::new(0)), None);

        foreign.set(SubqueryIndex::new(0), ScalarValue::Int64(Some(7)))?;
        assert_eq!(
            results.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int64(Some(7)))
        );

        Ok(())
    }

    /// `datafusion_proto::protobuf::ScalarValue` has no dedicated variant for
    /// every `DataType`: a non-null `Float16` is encoded as `Float32Value`,
    /// so decoding it back without the original type yields a `Float32`
    /// instead. A value crossing a forced-foreign `FFI_ScalarSubqueryResults`
    /// handle, in either direction, must still be a `Float16`, matching what
    /// a `ScalarSubqueryExpr` declared it would return.
    #[test]
    fn ffi_scalar_subquery_results_preserves_non_null_float16() -> Result<()> {
        use datafusion_common::arrow::datatypes::DataType;

        let float16 = ScalarValue::Float32(Some(1.5)).cast_to(&DataType::Float16)?;

        // get: the host sets a Float16, a forced-foreign reader must read a
        // Float16 back, not the Float32 the wire format would otherwise widen
        // it to.
        let results = ScalarSubqueryResults::new(1);
        let mut ffi_results = FFI_ScalarSubqueryResults::new(results.clone());
        ffi_results.library_marker_id = crate::mock_foreign_marker_id;
        let foreign: ScalarSubqueryResults = ffi_results.into();

        results.set(SubqueryIndex::new(0), float16.clone())?;
        let read_back = foreign.get(SubqueryIndex::new(0));
        assert_eq!(read_back, Some(float16.clone()));
        assert_eq!(read_back.unwrap().data_type(), DataType::Float16);

        // set: a forced-foreign writer sets a Float16, the host must read a
        // Float16 back too.
        let results = ScalarSubqueryResults::new(1);
        let mut ffi_results = FFI_ScalarSubqueryResults::new(results.clone());
        ffi_results.library_marker_id = crate::mock_foreign_marker_id;
        let foreign: ScalarSubqueryResults = ffi_results.into();

        foreign.set(SubqueryIndex::new(0), float16.clone())?;
        let read_back = results.get(SubqueryIndex::new(0));
        assert_eq!(read_back, Some(float16.clone()));
        assert_eq!(read_back.unwrap().data_type(), DataType::Float16);

        Ok(())
    }
}
