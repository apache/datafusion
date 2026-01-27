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
use std::task::Poll;

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RResult};
use arrow::array::{Array, RecordBatch, StructArray, make_array};
use arrow::ffi::{from_ffi, to_ffi};
use async_ffi::{ContextExt, FfiContext, FfiPoll};
use datafusion_common::{DataFusionError, Result, ffi_datafusion_err, ffi_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, TryStreamExt};
use tokio::runtime::Handle;

use crate::arrow_wrappers::{WrappedArray, WrappedSchema};
use crate::rresult;
use crate::util::FFIResult;

/// A stable struct for sharing [`RecordBatchStream`] across FFI boundaries.
/// We use the async-ffi crate for handling async calls across libraries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_RecordBatchStream {
    /// This mirrors the `poll_next` of [`RecordBatchStream`] but does so
    /// in a FFI safe manner.
    pub poll_next: unsafe extern "C" fn(
        stream: &Self,
        cx: &mut FfiContext,
    ) -> FfiPoll<ROption<FFIResult<WrappedArray>>>,

    /// Return the schema of the record batch
    pub schema: unsafe extern "C" fn(stream: &Self) -> WrappedSchema,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

pub struct RecordBatchStreamPrivateData {
    pub rbs: SendableRecordBatchStream,
    pub runtime: Option<Handle>,
}

impl From<SendableRecordBatchStream> for FFI_RecordBatchStream {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self::new(stream, None)
    }
}

impl FFI_RecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream, runtime: Option<Handle>) -> Self {
        let private_data = Box::into_raw(Box::new(RecordBatchStreamPrivateData {
            rbs: stream,
            runtime,
        })) as *mut c_void;
        FFI_RecordBatchStream {
            poll_next: poll_next_fn_wrapper,
            schema: schema_fn_wrapper,
            release: release_fn_wrapper,
            private_data,
        }
    }
}

unsafe impl Send for FFI_RecordBatchStream {}

unsafe extern "C" fn schema_fn_wrapper(stream: &FFI_RecordBatchStream) -> WrappedSchema {
    unsafe {
        let private_data = stream.private_data as *const RecordBatchStreamPrivateData;
        let stream = &(*private_data).rbs;

        (*stream).schema().into()
    }
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_RecordBatchStream) {
    unsafe {
        debug_assert!(!provider.private_data.is_null());
        let private_data =
            Box::from_raw(provider.private_data as *mut RecordBatchStreamPrivateData);
        drop(private_data);
        provider.private_data = std::ptr::null_mut();
    }
}

pub(crate) fn record_batch_to_wrapped_array(
    record_batch: RecordBatch,
) -> FFIResult<WrappedArray> {
    let schema = WrappedSchema::from(record_batch.schema());
    let struct_array = StructArray::from(record_batch);
    rresult!(
        to_ffi(&struct_array.to_data())
            .map(|(array, _schema)| WrappedArray { array, schema })
    )
}

// probably want to use pub unsafe fn from_ffi(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayData> {
fn maybe_record_batch_to_wrapped_stream(
    record_batch: Option<Result<RecordBatch>>,
) -> ROption<FFIResult<WrappedArray>> {
    match record_batch {
        Some(Ok(record_batch)) => {
            ROption::RSome(record_batch_to_wrapped_array(record_batch))
        }
        Some(Err(e)) => ROption::RSome(RResult::RErr(e.to_string().into())),
        None => ROption::RNone,
    }
}

unsafe extern "C" fn poll_next_fn_wrapper(
    stream: &FFI_RecordBatchStream,
    cx: &mut FfiContext,
) -> FfiPoll<ROption<FFIResult<WrappedArray>>> {
    unsafe {
        let private_data = stream.private_data as *mut RecordBatchStreamPrivateData;
        let stream = &mut (*private_data).rbs;

        let _guard = (*private_data).runtime.as_ref().map(|rt| rt.enter());

        let poll_result = cx.with_context(|std_cx| {
            (*stream)
                .try_poll_next_unpin(std_cx)
                .map(maybe_record_batch_to_wrapped_stream)
        });

        poll_result.into()
    }
}

impl RecordBatchStream for FFI_RecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        let wrapped_schema = unsafe { (self.schema)(self) };
        wrapped_schema.into()
    }
}

pub(crate) fn wrapped_array_to_record_batch(array: WrappedArray) -> Result<RecordBatch> {
    let array_data =
        unsafe { from_ffi(array.array, &array.schema.0).map_err(DataFusionError::from)? };
    let schema: arrow::datatypes::SchemaRef = array.schema.into();
    let array = make_array(array_data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ffi_datafusion_err!(
        "Unexpected array type during record batch collection in FFI_RecordBatchStream - expected StructArray"
    ))?;

    let rb: RecordBatch = struct_array.into();

    rb.with_schema(schema).map_err(Into::into)
}

fn maybe_wrapped_array_to_record_batch(
    array: ROption<FFIResult<WrappedArray>>,
) -> Option<Result<RecordBatch>> {
    match array {
        ROption::RSome(RResult::ROk(wrapped_array)) => {
            Some(wrapped_array_to_record_batch(wrapped_array))
        }
        ROption::RSome(RResult::RErr(e)) => Some(ffi_err!("{e}")),
        ROption::RNone => None,
    }
}

impl Stream for FFI_RecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll_result =
            unsafe { cx.with_ffi_context(|ffi_cx| (self.poll_next)(&self, ffi_cx)) };

        match poll_result {
            FfiPoll::Ready(array) => {
                Poll::Ready(maybe_wrapped_array_to_record_batch(array))
            }
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => Poll::Ready(Some(ffi_err!(
                "Panic occurred during poll_next on FFI_RecordBatchStream"
            ))),
        }
    }
}

impl Drop for FFI_RecordBatchStream {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::record_batch;
    use datafusion::error::Result;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::test_util::bounded_stream;
    use futures::StreamExt;

    use super::{
        FFI_RecordBatchStream, record_batch_to_wrapped_array,
        wrapped_array_to_record_batch,
    };
    use crate::df_result;

    #[tokio::test]
    async fn test_round_trip_record_batch_stream() -> Result<()> {
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 3]),
            ("b", Float64, vec![Some(4.0), None, Some(5.0)])
        )?;
        let original_rbs = bounded_stream(record_batch.clone(), 1);

        let ffi_rbs: FFI_RecordBatchStream = original_rbs.into();
        let mut ffi_rbs: SendableRecordBatchStream = Box::pin(ffi_rbs);

        let schema = ffi_rbs.schema();
        assert_eq!(
            schema,
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Float64, true)
            ]))
        );

        let batch = ffi_rbs.next().await;
        assert!(batch.is_some());
        assert!(batch.as_ref().unwrap().is_ok());
        assert_eq!(batch.unwrap().unwrap(), record_batch);

        // There should only be one batch
        let no_batch = ffi_rbs.next().await;
        assert!(no_batch.is_none());

        Ok(())
    }

    #[test]
    fn round_trip_record_batch_with_metadata() -> Result<()> {
        let rb = record_batch!(
            ("a", Int32, vec![1, 2, 3]),
            ("b", Float64, vec![Some(4.0), None, Some(5.0)])
        )?;

        let schema = rb
            .schema()
            .as_ref()
            .clone()
            .with_metadata([("some_key".to_owned(), "some_value".to_owned())].into())
            .into();

        let rb = rb.with_schema(schema)?;

        let ffi_rb = df_result!(record_batch_to_wrapped_array(rb.clone()))?;

        let round_trip_rb = wrapped_array_to_record_batch(ffi_rb)?;

        assert_eq!(rb, round_trip_rb);
        Ok(())
    }
}
