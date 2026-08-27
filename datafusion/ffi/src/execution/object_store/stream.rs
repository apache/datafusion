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

//! FFI-safe wrappers for the streams returned by
//! [`object_store::ObjectStore`].
//!
//! Three streams cross the boundary: the byte stream behind
//! [`object_store::GetResultPayload::Stream`], the [`ObjectMeta`] stream
//! returned by [`object_store::ObjectStore::list`], and the [`Path`] stream
//! used by [`object_store::ObjectStore::delete_stream`] in both directions.
//! All follow the same `poll_next` pattern used by
//! [`crate::record_batch_stream`], entering the producing library's tokio
//! runtime for the duration of the poll so that stores which spawn tasks or use
//! timers work when driven by a foreign executor.

use std::ffi::c_void;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_ffi::{ContextExt, FfiContext, FfiPoll};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectMeta};
use stabby::string::String as SString;
use tokio::runtime::Handle;

use crate::util::FFI_Option;

use super::buffer::FFI_Bytes;
use super::error::FFI_ObjectStoreResult;
use super::types::FFI_ObjectMeta;

type BytesStream = futures::stream::BoxStream<'static, object_store::Result<Bytes>>;
type ObjectMetaStream =
    futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>>;
type PathStream = futures::stream::BoxStream<'static, object_store::Result<Path>>;

/// Panic message used when a foreign `poll_next` unwinds.
const POLL_PANIC: &str = "Panic occurred while polling a foreign object store stream";

// -----------------------------------------------------------------------------
// Byte stream
// -----------------------------------------------------------------------------

/// A stable struct for sharing a stream of [`Bytes`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_BytesStream {
    /// Mirrors [`Stream::poll_next`] in an FFI safe manner.
    pub poll_next: unsafe extern "C" fn(
        stream: &Self,
        cx: &mut FfiContext,
    ) -> FfiPoll<
        FFI_Option<FFI_ObjectStoreResult<FFI_Bytes>>,
    >,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// stream. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

// Safety: the inner stream is a `BoxStream` which is `Send`, and access is
// serialized by the caller polling it.
unsafe impl Send for FFI_BytesStream {}

struct BytesStreamPrivateData {
    stream: BytesStream,
    runtime: Option<Handle>,
}

unsafe extern "C" fn bytes_poll_next_fn_wrapper(
    stream: &FFI_BytesStream,
    cx: &mut FfiContext,
) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<FFI_Bytes>>> {
    unsafe {
        let private_data = stream.private_data as *mut BytesStreamPrivateData;
        let _guard = (*private_data).runtime.as_ref().map(|rt| rt.enter());
        let stream = &mut (*private_data).stream;

        cx.with_context(|std_cx| {
            stream.poll_next_unpin(std_cx).map(|item| match item {
                Some(Ok(bytes)) => {
                    FFI_Option::Some(FFI_ObjectStoreResult::Ok(FFI_Bytes::from(bytes)))
                }
                Some(Err(e)) => FFI_Option::Some(FFI_ObjectStoreResult::Err(e.into())),
                None => FFI_Option::None,
            })
        })
        .into()
    }
}

unsafe extern "C" fn bytes_release_fn_wrapper(stream: &mut FFI_BytesStream) {
    unsafe {
        debug_assert!(!stream.private_data.is_null());
        drop(Box::from_raw(
            stream.private_data as *mut BytesStreamPrivateData,
        ));
        stream.private_data = std::ptr::null_mut();
    }
}

impl FFI_BytesStream {
    pub fn new(stream: BytesStream, runtime: Option<Handle>) -> Self {
        Self {
            poll_next: bytes_poll_next_fn_wrapper,
            release: bytes_release_fn_wrapper,
            private_data: Box::into_raw(Box::new(BytesStreamPrivateData {
                stream,
                runtime,
            })) as *mut c_void,
        }
    }
}

impl Drop for FFI_BytesStream {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Stream for FFI_BytesStream {
    type Item = object_store::Result<Bytes>;

    // `Stream::Item` is `object_store::Result<T>`, so the error type is fixed by
    // the trait and cannot be boxed to shrink it.
    #[expect(clippy::result_large_err)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll_result =
            unsafe { cx.with_ffi_context(|ffi_cx| (self.poll_next)(&self, ffi_cx)) };

        match poll_result {
            FfiPoll::Ready(item) => Poll::Ready(item.into_option().map(|result| {
                Result::<FFI_Bytes, ObjectStoreError>::from(result).map(Bytes::from)
            })),
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => Poll::Ready(Some(Err(ObjectStoreError::Generic {
                store: "ForeignObjectStore",
                source: POLL_PANIC.into(),
            }))),
        }
    }
}

// -----------------------------------------------------------------------------
// ObjectMeta stream
// -----------------------------------------------------------------------------

/// A stable struct for sharing a stream of [`ObjectMeta`] across FFI
/// boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ObjectMetaStream {
    /// Mirrors [`Stream::poll_next`] in an FFI safe manner.
    pub poll_next: unsafe extern "C" fn(
        stream: &Self,
        cx: &mut FfiContext,
    ) -> FfiPoll<
        FFI_Option<FFI_ObjectStoreResult<FFI_ObjectMeta>>,
    >,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// stream. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

// Safety: see `FFI_BytesStream`.
unsafe impl Send for FFI_ObjectMetaStream {}

struct ObjectMetaStreamPrivateData {
    stream: ObjectMetaStream,
    runtime: Option<Handle>,
}

unsafe extern "C" fn meta_poll_next_fn_wrapper(
    stream: &FFI_ObjectMetaStream,
    cx: &mut FfiContext,
) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<FFI_ObjectMeta>>> {
    unsafe {
        let private_data = stream.private_data as *mut ObjectMetaStreamPrivateData;
        let _guard = (*private_data).runtime.as_ref().map(|rt| rt.enter());
        let stream = &mut (*private_data).stream;

        cx.with_context(|std_cx| {
            stream.poll_next_unpin(std_cx).map(|item| match item {
                Some(Ok(meta)) => FFI_Option::Some(FFI_ObjectStoreResult::Ok(
                    FFI_ObjectMeta::from(&meta),
                )),
                Some(Err(e)) => FFI_Option::Some(FFI_ObjectStoreResult::Err(e.into())),
                None => FFI_Option::None,
            })
        })
        .into()
    }
}

unsafe extern "C" fn meta_release_fn_wrapper(stream: &mut FFI_ObjectMetaStream) {
    unsafe {
        debug_assert!(!stream.private_data.is_null());
        drop(Box::from_raw(
            stream.private_data as *mut ObjectMetaStreamPrivateData,
        ));
        stream.private_data = std::ptr::null_mut();
    }
}

impl FFI_ObjectMetaStream {
    pub fn new(stream: ObjectMetaStream, runtime: Option<Handle>) -> Self {
        Self {
            poll_next: meta_poll_next_fn_wrapper,
            release: meta_release_fn_wrapper,
            private_data: Box::into_raw(Box::new(ObjectMetaStreamPrivateData {
                stream,
                runtime,
            })) as *mut c_void,
        }
    }
}

impl Drop for FFI_ObjectMetaStream {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Stream for FFI_ObjectMetaStream {
    type Item = object_store::Result<ObjectMeta>;

    // `Stream::Item` is `object_store::Result<T>`, so the error type is fixed by
    // the trait and cannot be boxed to shrink it.
    #[expect(clippy::result_large_err)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll_result =
            unsafe { cx.with_ffi_context(|ffi_cx| (self.poll_next)(&self, ffi_cx)) };

        match poll_result {
            FfiPoll::Ready(item) => Poll::Ready(item.into_option().map(|result| {
                Result::<FFI_ObjectMeta, ObjectStoreError>::from(result)
                    .map(ObjectMeta::from)
            })),
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => Poll::Ready(Some(Err(ObjectStoreError::Generic {
                store: "ForeignObjectStore",
                source: POLL_PANIC.into(),
            }))),
        }
    }
}

// -----------------------------------------------------------------------------
// Path stream
// -----------------------------------------------------------------------------

/// A stable struct for sharing a stream of [`Path`] across FFI boundaries.
///
/// [`object_store::ObjectStore::delete_stream`] both consumes and produces a
/// path stream, so this struct crosses the boundary in both directions. Each
/// side wraps its own stream, so the same struct serves both roles.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PathStream {
    /// Mirrors [`Stream::poll_next`] in an FFI safe manner.
    pub poll_next: unsafe extern "C" fn(
        stream: &Self,
        cx: &mut FfiContext,
    ) -> FfiPoll<
        FFI_Option<FFI_ObjectStoreResult<SString>>,
    >,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// stream. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

// Safety: see `FFI_BytesStream`.
unsafe impl Send for FFI_PathStream {}

struct PathStreamPrivateData {
    stream: PathStream,
    runtime: Option<Handle>,
}

unsafe extern "C" fn path_poll_next_fn_wrapper(
    stream: &FFI_PathStream,
    cx: &mut FfiContext,
) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<SString>>> {
    unsafe {
        let private_data = stream.private_data as *mut PathStreamPrivateData;
        let _guard = (*private_data).runtime.as_ref().map(|rt| rt.enter());
        let stream = &mut (*private_data).stream;

        cx.with_context(|std_cx| {
            stream.poll_next_unpin(std_cx).map(|item| match item {
                Some(Ok(path)) => {
                    FFI_Option::Some(FFI_ObjectStoreResult::Ok(path.as_ref().into()))
                }
                Some(Err(e)) => FFI_Option::Some(FFI_ObjectStoreResult::Err(e.into())),
                None => FFI_Option::None,
            })
        })
        .into()
    }
}

unsafe extern "C" fn path_release_fn_wrapper(stream: &mut FFI_PathStream) {
    unsafe {
        debug_assert!(!stream.private_data.is_null());
        drop(Box::from_raw(
            stream.private_data as *mut PathStreamPrivateData,
        ));
        stream.private_data = std::ptr::null_mut();
    }
}

impl FFI_PathStream {
    pub fn new(stream: PathStream, runtime: Option<Handle>) -> Self {
        Self {
            poll_next: path_poll_next_fn_wrapper,
            release: path_release_fn_wrapper,
            private_data: Box::into_raw(Box::new(PathStreamPrivateData {
                stream,
                runtime,
            })) as *mut c_void,
        }
    }
}

impl Drop for FFI_PathStream {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Stream for FFI_PathStream {
    type Item = object_store::Result<Path>;

    // `Stream::Item` is `object_store::Result<T>`, so the error type is fixed by
    // the trait and cannot be boxed to shrink it.
    #[expect(clippy::result_large_err)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll_result =
            unsafe { cx.with_ffi_context(|ffi_cx| (self.poll_next)(&self, ffi_cx)) };

        match poll_result {
            FfiPoll::Ready(item) => Poll::Ready(item.into_option().map(|result| {
                Result::<SString, ObjectStoreError>::from(result)
                    .map(|p| Path::from(p.to_string()))
            })),
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => Poll::Ready(Some(Err(ObjectStoreError::Generic {
                store: "ForeignObjectStore",
                source: POLL_PANIC.into(),
            }))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bytes_stream_round_trip() {
        let chunks = vec![
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"defg")),
        ];
        let stream = futures::stream::iter(chunks).boxed();

        let ffi = FFI_BytesStream::new(stream, None);
        let collected: Vec<_> = ffi.collect().await;

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].as_ref().unwrap(), &Bytes::from_static(b"abc"));
        assert_eq!(collected[1].as_ref().unwrap(), &Bytes::from_static(b"defg"));
    }

    #[tokio::test]
    async fn bytes_stream_propagates_error_variant() {
        let chunks = vec![
            Ok(Bytes::from_static(b"ok")),
            Err(ObjectStoreError::NotFound {
                path: "gone".into(),
                source: "missing".into(),
            }),
        ];
        let ffi = FFI_BytesStream::new(futures::stream::iter(chunks).boxed(), None);
        let collected: Vec<_> = ffi.collect().await;

        assert!(collected[0].is_ok());
        assert!(matches!(
            collected[1].as_ref().unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn object_meta_stream_round_trip() {
        let metas = vec![
            Ok(ObjectMeta {
                location: Path::from("a/1.parquet"),
                last_modified: chrono::DateTime::from_timestamp(5, 0).unwrap(),
                size: 10,
                e_tag: Some("e1".to_string()),
                version: None,
            }),
            Ok(ObjectMeta {
                location: Path::from("a/2.parquet"),
                last_modified: chrono::DateTime::from_timestamp(6, 0).unwrap(),
                size: 20,
                e_tag: None,
                version: None,
            }),
        ];
        let expected: Vec<ObjectMeta> =
            metas.iter().map(|m| m.as_ref().unwrap().clone()).collect();

        let ffi = FFI_ObjectMetaStream::new(futures::stream::iter(metas).boxed(), None);
        let collected: Vec<_> = ffi.collect().await;

        assert_eq!(collected.len(), 2);
        for (actual, expected) in collected.into_iter().zip(expected) {
            assert_eq!(actual.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn empty_stream_terminates() {
        let ffi = FFI_BytesStream::new(futures::stream::empty().boxed(), None);
        let collected: Vec<_> = ffi.collect().await;
        assert!(collected.is_empty());
    }

    #[tokio::test]
    async fn path_stream_round_trip() {
        let paths = vec![
            Ok(Path::from("a/1.parquet")),
            Ok(Path::from("b/c/2.parquet")),
        ];
        let ffi = FFI_PathStream::new(futures::stream::iter(paths).boxed(), None);
        let collected: Vec<_> = ffi.collect().await;

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].as_ref().unwrap(), &Path::from("a/1.parquet"));
        assert_eq!(collected[1].as_ref().unwrap(), &Path::from("b/c/2.parquet"));
    }

    #[tokio::test]
    async fn path_stream_propagates_error_variant() {
        let paths = vec![
            Ok(Path::from("ok")),
            Err(ObjectStoreError::NotFound {
                path: "gone".into(),
                source: "missing".into(),
            }),
        ];
        let ffi = FFI_PathStream::new(futures::stream::iter(paths).boxed(), None);
        let collected: Vec<_> = ffi.collect().await;

        assert!(collected[0].is_ok());
        assert!(matches!(
            collected[1].as_ref().unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));
    }
}
