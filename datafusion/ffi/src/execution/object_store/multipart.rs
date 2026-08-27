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

//! FFI support for [`MultipartUpload`].

use std::ffi::c_void;

use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use object_store::{MultipartUpload, PutResult, UploadPart};
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use super::buffer::FFI_Bytes;
use super::error::FFI_ObjectStoreResult;
use super::types::{FFI_PutResult, put_payload_from_ffi, put_payload_to_ffi};

/// A stable struct for sharing [`MultipartUpload`] across FFI boundaries.
///
/// # Safety
///
/// [`MultipartUpload::complete`] and [`MultipartUpload::abort`] take
/// `&mut self`, so the futures they return borrow the upload. The function
/// pointers below erase that lifetime. Callers must keep this struct alive
/// until the returned future has been driven to completion or dropped.
/// [`ForeignMultipartUpload`] upholds this because the `async_trait` methods
/// borrow `self` for the duration of the returned future.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_MultipartUpload {
    /// Upload the next part. The returned future does not borrow this struct.
    pub put_part: unsafe extern "C" fn(
        upload: &mut Self,
        payload: SVec<FFI_Bytes>,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,

    /// Complete the upload. The returned future borrows this struct.
    pub complete: unsafe extern "C" fn(
        upload: &mut Self,
    )
        -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>>,

    /// Abort the upload. The returned future borrows this struct.
    pub abort:
        unsafe extern "C" fn(upload: &mut Self) -> FfiFuture<FFI_ObjectStoreResult<()>>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// upload. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

// Safety: the inner `Box<dyn MultipartUpload>` is `Send` and is only reached
// through `&mut Self`, so access is exclusive.
unsafe impl Send for FFI_MultipartUpload {}

struct MultipartUploadPrivateData {
    upload: Box<dyn MultipartUpload>,
    runtime: Option<Handle>,
}

impl FFI_MultipartUpload {
    pub fn new(upload: Box<dyn MultipartUpload>, runtime: Option<Handle>) -> Self {
        Self {
            put_part: put_part_fn_wrapper,
            complete: complete_fn_wrapper,
            abort: abort_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(MultipartUploadPrivateData {
                upload,
                runtime,
            })) as *mut c_void,
        }
    }

    unsafe fn private_data(&mut self) -> &mut MultipartUploadPrivateData {
        unsafe { &mut *(self.private_data as *mut MultipartUploadPrivateData) }
    }
}

unsafe extern "C" fn put_part_fn_wrapper(
    upload: &mut FFI_MultipartUpload,
    payload: SVec<FFI_Bytes>,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    unsafe {
        let private_data = upload.private_data();
        let _guard = private_data.runtime.as_ref().map(|rt| rt.enter());
        // `put_part` returns a `'static` future, so nothing borrowed from
        // `upload` escapes into the returned future.
        let part: UploadPart =
            private_data.upload.put_part(put_payload_from_ffi(payload));
        async move { FFI_ObjectStoreResult::from(part.await) }.into_ffi()
    }
}

unsafe extern "C" fn complete_fn_wrapper(
    upload: &mut FFI_MultipartUpload,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>> {
    unsafe {
        let private_data = upload.private_data as *mut MultipartUploadPrivateData;
        // See the safety note on `FFI_MultipartUpload`: the caller keeps the
        // struct alive until this future resolves.
        let private_data = private_data as usize;
        // A tokio `EnterGuard` is not `Send` and so cannot be held across an
        // await. As elsewhere in this crate, runtime context is established per
        // poll by the stream wrappers rather than around a whole future.
        async move {
            let private_data = &mut *(private_data as *mut MultipartUploadPrivateData);
            FFI_ObjectStoreResult::from(
                private_data
                    .upload
                    .complete()
                    .await
                    .map(|r| FFI_PutResult::from(&r)),
            )
        }
        .into_ffi()
    }
}

unsafe extern "C" fn abort_fn_wrapper(
    upload: &mut FFI_MultipartUpload,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    unsafe {
        let private_data = upload.private_data as usize;
        async move {
            let private_data = &mut *(private_data as *mut MultipartUploadPrivateData);
            FFI_ObjectStoreResult::from(private_data.upload.abort().await)
        }
        .into_ffi()
    }
}

unsafe extern "C" fn release_fn_wrapper(upload: &mut FFI_MultipartUpload) {
    unsafe {
        debug_assert!(!upload.private_data.is_null());
        drop(Box::from_raw(
            upload.private_data as *mut MultipartUploadPrivateData,
        ));
        upload.private_data = std::ptr::null_mut();
    }
}

impl Drop for FFI_MultipartUpload {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// A [`MultipartUpload`] backed by a foreign [`FFI_MultipartUpload`].
#[derive(Debug)]
pub struct ForeignMultipartUpload {
    upload: FFI_MultipartUpload,
}

unsafe impl Send for ForeignMultipartUpload {}

impl From<FFI_MultipartUpload> for ForeignMultipartUpload {
    fn from(upload: FFI_MultipartUpload) -> Self {
        Self { upload }
    }
}

#[async_trait]
impl MultipartUpload for ForeignMultipartUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> UploadPart {
        let future = unsafe {
            (self.upload.put_part)(&mut self.upload, put_payload_to_ffi(&data))
        };
        Box::pin(async move { future.await.into() })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let future = unsafe { (self.upload.complete)(&mut self.upload) };
        Result::<FFI_PutResult, _>::from(future.await).map(PutResult::from)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let future = unsafe { (self.upload.abort)(&mut self.upload) };
        future.await.into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::PutPayload;

    use super::*;

    #[derive(Debug)]
    struct TestUpload {
        parts: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MultipartUpload for TestUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            self.parts
                .fetch_add(data.content_length(), Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            Ok(PutResult {
                e_tag: Some("final".to_string()),
                version: Some("v2".to_string()),
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn multipart_upload_round_trip() -> datafusion_common::Result<()> {
        let parts = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let upload = TestUpload {
            parts: Arc::clone(&parts),
            aborted: Arc::clone(&aborted),
        };

        let ffi = FFI_MultipartUpload::new(Box::new(upload), None);
        let mut foreign = ForeignMultipartUpload::from(ffi);

        foreign.put_part(PutPayload::from(vec![0u8; 16])).await?;
        foreign.put_part(PutPayload::from(vec![0u8; 32])).await?;
        assert_eq!(parts.load(Ordering::SeqCst), 48);

        let result = foreign.complete().await?;
        assert_eq!(result.e_tag.as_deref(), Some("final"));
        assert_eq!(result.version.as_deref(), Some("v2"));

        Ok(())
    }

    #[tokio::test]
    async fn multipart_abort_round_trip() -> datafusion_common::Result<()> {
        let parts = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let upload = TestUpload {
            parts: Arc::clone(&parts),
            aborted: Arc::clone(&aborted),
        };

        let ffi = FFI_MultipartUpload::new(Box::new(upload), None);
        let mut foreign = ForeignMultipartUpload::from(ffi);

        foreign.abort().await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 1);

        Ok(())
    }
}
