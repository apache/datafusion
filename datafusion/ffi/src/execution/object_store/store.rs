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

//! FFI support for [`ObjectStore`].
//!
//! # Which methods cross the boundary
//!
//! Every required method of [`ObjectStore`] is forwarded, plus the optional
//! [`ObjectStore::get_ranges`] and [`ObjectStore::list_with_offset`] overrides.
//! `get_ranges` matters because the Parquet reader issues one call per row
//! group to fetch column chunks; leaving it to the default implementation would
//! turn a single batched request into one `get_opts` round trip per range.
//!
//! The `ObjectStoreExt` methods (`head`, `copy_if_not_exists`,
//! `rename_if_not_exists`) are deliberately *not* forwarded. They are extension
//! methods defined in terms of the core trait, so they compose correctly on top
//! of the forwarded methods: `copy_if_not_exists` becomes `copy_opts` with
//! [`object_store::CopyMode::Create`], which crosses the boundary intact and so
//! keeps its atomicity guarantee.

use std::ffi::c_void;
use std::ops::Range;
use std::sync::Arc;

use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::util::FFI_Option;

use super::buffer::FFI_Bytes;
use super::error::FFI_ObjectStoreResult;
use super::multipart::{FFI_MultipartUpload, ForeignMultipartUpload};
use super::stream::{FFI_BytesStream, FFI_ObjectMetaStream, FFI_PathStream};
use super::types::{
    FFI_Attribute, FFI_CopyOptions, FFI_GetOptions, FFI_ListResult, FFI_ObjectMeta,
    FFI_PutMultipartOptions, FFI_PutOptions, FFI_PutResult, FFI_RenameOptions,
    attributes_from_ffi, attributes_to_ffi, put_payload_from_ffi, put_payload_to_ffi,
};

/// An FFI-safe byte range, used by [`ObjectStore::get_ranges`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFI_ByteRange {
    pub start: u64,
    pub end: u64,
}

/// An FFI-safe [`GetResult`].
///
/// [`GetResultPayload`] has a `File` variant that lets a local filesystem store
/// hand back an open file descriptor. A raw file descriptor is not portable
/// across the boundary, so the payload is always converted to a byte stream
/// with [`GetResult::into_stream`]. A local filesystem store reached through
/// FFI therefore loses the file fast path, which is why
/// [`FFI_ObjectStore::as_local`] exists: a store used within its own library
/// never goes through this struct at all.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_GetResult {
    pub payload: FFI_BytesStream,
    pub meta: FFI_ObjectMeta,
    pub range_start: u64,
    pub range_end: u64,
    pub attributes: SVec<FFI_Attribute>,
}

impl FFI_GetResult {
    fn new(result: GetResult, runtime: Option<Handle>) -> Self {
        let meta = FFI_ObjectMeta::from(&result.meta);
        let range_start = result.range.start;
        let range_end = result.range.end;
        let attributes = attributes_to_ffi(&result.attributes);

        Self {
            payload: FFI_BytesStream::new(result.into_stream(), runtime),
            meta,
            range_start,
            range_end,
            attributes,
        }
    }
}

impl From<FFI_GetResult> for GetResult {
    fn from(result: FFI_GetResult) -> Self {
        GetResult {
            meta: ObjectMeta::from(result.meta),
            range: result.range_start..result.range_end,
            attributes: attributes_from_ffi(result.attributes),
            payload: GetResultPayload::Stream(Box::pin(result.payload)),
        }
    }
}

/// A stable struct for sharing [`ObjectStore`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ObjectStore {
    pub put_opts: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        payload: SVec<FFI_Bytes>,
        options: FFI_PutOptions,
    )
        -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>>,

    pub put_multipart_opts: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        options: FFI_PutMultipartOptions,
    ) -> FfiFuture<
        FFI_ObjectStoreResult<FFI_MultipartUpload>,
    >,

    pub get_opts: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        options: FFI_GetOptions,
    )
        -> FfiFuture<FFI_ObjectStoreResult<FFI_GetResult>>,

    pub get_ranges: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        ranges: SVec<FFI_ByteRange>,
    ) -> FfiFuture<
        FFI_ObjectStoreResult<SVec<FFI_Bytes>>,
    >,

    /// Delete a stream of locations, yielding those successfully deleted.
    ///
    /// This is the required trait method; the simpler `delete` is an
    /// `ObjectStoreExt` extension defined in terms of it.
    pub delete_stream:
        unsafe extern "C" fn(store: &Self, locations: FFI_PathStream) -> FFI_PathStream,

    pub list: unsafe extern "C" fn(
        store: &Self,
        prefix: FFI_Option<SString>,
    ) -> FFI_ObjectMetaStream,

    pub list_with_offset: unsafe extern "C" fn(
        store: &Self,
        prefix: FFI_Option<SString>,
        offset: SString,
    ) -> FFI_ObjectMetaStream,

    pub list_with_delimiter:
        unsafe extern "C" fn(
            store: &Self,
            prefix: FFI_Option<SString>,
        ) -> FfiFuture<FFI_ObjectStoreResult<FFI_ListResult>>,

    pub copy_opts: unsafe extern "C" fn(
        store: &Self,
        from: SString,
        to: SString,
        options: FFI_CopyOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,

    pub rename_opts: unsafe extern "C" fn(
        store: &Self,
        from: SString,
        to: SString,
        options: FFI_RenameOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,

    /// Backs the [`std::fmt::Display`] bound on [`ObjectStore`].
    pub display: unsafe extern "C" fn(store: &Self) -> SString,

    /// Used to create a clone on the provider of the store. This should
    /// only need to be called by the receiver of the store.
    pub clone: unsafe extern "C" fn(store: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the store.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ObjectStore {}
unsafe impl Sync for FFI_ObjectStore {}

struct ObjectStorePrivateData {
    store: Arc<dyn ObjectStore>,
    runtime: Option<Handle>,
}

impl FFI_ObjectStore {
    fn private_data(&self) -> &ObjectStorePrivateData {
        unsafe { &*(self.private_data as *const ObjectStorePrivateData) }
    }

    fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.private_data().store
    }

    fn runtime(&self) -> &Option<Handle> {
        &self.private_data().runtime
    }

    /// Create a new [`FFI_ObjectStore`] from a local store.
    ///
    /// `runtime` is the tokio runtime handle of the providing library. Stores
    /// that spawn tasks or use timers require it when the future is driven by a
    /// foreign executor.
    pub fn new(store: Arc<dyn ObjectStore>, runtime: Option<Handle>) -> Self {
        // Re-export rather than double wrap when this store is itself a foreign
        // store being handed back toward its owning library. See
        // `foreign_store_handle` for why this is not a downcast.
        if let Some(handle) = foreign_store_handle(&store) {
            return handle;
        }

        Self::new_unchecked(store, runtime)
    }

    /// Wrap `store` without checking whether it is a foreign store being sent
    /// home. Used by `clone`, where the handle is already known to be the right
    /// one and re-running the check would take the `FOREIGN_STORES` lock a
    /// second time on the same thread.
    fn new_unchecked(store: Arc<dyn ObjectStore>, runtime: Option<Handle>) -> Self {
        Self {
            put_opts: put_opts_fn_wrapper,
            put_multipart_opts: put_multipart_opts_fn_wrapper,
            get_opts: get_opts_fn_wrapper,
            get_ranges: get_ranges_fn_wrapper,
            delete_stream: delete_stream_fn_wrapper,
            list: list_fn_wrapper,
            list_with_offset: list_with_offset_fn_wrapper,
            list_with_delimiter: list_with_delimiter_fn_wrapper,
            copy_opts: copy_opts_fn_wrapper,
            rename_opts: rename_opts_fn_wrapper,
            display: display_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(ObjectStorePrivateData {
                store,
                runtime,
            })) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }

    /// If this store originated in the current library, return the underlying
    /// [`ObjectStore`] directly.
    ///
    /// This is the path that matters for a table provider that creates its own
    /// store, registers it with the session, and then reads through it during
    /// execution: the store makes a round trip through the registry but is
    /// unwrapped back to the original `Arc` at execution time, so no data
    /// actually crosses the FFI boundary.
    pub fn as_local(&self) -> Option<Arc<dyn ObjectStore>> {
        ((self.library_marker_id)() == crate::get_library_marker_id())
            .then(|| Arc::clone(self.inner()))
    }
}

fn path_from(location: &SString) -> Path {
    Path::from(location.to_string())
}

fn prefix_from(prefix: FFI_Option<SString>) -> Option<Path> {
    prefix.into_option().map(|p| path_from(&p))
}

unsafe extern "C" fn put_opts_fn_wrapper(
    store: &FFI_ObjectStore,
    location: SString,
    payload: SVec<FFI_Bytes>,
    options: FFI_PutOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>> {
    let inner = Arc::clone(store.inner());
    async move {
        let result = inner
            .put_opts(
                &path_from(&location),
                put_payload_from_ffi(payload),
                options.into(),
            )
            .await;
        FFI_ObjectStoreResult::from(result.map(|r| FFI_PutResult::from(&r)))
    }
    .into_ffi()
}

unsafe extern "C" fn put_multipart_opts_fn_wrapper(
    store: &FFI_ObjectStore,
    location: SString,
    options: FFI_PutMultipartOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_MultipartUpload>> {
    let inner = Arc::clone(store.inner());
    let runtime = store.runtime().clone();
    async move {
        let result = inner
            .put_multipart_opts(&path_from(&location), options.into())
            .await;
        FFI_ObjectStoreResult::from(
            result.map(|upload| FFI_MultipartUpload::new(upload, runtime)),
        )
    }
    .into_ffi()
}

unsafe extern "C" fn get_opts_fn_wrapper(
    store: &FFI_ObjectStore,
    location: SString,
    options: FFI_GetOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_GetResult>> {
    let inner = Arc::clone(store.inner());
    let runtime = store.runtime().clone();
    async move {
        let result = inner.get_opts(&path_from(&location), options.into()).await;
        FFI_ObjectStoreResult::from(
            result.map(|result| FFI_GetResult::new(result, runtime)),
        )
    }
    .into_ffi()
}

unsafe extern "C" fn get_ranges_fn_wrapper(
    store: &FFI_ObjectStore,
    location: SString,
    ranges: SVec<FFI_ByteRange>,
) -> FfiFuture<FFI_ObjectStoreResult<SVec<FFI_Bytes>>> {
    let inner = Arc::clone(store.inner());
    async move {
        let ranges: Vec<Range<u64>> =
            ranges.into_iter().map(|r| r.start..r.end).collect();
        let result = inner.get_ranges(&path_from(&location), &ranges).await;
        FFI_ObjectStoreResult::from(
            result.map(|chunks| chunks.into_iter().map(FFI_Bytes::from).collect()),
        )
    }
    .into_ffi()
}

unsafe extern "C" fn delete_stream_fn_wrapper(
    store: &FFI_ObjectStore,
    locations: FFI_PathStream,
) -> FFI_PathStream {
    let deleted = store.inner().delete_stream(Box::pin(locations));
    FFI_PathStream::new(deleted, store.runtime().clone())
}

unsafe extern "C" fn list_fn_wrapper(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
) -> FFI_ObjectMetaStream {
    let stream = store.inner().list(prefix_from(prefix).as_ref());
    FFI_ObjectMetaStream::new(stream, store.runtime().clone())
}

unsafe extern "C" fn list_with_offset_fn_wrapper(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
    offset: SString,
) -> FFI_ObjectMetaStream {
    let stream = store
        .inner()
        .list_with_offset(prefix_from(prefix).as_ref(), &path_from(&offset));
    FFI_ObjectMetaStream::new(stream, store.runtime().clone())
}

unsafe extern "C" fn list_with_delimiter_fn_wrapper(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_ListResult>> {
    let inner = Arc::clone(store.inner());
    async move {
        let result = inner
            .list_with_delimiter(prefix_from(prefix).as_ref())
            .await;
        FFI_ObjectStoreResult::from(result.map(|r| FFI_ListResult::from(&r)))
    }
    .into_ffi()
}

unsafe extern "C" fn copy_opts_fn_wrapper(
    store: &FFI_ObjectStore,
    from: SString,
    to: SString,
    options: FFI_CopyOptions,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    let inner = Arc::clone(store.inner());
    async move {
        FFI_ObjectStoreResult::from(
            inner
                .copy_opts(&path_from(&from), &path_from(&to), options.into())
                .await,
        )
    }
    .into_ffi()
}

unsafe extern "C" fn rename_opts_fn_wrapper(
    store: &FFI_ObjectStore,
    from: SString,
    to: SString,
    options: FFI_RenameOptions,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    let inner = Arc::clone(store.inner());
    async move {
        FFI_ObjectStoreResult::from(
            inner
                .rename_opts(&path_from(&from), &path_from(&to), options.into())
                .await,
        )
    }
    .into_ffi()
}

unsafe extern "C" fn display_fn_wrapper(store: &FFI_ObjectStore) -> SString {
    store.inner().to_string().as_str().into()
}

unsafe extern "C" fn clone_fn_wrapper(store: &FFI_ObjectStore) -> FFI_ObjectStore {
    let private_data = store.private_data();
    FFI_ObjectStore::new_unchecked(
        Arc::clone(&private_data.store),
        private_data.runtime.clone(),
    )
}

unsafe extern "C" fn release_fn_wrapper(store: &mut FFI_ObjectStore) {
    unsafe {
        debug_assert!(!store.private_data.is_null());
        drop(Box::from_raw(
            store.private_data as *mut ObjectStorePrivateData,
        ));
        store.private_data = std::ptr::null_mut();
    }
}

impl Clone for FFI_ObjectStore {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_ObjectStore {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// Tracks the [`FFI_ObjectStore`] handle behind every live
/// [`ForeignObjectStore`], keyed by the address of the `ForeignObjectStore`
/// inside its `Arc`.
///
/// A store that crosses from library B into library A and is later handed back
/// toward B must arrive as B's original handle. Looking the handle up here lets
/// [`FFI_ObjectStore::new`] re-export it unchanged, so
/// [`FFI_ObjectStore::as_local`] succeeds in B and reads stay in B rather than
/// making a B -> A -> B round trip through the wrapper.
///
/// A downcast would express this more directly, but [`ObjectStore`] has no
/// `Any` supertrait and no `as_any` method, so a `dyn ObjectStore` cannot be
/// tested for a concrete type.
///
/// Two invariants keep this sound:
///
/// * Entries are removed in `Drop for ForeignObjectStore`, which runs before
///   the allocation is freed, so one address never maps to two live entries.
/// * Handles are held behind an `Arc` so a lookup can clone the `Arc` under the
///   lock and run [`FFI_ObjectStore::clone`] after releasing it. That clone
///   calls back into the owning library and re-enters
///   [`FFI_ObjectStore::new`]; running it under the lock would deadlock when
///   the owning library is this one, since [`std::sync::Mutex`] is not
///   reentrant.
static FOREIGN_STORES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<usize, Arc<FFI_ObjectStore>>>,
> = std::sync::OnceLock::new();

fn foreign_stores()
-> &'static std::sync::Mutex<std::collections::HashMap<usize, Arc<FFI_ObjectStore>>> {
    FOREIGN_STORES.get_or_init(Default::default)
}

/// If `store` is a [`ForeignObjectStore`], return a clone of the original
/// [`FFI_ObjectStore`] handle it wraps.
fn foreign_store_handle(store: &Arc<dyn ObjectStore>) -> Option<FFI_ObjectStore> {
    let key = Arc::as_ptr(store) as *const () as usize;

    let handle = {
        let stores = foreign_stores().lock().ok()?;
        Arc::clone(stores.get(&key)?)
        // Lock released here, before the cross-library clone below.
    };

    Some(handle.as_ref().clone())
}

/// An [`ObjectStore`] backed by a foreign [`FFI_ObjectStore`].
#[derive(Debug)]
pub struct ForeignObjectStore {
    pub(crate) store: FFI_ObjectStore,
}

unsafe impl Send for ForeignObjectStore {}
unsafe impl Sync for ForeignObjectStore {}

impl From<FFI_ObjectStore> for ForeignObjectStore {
    fn from(store: FFI_ObjectStore) -> Self {
        Self { store }
    }
}

impl Drop for ForeignObjectStore {
    fn drop(&mut self) {
        let key = std::ptr::from_ref::<Self>(self) as *const () as usize;
        let removed = foreign_stores()
            .lock()
            .ok()
            .and_then(|mut stores| stores.remove(&key));
        // Drop the handle after the lock is released: releasing it calls back
        // into the owning library, which must not happen under our lock.
        drop(removed);
    }
}

impl std::fmt::Display for ForeignObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = unsafe { (self.store.display)(&self.store) };
        write!(f, "{}", name.as_str())
    }
}

/// Convert an [`FFI_ObjectStore`] into an [`ObjectStore`], unwrapping it when
/// it originated in this library.
impl From<FFI_ObjectStore> for Arc<dyn ObjectStore> {
    fn from(store: FFI_ObjectStore) -> Self {
        if let Some(local) = store.as_local() {
            return local;
        }

        // Keep the original handle recoverable so that sending this store back
        // toward its owning library re-exports it rather than adding a second
        // wrapper. See `FOREIGN_STORES`.
        let handle = Arc::new(store.clone());
        let wrapper = Arc::new(ForeignObjectStore::from(store));
        let key = Arc::as_ptr(&wrapper) as *const () as usize;
        if let Ok(mut stores) = foreign_stores().lock() {
            stores.insert(key, handle);
        }

        wrapper
    }
}

fn path_to(path: &Path) -> SString {
    path.as_ref().into()
}

fn prefix_to(prefix: Option<&Path>) -> FFI_Option<SString> {
    match prefix {
        Some(p) => FFI_Option::Some(path_to(p)),
        None => FFI_Option::None,
    }
}

#[async_trait]
impl ObjectStore for ForeignObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let future = unsafe {
            (self.store.put_opts)(
                &self.store,
                path_to(location),
                put_payload_to_ffi(&payload),
                FFI_PutOptions::from(&opts),
            )
        };
        Result::<FFI_PutResult, _>::from(future.await).map(PutResult::from)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let future = unsafe {
            (self.store.put_multipart_opts)(
                &self.store,
                path_to(location),
                FFI_PutMultipartOptions::from(&opts),
            )
        };
        let upload = Result::<FFI_MultipartUpload, _>::from(future.await)?;
        Ok(Box::new(ForeignMultipartUpload::from(upload)))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let future = unsafe {
            (self.store.get_opts)(
                &self.store,
                path_to(location),
                FFI_GetOptions::from(&options),
            )
        };
        Result::<FFI_GetResult, _>::from(future.await).map(GetResult::from)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let ffi_ranges: SVec<FFI_ByteRange> = ranges
            .iter()
            .map(|r| FFI_ByteRange {
                start: r.start,
                end: r.end,
            })
            .collect();
        let future = unsafe {
            (self.store.get_ranges)(&self.store, path_to(location), ffi_ranges)
        };
        let chunks = Result::<SVec<FFI_Bytes>, _>::from(future.await)?;
        Ok(chunks.into_iter().map(Bytes::from).collect())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let deleted = unsafe {
            (self.store.delete_stream)(&self.store, FFI_PathStream::new(locations, None))
        };
        Box::pin(deleted)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let stream = unsafe { (self.store.list)(&self.store, prefix_to(prefix)) };
        Box::pin(stream)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let stream = unsafe {
            (self.store.list_with_offset)(&self.store, prefix_to(prefix), path_to(offset))
        };
        Box::pin(stream)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let future =
            unsafe { (self.store.list_with_delimiter)(&self.store, prefix_to(prefix)) };
        Result::<FFI_ListResult, _>::from(future.await).map(ListResult::from)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let future = unsafe {
            (self.store.copy_opts)(
                &self.store,
                path_to(from),
                path_to(to),
                FFI_CopyOptions::from(&options),
            )
        };
        future.await.into()
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let future = unsafe {
            (self.store.rename_opts)(
                &self.store,
                path_to(from),
                path_to(to),
                FFI_RenameOptions::from(&options),
            )
        };
        future.await.into()
    }
}
