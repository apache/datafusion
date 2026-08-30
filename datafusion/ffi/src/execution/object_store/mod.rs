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

//! FFI support for [`object_store::ObjectStore`] and
//! [`datafusion_execution::object_store::ObjectStoreRegistry`].
//!
//! # Sharing the registry
//!
//! A table provider shared over FFI is asked to `scan` by a session in another
//! library, and the [`ExecutionPlan`] it returns is later executed with a
//! [`TaskContext`] produced by that same session. Both sides need to agree on
//! which object store serves a given URL. Sharing the registry is what makes
//! that agreement possible: a store registered by the provider during planning
//! is visible to the session at execution time, and a store registered on the
//! session by the host is visible to the provider.
//!
//! # The local fast path
//!
//! A provider that creates its own store, registers it, and then reads through
//! it during execution never actually moves data across the boundary. The store
//! makes a round trip through the foreign registry but
//! [`FFI_ObjectStore::as_local`] recovers the original `Arc<dyn ObjectStore>`,
//! so reads run at full speed. Only a store genuinely owned by the *other*
//! library is driven through the wrapper.
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan
//! [`TaskContext`]: datafusion_execution::TaskContext

mod buffer;
mod error;
mod multipart;
mod registry;
mod store;
mod stream;
mod types;

pub use buffer::FFI_Bytes;
pub use error::{FFI_ObjectStoreError, FFI_ObjectStoreErrorKind, FFI_ObjectStoreResult};
pub use multipart::{FFI_MultipartUpload, ForeignMultipartUpload};
pub use registry::{FFI_ObjectStoreRegistry, ForeignObjectStoreRegistry};
pub use store::{FFI_ByteRange, FFI_GetResult, FFI_ObjectStore, ForeignObjectStore};
pub use stream::{FFI_BytesStream, FFI_ObjectMetaStream, FFI_PathStream};
pub use types::{
    FFI_Attribute, FFI_CopyOptions, FFI_GetOptions, FFI_GetRange, FFI_ListResult,
    FFI_ObjectMeta, FFI_PutMode, FFI_PutMultipartOptions, FFI_PutOptions, FFI_PutResult,
    FFI_RenameOptions, FFI_Timestamp,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
        PutPayload,
    };

    use super::*;

    /// Wrap a store and force the foreign code path so the FFI functions are
    /// exercised rather than the local unwrap.
    fn foreign_store(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let mut ffi = FFI_ObjectStore::new(store, None);
        ffi.library_marker_id = crate::mock_foreign_marker_id;
        Arc::new(ForeignObjectStore::from(ffi))
    }

    #[tokio::test]
    async fn put_and_get_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("a/b.parquet");

        store
            .put(&path, PutPayload::from_static(b"hello ffi"))
            .await?;

        let result = store.get(&path).await?;
        assert_eq!(result.bytes().await?, Bytes::from_static(b"hello ffi"));

        Ok(())
    }

    #[tokio::test]
    async fn get_ranges_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("data.bin");
        store
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await?;

        let ranges = store.get_ranges(&path, &[0..3, 5..8]).await?;
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], Bytes::from_static(b"012"));
        assert_eq!(ranges[1], Bytes::from_static(b"567"));

        Ok(())
    }

    #[tokio::test]
    async fn get_range_option_is_honored() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("data.bin");
        store
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await?;

        let bytes = store.get_range(&path, 2..6).await?;
        assert_eq!(bytes, Bytes::from_static(b"2345"));

        Ok(())
    }

    #[tokio::test]
    async fn head_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("a/b.parquet");
        store.put(&path, PutPayload::from_static(b"12345")).await?;

        let meta = store.head(&path).await?;
        assert_eq!(meta.location, path);
        assert_eq!(meta.size, 5);

        Ok(())
    }

    #[tokio::test]
    async fn list_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        for name in ["p/1.parquet", "p/2.parquet", "q/3.parquet"] {
            store
                .put(&Path::from(name), PutPayload::from_static(b"x"))
                .await?;
        }

        let mut listed: Vec<String> = store
            .list(Some(&Path::from("p")))
            .map(|m| m.unwrap().location.to_string())
            .collect()
            .await;
        listed.sort();

        assert_eq!(listed, vec!["p/1.parquet", "p/2.parquet"]);

        Ok(())
    }

    #[tokio::test]
    async fn list_with_delimiter_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        for name in ["p/1.parquet", "p/sub/2.parquet"] {
            store
                .put(&Path::from(name), PutPayload::from_static(b"x"))
                .await?;
        }

        let result = store.list_with_delimiter(Some(&Path::from("p"))).await?;
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].location, Path::from("p/1.parquet"));
        assert_eq!(result.common_prefixes, vec![Path::from("p/sub")]);

        Ok(())
    }

    #[tokio::test]
    async fn delete_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("gone.parquet");
        store.put(&path, PutPayload::from_static(b"x")).await?;
        store.delete(&path).await?;

        assert!(matches!(
            store.head(&path).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn copy_and_rename_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let src = Path::from("src.parquet");
        store.put(&src, PutPayload::from_static(b"payload")).await?;

        let copied = Path::from("copied.parquet");
        store.copy(&src, &copied).await?;
        assert_eq!(store.get(&copied).await?.bytes().await?, "payload");

        let renamed = Path::from("renamed.parquet");
        store.rename(&copied, &renamed).await?;
        assert!(matches!(
            store.head(&copied).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));
        assert_eq!(store.get(&renamed).await?.bytes().await?, "payload");

        Ok(())
    }

    /// `copy_if_not_exists` is an extension method built on `copy_opts` with
    /// `CopyMode::Create`. Its atomicity depends on the mode surviving the
    /// boundary and on `AlreadyExists` being reported as that variant.
    #[tokio::test]
    async fn copy_if_not_exists_preserves_already_exists() -> datafusion_common::Result<()>
    {
        let store = foreign_store(Arc::new(InMemory::new()));
        let src = Path::from("src.parquet");
        let dst = Path::from("dst.parquet");
        store.put(&src, PutPayload::from_static(b"a")).await?;
        store.put(&dst, PutPayload::from_static(b"b")).await?;

        let err = store.copy_if_not_exists(&src, &dst).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::AlreadyExists { .. }),
            "expected AlreadyExists, got {err:?}"
        );

        Ok(())
    }

    /// `PutMode::Create` underpins optimistic concurrency control in commit
    /// protocols such as Delta's. A conflicting write must surface as
    /// `AlreadyExists`, not as a generic error.
    #[tokio::test]
    async fn put_mode_create_preserves_already_exists() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("_delta_log/00000000000000000001.json");

        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        store
            .put_opts(&path, PutPayload::from_static(b"first"), opts.clone())
            .await?;

        let err = store
            .put_opts(&path, PutPayload::from_static(b"second"), opts)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::AlreadyExists { .. }),
            "expected AlreadyExists, got {err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn not_found_preserves_variant() {
        let store = foreign_store(Arc::new(InMemory::new()));
        let err = store.get(&Path::from("missing")).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "expected NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn multipart_upload_round_trip() -> datafusion_common::Result<()> {
        let store = foreign_store(Arc::new(InMemory::new()));
        let path = Path::from("multipart.bin");

        let mut upload = store.put_multipart(&path).await?;
        upload.put_part(PutPayload::from(vec![1u8; 8])).await?;
        upload.put_part(PutPayload::from(vec![2u8; 8])).await?;
        upload.complete().await?;

        let bytes = store.get(&path).await?.bytes().await?;
        assert_eq!(bytes.len(), 16);
        assert_eq!(&bytes[..8], &[1u8; 8]);
        assert_eq!(&bytes[8..], &[2u8; 8]);

        Ok(())
    }

    /// A store owned by this library must be handed back unchanged rather than
    /// wrapped, so that reads never cross the boundary.
    #[test]
    fn local_store_is_unwrapped() {
        let original = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let ffi = FFI_ObjectStore::new(Arc::clone(&original), None);

        let recovered = ffi.as_local().expect("local store should unwrap");
        assert!(Arc::ptr_eq(&original, &recovered));
    }

    /// A store that has crossed into another library and is then sent back
    /// arrives as the original handle rather than a second wrapper, keeping
    /// reads on the local fast path.
    ///
    /// The check is on store identity rather than `library_marker_id`, because
    /// `clone` regenerates the marker from whichever library runs it. That is
    /// correct across a real boundary, where the clone executes in the owning
    /// library, but means a mocked marker does not survive a clone.
    #[test]
    fn foreign_store_round_trips_back_to_original() {
        let original = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Cross into a "foreign" library.
        let mut ffi = FFI_ObjectStore::new(Arc::clone(&original), None);
        ffi.library_marker_id = crate::mock_foreign_marker_id;
        let wrapped = Arc::<dyn ObjectStore>::from(ffi);
        assert!(
            !Arc::ptr_eq(&wrapped, &original),
            "crossing the boundary should produce a wrapper"
        );

        // Send it back the other way.
        let returned = FFI_ObjectStore::new(Arc::clone(&wrapped), None);
        let recovered = returned
            .as_local()
            .expect("a store sent home should be recognised as local");

        assert!(
            Arc::ptr_eq(&recovered, &original),
            "sending a store home should recover the original store, not wrap the wrapper"
        );
    }

    /// The re-export must not survive the wrapper: once the `ForeignObjectStore`
    /// is dropped its side-table entry has to go, otherwise a later store
    /// allocated at the same address would be mistaken for it.
    #[test]
    fn dropped_wrapper_is_forgotten() {
        let original = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let mut ffi = FFI_ObjectStore::new(Arc::clone(&original), None);
        ffi.library_marker_id = crate::mock_foreign_marker_id;

        let wrapped = Arc::<dyn ObjectStore>::from(ffi);
        let key = Arc::as_ptr(&wrapped) as *const () as usize;
        drop(wrapped);

        let unrelated = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let handle = FFI_ObjectStore::new(Arc::clone(&unrelated), None);
        let recovered = handle.as_local().expect("local store should unwrap");
        assert!(
            Arc::ptr_eq(&recovered, &unrelated),
            "a stale side-table entry at address {key:#x} leaked into a new store"
        );
    }
}
