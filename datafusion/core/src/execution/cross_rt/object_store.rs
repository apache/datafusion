use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore, PutMultipartOpts, PutOptions,
    PutPayload, PutResult, Result,
};
use std::sync::Arc;

use super::spawn_io;

/// An object store that uses [`spawn_io`] to move IO operations to the IO thread.
pub struct CrossRtObjectStore {
    pub store: Arc<dyn ObjectStore>,
}

impl CrossRtObjectStore {
    /// Create a new `CrossRtObjectStore` wrapping the given `ObjectStore`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ObjectStore for CrossRtObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.put(&location, payload).await }).await
    }

    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.put_opts(&location, payload, opts).await }).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.put_multipart(&location).await }).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOpts) -> Result<Box<dyn MultipartUpload>> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.put_multipart_opts(&location, opts).await }).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.get(&location).await }).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.get_opts(&location, options).await }).await
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<usize>) -> Result<Bytes> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.get_range(&location, range).await }).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[std::ops::Range<usize>]) -> Result<Vec<Bytes>> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        let ranges = ranges.to_vec();
        spawn_io(async move { store.get_ranges(&location, &ranges).await }).await
    }

    async fn head(&self, location: &Path) -> Result<object_store::ObjectMeta> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.head(&location).await }).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let store = Arc::clone(&self.store);
        let location = location.clone();
        spawn_io(async move { store.delete(&location).await }).await
    }

    fn delete_stream<'a>(&'a self, locations: BoxStream<'a, Result<Path>>) -> BoxStream<'a, Result<Path>> {
        self.store.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<object_store::ObjectMeta>> {
        self.store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<object_store::ObjectMeta>> {
        self.store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let store = Arc::clone(&self.store);
        let prefix = prefix.cloned();
        spawn_io(async move { store.list_with_delimiter(prefix.as_ref()).await }).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.store);
        let from = from.clone();
        let to = to.clone();
        spawn_io(async move { store.copy(&from, &to).await }).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.store);
        let from = from.clone();
        let to = to.clone();
        spawn_io(async move { store.rename(&from, &to).await }).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.store);
        let from = from.clone();
        let to = to.clone();
        spawn_io(async move { store.copy_if_not_exists(&from, &to).await }).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.store);
        let from = from.clone();
        let to = to.clone();
        spawn_io(async move { store.rename_if_not_exists(&from, &to).await }).await
    }
}

impl std::fmt::Display for CrossRtObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrossRtObjectStore({})", self.store)
    }
}

impl std::fmt::Debug for CrossRtObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossRtObjectStore")
            .field("store", &self.store)
            .finish()
    }
}
