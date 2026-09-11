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

//! Adapter for the Arrow `object_store` crate.
use ::object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_storage::path::Path;
use datafusion_storage::*;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use std::{ops::Range, sync::Arc};

/// Wrap an existing client; credentials, pools, and middleware stay on that client.
#[derive(Debug)]
pub struct ObjectStoreStorage(pub Arc<dyn ObjectStore>);

impl ObjectStoreStorage {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self(store)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn local() -> Self {
        Self(Arc::new(::object_store::local::LocalFileSystem::new()))
    }
}

fn error(error: ::object_store::Error) -> Error {
    match error {
        ::object_store::Error::NotFound { path, .. } => Error::NotFound(path),
        other => Error::Backend {
            backend: "object_store",
            source: Box::new(other),
        },
    }
}

fn location(path: &Path) -> Result<::object_store::path::Path> {
    ::object_store::path::Path::parse(path.as_ref()).map_err(|e| error(e.into()))
}

fn get_range(range: ReadRange) -> GetRange {
    match range {
        ReadRange::Bounded(range) => GetRange::Bounded(range),
        ReadRange::Suffix(size) => GetRange::Suffix(size),
    }
}

#[async_trait]
impl Storage for ObjectStoreStorage {
    async fn open(
        &self,
        path: &Path,
        _context: FileAccessContext,
    ) -> Result<Arc<dyn FileReader>> {
        Ok(Arc::new(Reader {
            store: self.0.clone(),
            path: location(path)?,
        }))
    }

    async fn stat(&self, path: &Path, _context: &FileAccessContext) -> Result<FileInfo> {
        Ok(file_info(
            self.0.head(&location(path)?).await.map_err(error)?,
        ))
    }

    async fn list_with_delimiter(
        &self,
        prefix: &Path,
        _context: &FileAccessContext,
    ) -> Result<DirectoryListing> {
        let result = self
            .0
            .list_with_delimiter(Some(&location(prefix)?))
            .await
            .map_err(error)?;
        Ok(DirectoryListing {
            files: result.objects.into_iter().map(file_info).collect(),
            directories: result
                .common_prefixes
                .into_iter()
                .map(|path| Path::parse(path.as_ref()))
                .collect::<std::result::Result<_, _>>()?,
        })
    }

    fn list(
        &self,
        prefix: &Path,
        _context: FileAccessContext,
    ) -> BoxStream<'_, Result<FileInfo>> {
        let path = match location(prefix) {
            Ok(path) => path,
            Err(e) => return futures::stream::once(async { Err(e) }).boxed(),
        };
        self.0
            .list(Some(&path))
            .map(|meta| meta.map(file_info).map_err(error))
            .boxed()
    }

    async fn writer(
        &self,
        path: &Path,
        options: WriterOptions,
        _context: FileAccessContext,
    ) -> Result<FileOutput> {
        let path = location(path)?;
        let writer = match options.buffer_size {
            Some(size) => ::object_store::buffered::BufWriter::with_capacity(
                self.0.clone(),
                path,
                size,
            ),
            None => ::object_store::buffered::BufWriter::new(self.0.clone(), path),
        };
        Ok(Box::new(writer))
    }
}

#[derive(Debug)]
struct Reader {
    store: Arc<dyn ObjectStore>,
    path: ::object_store::path::Path,
}

#[async_trait]
impl FileReader for Reader {
    async fn read_range(&self, range: ReadRange) -> Result<Bytes> {
        match range {
            ReadRange::Bounded(range) => {
                self.store.get_range(&self.path, range).await.map_err(error)
            }
            range @ ReadRange::Suffix(_) => self
                .store
                .get_opts(
                    &self.path,
                    GetOptions {
                        range: Some(get_range(range)),
                        ..Default::default()
                    },
                )
                .await
                .map_err(error)?
                .bytes()
                .await
                .map_err(error),
        }
    }

    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.store
            .get_ranges(&self.path, &ranges)
            .await
            .map_err(error)
    }

    fn stream(
        self: Arc<Self>,
        range: Option<ReadRange>,
    ) -> BoxStream<'static, Result<Bytes>> {
        futures::stream::once(async move {
            Ok::<_, Error>(
                self.store
                    .get_opts(
                        &self.path,
                        GetOptions {
                            range: range.map(get_range),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(error)?
                    .into_stream()
                    .map_err(error),
            )
        })
        .try_flatten()
        .boxed()
    }
}

/// Convert metadata at the SDK boundary, retaining both ETag and version.
pub fn file_info(meta: ::object_store::ObjectMeta) -> FileInfo {
    FileInfo {
        location: Path::parse(meta.location.as_ref()).expect("validated SDK path"),
        size: meta.size,
        last_modified: meta.last_modified,
        e_tag: meta.e_tag,
        version: meta.version,
    }
}
