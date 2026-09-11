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

//! Direct OpenDAL adapter. No ObjectStore bridge is involved.
use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use datafusion_storage::{path::Path, *};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use std::{fmt::Debug, ops::Range, sync::Arc};
use tokio_util::compat::FuturesAsyncWriteCompatExt;

/// Wrap an existing operator, including its layers and credential configuration.
#[derive(Debug)]
pub struct OpendalStorage(pub ::opendal::Operator);

impl OpendalStorage {
    pub fn new(operator: ::opendal::Operator) -> Self {
        Self(operator)
    }
}

fn error(error: ::opendal::Error) -> Error {
    match error.kind() {
        ::opendal::ErrorKind::NotFound => Error::NotFound(error.to_string()),
        _ => Error::Backend {
            backend: "opendal",
            source: Box::new(error),
        },
    }
}

fn file_info(path: Path, meta: &::opendal::Metadata) -> FileInfo {
    FileInfo {
        location: path,
        size: meta.content_length(),
        last_modified: meta
            .last_modified()
            .and_then(|t| {
                DateTime::from_timestamp(
                    t.into_inner().as_second(),
                    t.into_inner().subsec_nanosecond() as u32,
                )
            })
            .unwrap_or(DateTime::UNIX_EPOCH),
        e_tag: meta.etag().map(String::from),
        version: meta.version().map(String::from),
    }
}

#[async_trait]
impl Storage for OpendalStorage {
    async fn open(
        &self,
        path: &Path,
        _context: FileAccessContext,
    ) -> Result<Arc<dyn FileReader>> {
        Ok(Arc::new(Reader {
            inner: self.0.reader(path.as_ref()).await.map_err(error)?,
            operator: self.0.clone(),
            path: path.clone(),
        }))
    }

    async fn stat(&self, path: &Path, _context: &FileAccessContext) -> Result<FileInfo> {
        Ok(file_info(
            path.clone(),
            &self.0.stat(path.as_ref()).await.map_err(error)?,
        ))
    }

    async fn list_with_delimiter(
        &self,
        prefix: &Path,
        context: &FileAccessContext,
    ) -> Result<DirectoryListing> {
        let mut entries = self
            .0
            .lister(&directory_path(prefix))
            .await
            .map_err(error)?;
        let mut result = DirectoryListing::default();
        while let Some(entry) = entries.try_next().await.map_err(error)? {
            let path = Path::parse(entry.path())?;
            if entry.metadata().is_dir() {
                if &path != prefix {
                    result.directories.push(path);
                }
            } else {
                result.files.push(self.stat(&path, context).await?);
            }
        }
        Ok(result)
    }

    fn list(
        &self,
        prefix: &Path,
        context: FileAccessContext,
    ) -> BoxStream<'_, Result<FileInfo>> {
        let path = directory_path(prefix);
        futures::stream::once(async move {
            Ok::<_, Error>(
                self.0
                    .lister_with(&path)
                    .recursive(true)
                    .await
                    .map_err(error)?
                    .map_err(error),
            )
        })
        .try_flatten()
        .try_filter(|entry| futures::future::ready(entry.metadata().is_file()))
        .and_then(move |entry| {
            let context = context.clone();
            async move { self.stat(&Path::parse(entry.path())?, &context).await }
        })
        .boxed()
    }

    async fn writer(
        &self,
        path: &Path,
        options: WriterOptions,
        _context: FileAccessContext,
    ) -> Result<FileOutput> {
        let mut builder = self.0.writer_with(path.as_ref());
        if let Some(size) = options.buffer_size {
            builder = builder.chunk(size);
        }
        Ok(Box::new(
            builder
                .await
                .map_err(error)?
                .into_futures_async_write()
                .compat_write(),
        ))
    }
}

struct Reader {
    inner: ::opendal::Reader,
    operator: ::opendal::Operator,
    path: Path,
}
impl Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalReader")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
impl Reader {
    async fn range(&self, range: ReadRange) -> Result<Range<u64>> {
        match range {
            ReadRange::Bounded(range) => Ok(range),
            ReadRange::Suffix(length) => {
                // OpenDAL reads offset ranges. Resolve size only for suffix requests.
                let size = match self.inner.metadata() {
                    Some(meta) => meta.content_length(),
                    None => self
                        .operator
                        .stat(self.path.as_ref())
                        .await
                        .map_err(error)?
                        .content_length(),
                };
                Ok(size.saturating_sub(length)..size)
            }
        }
    }
}
#[async_trait]
impl FileReader for Reader {
    async fn read_range(&self, range: ReadRange) -> Result<Bytes> {
        Ok(self
            .inner
            .read(self.range(range).await?)
            .await
            .map_err(error)?
            .to_bytes())
    }
    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        Ok(self
            .inner
            .fetch(ranges)
            .await
            .map_err(error)?
            .into_iter()
            .map(|b| b.to_bytes())
            .collect())
    }
    fn stream(
        self: Arc<Self>,
        range: Option<ReadRange>,
    ) -> BoxStream<'static, Result<Bytes>> {
        futures::stream::once(async move {
            let stream = match range {
                Some(range) => {
                    self.inner
                        .clone()
                        .into_bytes_stream(self.range(range).await?)
                        .await
                }
                None => self.inner.clone().into_bytes_stream(..).await,
            }
            .map_err(error)?;
            Ok::<_, Error>(stream.map_err(Error::Io))
        })
        .try_flatten()
        .boxed()
    }
}

fn directory_path(path: &Path) -> String {
    if path.as_ref().is_empty() {
        String::new()
    } else {
        format!("{path}/")
    }
}
