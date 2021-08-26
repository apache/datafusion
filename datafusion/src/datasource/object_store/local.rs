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

//! Object store that represents the Local File System.

use std::fs::Metadata;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, AsyncRead, StreamExt};

use crate::datasource::object_store::{
    FileMeta, FileMetaStream, ObjectReader, ObjectStore,
};
use crate::error::DataFusionError;
use crate::error::Result;

#[derive(Debug)]
/// Local File System as Object Store.
pub struct LocalFileSystem;

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn list(&self, prefix: &str) -> Result<FileMetaStream> {
        list_all(prefix.to_owned()).await
    }

    async fn get_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(LocalFileReader::new(file)?))
    }
}

struct LocalFileReader {
    file: FileMeta,
}

impl LocalFileReader {
    fn new(file: FileMeta) -> Result<Self> {
        Ok(Self { file })
    }
}

#[async_trait]
impl ObjectReader for LocalFileReader {
    async fn get_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Arc<dyn AsyncRead>> {
        todo!()
    }

    async fn length(&self) -> Result<u64> {
        match self.file.size {
            Some(size) => Ok(size),
            None => Ok(0u64),
        }
    }
}

async fn list_all(prefix: String) -> Result<FileMetaStream> {
    fn get_meta(path: String, metadata: Metadata) -> FileMeta {
        FileMeta {
            path,
            last_modified: metadata.modified().map(chrono::DateTime::from).ok(),
            size: Some(metadata.len()),
        }
    }

    async fn find_files_in_dir(
        path: String,
        to_visit: &mut Vec<String>,
    ) -> Result<Vec<FileMeta>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut files = Vec::new();

        while let Some(child) = dir.next_entry().await? {
            if let Some(child_path) = child.path().to_str() {
                let metadata = child.metadata().await?;
                if metadata.is_dir() {
                    to_visit.push(child_path.to_string());
                } else {
                    files.push(get_meta(child_path.to_owned(), metadata))
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
        Ok(files)
    }

    let prefix_meta = tokio::fs::metadata(&prefix).await?;
    let prefix = prefix.to_owned();
    if prefix_meta.is_file() {
        Ok(Box::pin(stream::once(async move {
            Ok(get_meta(prefix, prefix_meta))
        })))
    } else {
        let result = stream::unfold(vec![prefix], move |mut to_visit| async move {
            match to_visit.pop() {
                None => None,
                Some(path) => {
                    let file_stream = match find_files_in_dir(path, &mut to_visit).await {
                        Ok(files) => stream::iter(files).map(Ok).left_stream(),
                        Err(e) => stream::once(async { Err(e) }).right_stream(),
                    };

                    Some((file_stream, to_visit))
                }
            }
        })
        .flatten();
        Ok(Box::pin(result))
    }
}
