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

//! Object store implem used for testing

use std::{
    fs, io,
    io::{Cursor, Read},
    sync::Arc,
};

use crate::datasource::object_store::{
    local::LocalFileSystem, FileMeta, FileMetaStream, ListEntryStream, ObjectReader,
    ObjectStore, SizedFile,
};
use crate::error::{DataFusionError, Result};

use async_trait::async_trait;
use futures::{stream, AsyncRead, StreamExt};

#[derive(Debug)]
/// An object store implem that is useful for testing.
/// Can either generate `ObjectReader`s that are filled with zero-
/// bytes or mirror a given file to multiple path.
pub struct TestObjectStore {
    /// The `(path,size)` of the files that "exist" in the store
    files: Vec<(String, u64)>,
    /// The file that will be read at all path. If none fille the
    /// file with zero-bytes.
    mirrored_file: Option<String>,
}

impl TestObjectStore {
    pub fn new_arc(files: &[(&str, u64)]) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            files: files.iter().map(|f| (f.0.to_owned(), f.1)).collect(),
            mirrored_file: None,
        })
    }
    pub fn new_mirror(mirrored_file: String, paths: &[&str]) -> Arc<dyn ObjectStore> {
        let metadata = fs::metadata(&mirrored_file).expect("Local file metadata");
        Arc::new(Self {
            files: paths
                .iter()
                .map(|&f| (f.to_owned(), metadata.len()))
                .collect(),
            mirrored_file: Some(mirrored_file),
        })
    }
}

#[async_trait]
impl ObjectStore for TestObjectStore {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let prefix = prefix.to_owned();
        Ok(Box::pin(
            stream::iter(
                self.files
                    .clone()
                    .into_iter()
                    .filter(move |f| f.0.starts_with(&prefix)),
            )
            .map(|f| {
                Ok(FileMeta {
                    sized_file: SizedFile {
                        path: f.0.clone(),
                        size: f.1,
                    },
                    last_modified: None,
                })
            }),
        ))
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        unimplemented!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        match self.files.iter().find(|item| file.path == item.0) {
            Some(&(_, size)) if size == file.size => {
                if let Some(mirrored_file) = &self.mirrored_file {
                    Ok(LocalFileSystem {}.file_reader(SizedFile {
                        path: mirrored_file.clone(),
                        size,
                    })?)
                } else {
                    Ok(Arc::new(EmptyObjectReader(size)))
                }
            }
            Some(_) => Err(DataFusionError::IoError(io::Error::new(
                io::ErrorKind::NotFound,
                "found in test list but wrong size",
            ))),
            None => Err(DataFusionError::IoError(io::Error::new(
                io::ErrorKind::NotFound,
                "not in provided test list",
            ))),
        }
    }
}

struct EmptyObjectReader(u64);

#[async_trait]
impl ObjectReader for EmptyObjectReader {
    async fn chunk_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Box<dyn AsyncRead>> {
        unimplemented!()
    }

    fn sync_chunk_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        Ok(Box::new(Cursor::new(vec![0; self.0 as usize])))
    }

    fn length(&self) -> u64 {
        self.0
    }
}
