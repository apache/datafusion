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

//! Object store that represents the HDFS File System.
use futures::{stream, StreamExt};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;

use hdfs::hdfs::{FileStatus, HdfsErr, HdfsFile, HdfsFs};

use crate::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use crate::error::{DataFusionError, Result};
use chrono::{NaiveDateTime, Utc};

pub mod os_parquet;

/// scheme for HDFS File System
pub static HDFS_SCHEME: &'static str = "hdfs";

/// Hadoop File.
#[derive(Clone)]
pub struct HadoopFile {
    inner: HdfsFile,
}

unsafe impl Send for HadoopFile {}

unsafe impl Sync for HadoopFile {}

/// Hadoop File System as Object Store.
#[derive(Clone)]
pub struct HadoopFileSystem {
    inner: HdfsFs,
}

impl HadoopFileSystem {
    /// Get HdfsFs based on the input path
    pub fn new(path: &str) -> Result<Self> {
        HdfsFs::new(path)
            .map(|fs| HadoopFileSystem { inner: fs })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Open a HdfsFile with specified path
    pub fn open(&self, path: &str) -> Result<HadoopFile> {
        self.inner
            .open(path)
            .map(|file| HadoopFile { inner: file })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Find out the files directly under a directory
    fn find_files_in_dir(
        &self,
        path: String,
        ext: &Option<String>,
        to_visit: &mut Vec<String>,
    ) -> Result<Vec<FileMeta>> {
        let mut files = Vec::new();

        let children = self
            .inner
            .list_status(path.as_str())
            .map_err(|e| to_error(e))?;
        for child in children {
            let child_path = child.name();
            if child.is_directory() {
                to_visit.push(child_path.to_string());
            } else {
                if ext.is_none() || child_path.ends_with(ext.as_ref().unwrap().as_str()) {
                    files.push(get_meta(child_path.to_owned(), child))
                }
            }
        }

        Ok(files)
    }
}

impl Debug for HadoopFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    fn get_schema(&self) -> &'static str {
        HDFS_SCHEME
    }

    async fn list_file(
        &self,
        prefix: &str,
        ext: Option<String>,
    ) -> Result<FileMetaStream> {
        // TODO list all of the files under a directory, including directly and indirectly
        let files = self.find_files_in_dir(prefix.to_string(), &ext, &mut Vec::new())?;
        get_files_in_dir(files).await
    }

    async fn list_dir(
        &self,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader_from_path(&self, file_path: &str) -> Result<Arc<dyn ObjectReader>> {
        let file_status = self
            .inner
            .get_file_status(file_path)
            .map_err(|e| to_error(e))?;
        if file_status.is_file() {
            self.file_reader(get_meta(String::from(file_path), file_status))
        } else {
            Err(DataFusionError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Path of {:?} is not for file", file_path),
            )))
        }
    }

    fn file_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(HadoopFileReader::new(file)?))
    }
}

async fn get_files_in_dir(files: Vec<FileMeta>) -> Result<FileMetaStream> {
    Ok(Box::pin(stream::iter(files).map(Ok)))
}

fn get_meta(path: String, file_status: FileStatus) -> FileMeta {
    FileMeta {
        path,
        last_modified: Some(chrono::DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file_status.last_modified(), 0),
            Utc,
        )),
        size: file_status.len() as u64,
    }
}

struct HadoopFileReader {
    file: Arc<FileMeta>,
}

impl HadoopFileReader {
    fn new(file: FileMeta) -> Result<Self> {
        Ok(Self {
            file: Arc::new(file),
        })
    }
}

#[async_trait]
impl ObjectReader for HadoopFileReader {
    fn get_file_meta(&self) -> Arc<FileMeta> {
        self.file.clone()
    }

    async fn chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> Result<Arc<dyn AsyncRead>> {
        todo!()
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

fn to_error(err: HdfsErr) -> std::io::Error {
    match err {
        HdfsErr::FileNotFound(err_str) => {
            std::io::Error::new(std::io::ErrorKind::NotFound, err_str.as_str())
        }
        HdfsErr::FileAlreadyExists(err_str) => {
            std::io::Error::new(std::io::ErrorKind::AlreadyExists, err_str.as_str())
        }
        HdfsErr::CannotConnectToNameNode(err_str) => {
            std::io::Error::new(std::io::ErrorKind::NotConnected, err_str.as_str())
        }
        HdfsErr::InvalidUrl(err_str) => {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err_str.as_str())
        }
        HdfsErr::Unknown => std::io::Error::new(std::io::ErrorKind::Other, "Unknown"),
    }
}
