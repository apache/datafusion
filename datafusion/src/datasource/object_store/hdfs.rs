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
use std::fmt::Debug;
use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use futures::AsyncRead;
use futures::{stream, StreamExt};
use hdfs::hdfs::{FileStatus, HdfsErr, HdfsFile, HdfsFs};

use crate::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectReaderStream,
    ObjectStore, SizedFile,
};
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};

/// scheme for HDFS File System
pub static HDFS_SCHEME: &'static str = "hdfs";

/// Hadoop File.
#[derive(Clone, Debug)]
pub struct HadoopFile {
    inner: HdfsFile,
}

unsafe impl Send for HadoopFile {}

unsafe impl Sync for HadoopFile {}

impl Read for HadoopFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner
            .read(buf)
            .map(|read_len| read_len as usize)
            .map_err(|e| to_error(e))
    }
}

/// Hadoop File System as Object Store.
#[derive(Clone, Debug)]
pub struct HadoopFileSystem {
    inner: HdfsFs,
}

impl HadoopFileSystem {
    /// Get HdfsFs configured by default configuration files
    pub fn new() -> Result<Self> {
        HdfsFs::default()
            .map(|fs| HadoopFileSystem { inner: fs })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Wrap the known HdfsFs. Only used for testing
    pub fn wrap(inner: HdfsFs) -> Self {
        HadoopFileSystem { inner }
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
                files.push(get_meta(child_path.to_owned(), child));
            }
        }

        Ok(files)
    }
}

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    fn get_relative_path<'a>(&self, uri: &'a str) -> &'a str {
        let mut result = uri;
        if let Some((scheme, path_without_schema)) = uri.split_once("://") {
            assert_eq!(scheme, HDFS_SCHEME);
            let start_index = path_without_schema.find("/").unwrap();
            let (_host_address, path) = path_without_schema.split_at(start_index);
            result = path;
        }
        result
    }

    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        // TODO list all of the files under a directory, including directly and indirectly
        let files = self.find_files_in_dir(prefix.to_string(), &mut Vec::new())?;
        get_files_in_dir(files).await
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(HadoopFileReader::new(
            Arc::new(self.clone()),
            file,
        )?))
    }
}

async fn get_files_in_dir(files: Vec<FileMeta>) -> Result<FileMetaStream> {
    Ok(Box::pin(stream::iter(files).map(Ok)))
}

fn get_meta(path: String, file_status: FileStatus) -> FileMeta {
    FileMeta {
        sized_file: SizedFile {
            path,
            size: file_status.len() as u64,
        },
        last_modified: Some(chrono::DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file_status.last_modified(), 0),
            Utc,
        )),
    }
}

/// Create a stream of `ObjectReader` by converting each file in the `files` vector
/// into instances of `HadoopFileReader`
pub fn hadoop_object_reader_stream(
    fs: Arc<HadoopFileSystem>,
    files: Vec<String>,
) -> ObjectReaderStream {
    Box::pin(
        futures::stream::iter(files)
            .map(move |f| Ok(hadoop_object_reader(fs.clone(), f))),
    )
}

/// Helper method to convert a file location to a `LocalFileReader`
pub fn hadoop_object_reader(
    fs: Arc<HadoopFileSystem>,
    file: String,
) -> Arc<dyn ObjectReader> {
    fs.file_reader(
        hadoop_unpartitioned_file(fs.clone(), file)
            .file_meta
            .sized_file,
    )
    .expect("File not found")
}

/// Helper method to fetch the file size and date at given path and create a `FileMeta`
pub fn hadoop_unpartitioned_file(
    fs: Arc<HadoopFileSystem>,
    file: String,
) -> PartitionedFile {
    let file_status = fs.inner.get_file_status(&file).ok().unwrap();
    PartitionedFile {
        file_meta: get_meta(file, file_status),
        partition_values: vec![],
    }
}

struct HadoopFileReader {
    fs: Arc<HadoopFileSystem>,
    file: SizedFile,
}

impl HadoopFileReader {
    fn new(fs: Arc<HadoopFileSystem>, file: SizedFile) -> Result<Self> {
        Ok(Self { fs, file })
    }
}

#[async_trait]
impl ObjectReader for HadoopFileReader {
    async fn chunk_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Box<dyn AsyncRead>> {
        todo!(
            "implement once async file readers are available (arrow-rs#78, arrow-rs#111)"
        )
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        let file = self.fs.open(&self.file.path)?;
        file.inner.seek(start);
        Ok(Box::new(file.take(length as u64)))
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
