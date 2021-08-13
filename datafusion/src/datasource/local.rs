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
use crate::datasource::object_store::{ObjectReader, ObjectStore, FileNameStream};
use crate::error::DataFusionError;
use crate::error::Result;
use crate::parquet::file::reader::Length;
use crate::parquet::file::serialized_reader::FileSource;
use async_trait::async_trait;
use std::any::Any;
use std::io::Read;
use std::sync::Arc;
use futures::{stream, Stream, StreamExt};
use tokio::fs::{File, self, ReadDir};
use std::path::PathBuf;

#[derive(Debug)]
/// Local File System as Object Store.
pub struct LocalFileSystem;


#[async_trait]
impl ObjectStore for LocalFileSystem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn list_all_files(&self, path: &str, ext: &str) -> Result<FileNameStream> {
        list_all(path.to_string(), ext.to_string()).await
    }

    fn get_reader(&self, file_path: &str) -> Result<Arc<dyn ObjectReader>> {
        let file = File::open(file_path)?;
        let reader = LocalFSObjectReader::new(file)?;
        Ok(Arc::new(reader))
    }
}

struct LocalFSObjectReader {
    file: File,
}

impl LocalFSObjectReader {
    fn new(file: File) -> Result<Self> {
        Ok(Self { file })
    }
}

impl ObjectReader for LocalFSObjectReader {
    fn get_reader(&self, start: u64, length: usize) -> Box<dyn Read> {
        Box::new(FileSource::<File>::new(&self.file, start, length))
    }

    fn length(&self) -> u64 {
        self.file.len()
    }
}

async fn list_all(root_path: String, ext: String) -> Result<FileNameStream> {
    // let mut filenames: Vec<String> = Vec::new();
    // list_all_files(root_path, &mut filenames, ext).await?;
    // Ok(filenames)

    async fn one_level(path: String, to_visit: &mut Vec<String>, ext: String) -> Result<Vec<String>> {
        let mut dir = fs::read_dir(path).await?;
        let mut files = Vec::new();

        while let Some(child) = dir.next_entry().await? {
            if let Some(child_path) = child.path().to_str() {
                if child.metadata().await?.is_dir() {
                    to_visit.push(child_path.to_string());
                } else {
                    if child_path.ends_with(&ext) {
                        files.push(child_path.to_string())
                    }
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()))
            }

        }
        Ok(files)
    }

    stream::unfold(vec![root_path], |mut to_visit| {
        async {
            let path = to_visit.pop()?;
            let file_stream = match one_level(path, &mut to_visit, ext).await {
                Ok(files) => stream::iter(files).map(Ok).left_stream(),
                Err(e) => stream::once(async { Err(e) }).right_stream(),
            };

            Some((file_stream, to_visit))
        }
    }).flatten()
}

/// Recursively build a list of files in a directory with a given extension with an accumulator list
// async fn list_all_files(dir: &str, filenames: &mut Vec<String>, ext: &str) -> Result<()> {
//     let metadata = std::fs::metadata(dir)?;
//     if metadata.is_file() {
//         if dir.ends_with(ext) {
//             filenames.push(dir.to_string());
//         }
//     } else {
//         for entry in std::fs::read_dir(dir)? {
//             let entry = entry?;
//             let path = entry.path();
//             if let Some(path_name) = path.to_str() {
//                 if path.is_dir() {
//                     list_all_files(path_name, filenames, ext).await?;
//                 } else if path_name.ends_with(ext) {
//                     filenames.push(path_name.to_string());
//                 }
//             } else {
//                 return Err(DataFusionError::Plan("Invalid path".to_string()));
//             }
//         }
//     }
//     Ok(())
// }
