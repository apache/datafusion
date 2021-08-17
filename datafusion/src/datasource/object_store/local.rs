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
use crate::datasource::get_runtime_handle;
use crate::datasource::object_store::{FileNameStream, ObjectReader, ObjectStore};
use crate::error::DataFusionError;
use crate::error::Result;
use crate::parquet::file::reader::Length;
use crate::parquet::file::serialized_reader::FileSource;
use async_trait::async_trait;
use futures::{stream, StreamExt};
use std::any::Any;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

#[derive(Debug)]
/// Local File System as Object Store.
pub struct LocalFileSystem;

#[async_trait]
impl ObjectStore for LocalFileSystem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn list(&self, path: &str, ext: &str) -> Result<Vec<String>> {
        let filenames: Vec<String> = list_all(path.to_string(), ext.to_string())?;
        Ok(filenames)
    }

    async fn list_async(&self, path: &str, ext: &str) -> Result<FileNameStream> {
        list_all_async(path.to_string(), ext.to_string()).await
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

    fn get_reader_async(&self, _start: u64, _length: usize) -> Box<dyn Read> {
        todo!()
    }

    fn length(&self) -> u64 {
        self.file.len()
    }
}

fn list_all(root_path: String, ext: String) -> Result<Vec<String>> {
    let handle = get_runtime_handle();
    let mut file_results: Vec<Result<String>> = Vec::new();
    handle.block_on(async {
        match list_all_async(root_path, ext).await {
            Ok(mut stream) => {
                while let Some(result) = stream.next().await {
                    file_results.push(result);
                }
            }
            Err(_) => {
                file_results.push(Err(DataFusionError::Plan("Invalid path".to_string())));
            }
        }
    });
    file_results.into_iter().collect()
}

async fn list_all_async(root_path: String, ext: String) -> Result<FileNameStream> {
    async fn find_files_in_dir(
        path: String,
        to_visit: &mut Vec<String>,
        ext: String,
    ) -> Result<Vec<String>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut files = Vec::new();

        while let Some(child) = dir.next_entry().await? {
            if let Some(child_path) = child.path().to_str() {
                if child.metadata().await?.is_dir() {
                    to_visit.push(child_path.to_string());
                } else if child_path.ends_with(&ext.clone()) {
                    files.push(child_path.to_string())
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
        Ok(files)
    }

    let result = stream::unfold(vec![root_path], move |mut to_visit| {
        let ext = ext.clone();
        async move {
            match to_visit.pop() {
                None => None,
                Some(path) => {
                    let file_stream =
                        match find_files_in_dir(path, &mut to_visit, ext).await {
                            Ok(files) => stream::iter(files).map(Ok).left_stream(),
                            Err(e) => stream::once(async { Err(e) }).right_stream(),
                        };

                    Some((file_stream, to_visit))
                }
            }
        }
    })
    .flatten();
    Ok(Box::pin(result))
}
