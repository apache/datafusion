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

//! Provides object store implementation [LocalFileSystem], which wraps 
//! file system operations in tokio.
//! 
//! ```
//! use tempfile::tempdir;
//! use tokio::io::{AsyncRead, AsyncWrite};
//! use future::StreamExt;
//! use datafusion_data_access::object_store::ObjectStore;
//! use datafusion_data_access::object_store::local::LocalFileSystem;
//! 
//! async {
//!   let fs = LocalFileSystem;
//!   let tmp_dir = tempdir()?.path();
//!   let dir_path = tmp_dir.join("x");
//!   let file_path = tmp_dir.join("a.txt");
//! 
//!   // Create dir
//!   fs.create_dir(dir_path.to_str().unwrap(), true).await?;
//!
//!   // Write a file
//!   let writer = fs.file_writer(file_path.to_str().unwrap())?.writer().await?;
//!   writer.write_all("test").await?;
//!   writer.shutdown();
//! 
//!   // List files
//!   let files = fs.list_file(dir_path.to_str().unwrap()).await?;
//!   let Some(file) = files.next();
//!   assert_eq!(file.path, file_path.to_str().unwrap());
//! 
//!   // Read data back
//!   let reader = fs.file_reader(file)?.chunk_reader().await?;
//!   let data = reader.read_all().await?;
//!   assert_eq!(data, "test");
//! 
//!   // Clear dir
//!   fs.remove_dir_all(dir_path.to_str().unwrap()).await?;
//! };
//! ```

use std::fs::{self, File, Metadata};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream, AsyncRead, StreamExt};
use tokio::{fs::File as AsyncFile, io::AsyncWrite};

use crate::{FileMeta, Result, SizedFile};

use super::{
    path_without_scheme, FileMetaStream, ListEntryStream, ObjectReader,
    ObjectReaderStream, ObjectStore, ObjectWriter
};

pub static LOCAL_SCHEME: &str = "file";

#[derive(Debug, Default)]
/// Local File System as Object Store.
pub struct LocalFileSystem;

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let prefix = path_without_scheme(prefix);
        list_all(prefix.to_owned()).await
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(LocalFileReader::new(file)?))
    }

    fn file_writer(&self, path: &str) -> Result<Arc<dyn ObjectWriter>> {
        let path = path_without_scheme(path).to_string();
        Ok(Arc::new(LocalFileWriter::new(path)?))
    }

    async fn create_dir(&self, path: &str, recursive: bool) -> Result<()> {
        let path = path_without_scheme(path);
        let res = match recursive {
            false => tokio::fs::create_dir(path).await,
            true => tokio::fs::create_dir_all(path).await,
        };
        match res {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn remove_dir_all(&self, path: &str) -> Result<()> {
        let path = path_without_scheme(path);
        tokio::fs::remove_dir_all(path).await
    }

    async fn remove_dir_contents(&self, path: &str) -> Result<()> {
        let path = path_without_scheme(path);
        let mut entries = tokio::fs::read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                self.remove_dir_all(entry.path().to_str().unwrap()).await?;
            } else {
                self.remove_file(entry.path().to_str().unwrap()).await?;
            }
        }
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        let path = path_without_scheme(path);
        let res = tokio::fs::remove_file(path).await;
        match res {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                // If path is a directory, we should return InvalidInput instead
                if tokio::fs::metadata(path).await?.is_dir() {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "was a directory",
                    ))
                } else {
                    Err(e)
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn rename(&self, source: &str, dest: &str) -> Result<()> {
        let source = path_without_scheme(source);
        let dest = path_without_scheme(dest);
        tokio::fs::rename(source, dest).await
    }

    async fn copy(&self, source: &str, dest: &str) -> Result<()> {
        let source_root = PathBuf::from(path_without_scheme(source));
        let dest_root = PathBuf::from(path_without_scheme(dest));

        if !source_root.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Source path not found",
            ));
        }

        if dest_root.exists() && dest_root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Cannot overwrite an existing directory.",
            ));
        }

        if source_root.is_file() {
            tokio::fs::copy(source, dest).await?;
            return Ok(());
        }

        self.create_dir(dest_root.clone().to_str().unwrap(), true)
            .await?;

        let mut stack = vec![source_root.clone()];

        while let Some(working_path) = stack.pop() {
            let mut entries = tokio::fs::read_dir(working_path.clone()).await?;

            let working_dest =
                dest_root.join(working_path.strip_prefix(&source_root).unwrap());
            self.create_dir(working_dest.to_str().unwrap(), true)
                .await?;

            while let Some(entry) = entries.next_entry().await? {
                if entry.path().is_file() {
                    let entry_dest =
                        dest_root.join(entry.path().strip_prefix(&source_root).unwrap());
                    tokio::fs::copy(entry.path(), entry_dest.clone()).await?;
                } else {
                    stack.push(entry.path());
                }
            }
        }

        Ok(())
    }
}

struct LocalFileReader {
    file: SizedFile,
}

impl LocalFileReader {
    fn new(file: SizedFile) -> Result<Self> {
        Ok(Self { file })
    }
}

#[async_trait]
impl ObjectReader for LocalFileReader {
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
        // A new file descriptor is opened for each chunk reader.
        // This okay because chunks are usually fairly large.
        let mut file = File::open(&self.file.path)?;
        file.seek(SeekFrom::Start(start))?;

        let file = BufReader::new(file.take(length as u64));

        Ok(Box::new(file))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

async fn list_all(prefix: String) -> Result<FileMetaStream> {
    fn get_meta(path: String, metadata: Metadata) -> FileMeta {
        FileMeta {
            sized_file: SizedFile {
                path,
                size: metadata.len(),
            },
            last_modified: metadata.modified().map(chrono::DateTime::from).ok(),
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid path".to_string(),
                ));
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

/// Create a stream of `ObjectReader` by converting each file in the `files` vector
/// into instances of `LocalFileReader`
pub fn local_object_reader_stream(files: Vec<String>) -> ObjectReaderStream {
    Box::pin(futures::stream::iter(files).map(|f| Ok(local_object_reader(f))))
}

/// Helper method to convert a file location to a `LocalFileReader`
pub fn local_object_reader(file: String) -> Arc<dyn ObjectReader> {
    LocalFileSystem
        .file_reader(local_unpartitioned_file(file).sized_file)
        .expect("File not found")
}

// TODO: Why would a user want this function that unwraps? unless we said it's unsafe?
/// Helper method to fetch the file size and date at given path and create a `FileMeta`
pub fn local_unpartitioned_file(file: String) -> FileMeta {
    let metadata = fs::metadata(&file).expect("Local file metadata");
    FileMeta {
        sized_file: SizedFile {
            size: metadata.len(),
            path: file,
        },
        last_modified: metadata.modified().map(DateTime::<Utc>::from).ok(),
    }
}

struct LocalFileWriter {
    path: String,
}

impl LocalFileWriter {
    fn new(path: String) -> Result<Self> {
        Ok(Self { path })
    }
}

#[async_trait]
impl ObjectWriter for LocalFileWriter {
    async fn writer(&self) -> Result<Pin<Box<dyn AsyncWrite>>> {
        let file = AsyncFile::open(&self.path).await?;
        Ok(Box::pin(file))
    }

    fn sync_writer(&self) -> Result<Box<dyn Write + Send + Sync>> {
        let file = File::open(&self.path)?;
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_object_store;
    use futures::StreamExt;
    use std::collections::HashSet;
    use std::fs::File;
    use std::fs::{create_dir, read_dir};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_recursive_listing() -> Result<()> {
        // tmp/a.txt
        // tmp/x/b.txt
        // tmp/y/c.txt
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();
        let x_path = tmp.join("x");
        let y_path = tmp.join("y");
        let a_path = tmp.join("a.txt");
        let b_path = x_path.join("b.txt");
        let c_path = y_path.join("c.txt");
        create_dir(&x_path)?;
        create_dir(&y_path)?;
        File::create(&a_path)?;
        File::create(&b_path)?;
        File::create(&c_path)?;

        let mut all_files = HashSet::new();
        let mut files = list_all(tmp.to_str().unwrap().to_string()).await?;
        while let Some(file) = files.next().await {
            let file = file?;
            assert_eq!(file.size(), 0);
            all_files.insert(file.path().to_owned());
        }

        assert_eq!(all_files.len(), 3);
        assert!(all_files.contains(a_path.to_str().unwrap()));
        assert!(all_files.contains(b_path.to_str().unwrap()));
        assert!(all_files.contains(c_path.to_str().unwrap()));

        Ok(())
    }

    #[tokio::test]
    async fn test_create_dir() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();

        let fs = LocalFileSystem;

        // Create directory succeeds
        let z_path = tmp.join("z");
        fs.create_dir(z_path.to_str().unwrap(), false).await?;
        assert!(z_path.exists());

        // Create recursive directory succeeds
        let rec_path = tmp.join("w").join("a");
        fs.create_dir(rec_path.to_str().unwrap(), true).await?;
        assert!(rec_path.exists());

        // Returns Ok if already exists
        fs.create_dir(tmp.to_str().unwrap(), false).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_dir() -> Result<()> {
        // tmp/a.txt
        // tmp/x/b.txt
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();
        let x_path = tmp.join("x");
        let a_path = tmp.join("a.txt");
        let b_path = x_path.join("b.txt");
        create_dir(&x_path)?;
        File::create(&a_path)?;
        File::create(&b_path)?;

        let fs = LocalFileSystem;

        // Delete contents tmp means tmp is empty
        fs.remove_dir_contents(tmp.to_str().unwrap()).await?;
        assert!(tmp.exists());
        assert_eq!(read_dir(tmp)?.count(), 0);

        // Delete tmp means no tmp
        fs.remove_dir_all(tmp.to_str().unwrap()).await?;
        assert!(!tmp.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_file() -> Result<()> {
        // tmp/a.txt
        // tmp/x
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();
        let x_path = tmp.join("x");
        let a_path = tmp.join("a.txt");
        create_dir(&x_path)?;
        File::create(&a_path)?;

        let fs = LocalFileSystem;

        // Delete existing file works
        fs.remove_file(a_path.to_str().unwrap()).await?;
        assert!(!a_path.exists());

        // Delete non-existent file errors
        let res = fs
            .remove_file(tmp.join("missing.txt").to_str().unwrap())
            .await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::NotFound);

        // Delete file on directory errors
        let res = fs.remove_file(x_path.to_str().unwrap()).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        Ok(())
    }

    #[tokio::test]
    async fn test_rename() -> Result<()> {
        // tmp/a.txt
        // tmp/b.txt
        // tmp/x/b.txt
        // tmp/empty/
        // tmp/y/c.txt
        // tmp/y/z/d.txt
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();
        let x_path = tmp.join("x");
        let empty_path = tmp.join("empty");
        let y_path = tmp.join("y");
        let z_path = y_path.join("z");
        let a_path = tmp.join("a.txt");
        let b_path = tmp.join("b.txt");
        let x_b_path = x_path.join("b.txt");
        let c_path = y_path.join("c.txt");
        let d_path = z_path.join("d.txt");
        create_dir(&x_path)?;
        create_dir(&empty_path)?;
        create_dir(&y_path)?;
        create_dir(&z_path)?;
        File::create(&a_path)?;
        File::create(&b_path)?;
        File::create(&x_b_path)?;
        File::create(&c_path)?;
        File::create(&d_path)?;

        let fs = LocalFileSystem;

        // Can rename a file, and it will exist at dest and not at source
        let a2_path = tmp.join("a2.txt");
        fs.rename(a_path.to_str().unwrap(), a2_path.to_str().unwrap())
            .await?;
        assert!(!a_path.exists());
        assert!(a2_path.exists());

        // rename replaces files
        let test_content = b"test";
        let mut f = File::create(&a_path)?;
        f.write_all(test_content)?;
        f.flush()?;
        fs.rename(a_path.to_str().unwrap(), b_path.to_str().unwrap())
            .await?;
        assert!(!a_path.exists());
        assert!(b_path.exists());
        let mut f = File::open(&b_path)?;
        let mut actual_content = Vec::new();
        f.read_to_end(&mut actual_content)?;
        assert_eq!(actual_content, test_content);

        // Can rename a directory, and it will recursively copy contents
        let dest_path = tmp.join("v");
        fs.rename(y_path.to_str().unwrap(), dest_path.to_str().unwrap())
            .await?;
        assert!(!y_path.exists());
        assert!(dest_path.exists());
        assert!(dest_path.join("c.txt").exists());
        assert!(dest_path.join("z").join("d.txt").exists());

        // rename errors if it would overwrite non-empty directory
        let res = fs
            .rename(dest_path.to_str().unwrap(), x_path.to_str().unwrap())
            .await;
        assert!(res.is_err());
        // We cannot test for specific error. See: https://diziet.dreamwidth.org/9894.html

        // rename succeeds if it would overwrite an empty directory
        fs.rename(dest_path.to_str().unwrap(), empty_path.to_str().unwrap())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_copy() -> Result<()> {
        // tmp/a.txt
        // tmp/b.txt
        // tmp/x/b.txt
        // tmp/empty/
        // tmp/y/c.txt
        // tmp/y/z/d.txt
        let tmp_dir = tempdir()?;
        let tmp = tmp_dir.path();
        let x_path = tmp.join("x");
        let empty_path = tmp.join("empty");
        let y_path = tmp.join("y");
        let z_path = y_path.join("z");
        let a_path = tmp.join("a.txt");
        let b_path = tmp.join("b.txt");
        let x_b_path = x_path.join("b.txt");
        let c_path = y_path.join("c.txt");
        let d_path = z_path.join("d.txt");
        create_dir(&x_path)?;
        create_dir(&empty_path)?;
        create_dir(&y_path)?;
        create_dir(&z_path)?;
        File::create(&a_path)?;
        File::create(&b_path)?;
        File::create(&x_b_path)?;
        File::create(&c_path)?;
        File::create(&d_path)?;

        let fs = LocalFileSystem;

        // Can copy a file, and it will exist at dest and source
        let a2_path = tmp.join("a2.txt");
        fs.copy(a_path.to_str().unwrap(), a2_path.to_str().unwrap())
            .await?;
        assert!(a_path.exists());
        assert!(a2_path.exists());

        // Copy replaces files
        let test_content = b"test";
        let mut f = File::create(&a_path)?;
        f.write_all(test_content)?;
        f.flush()?;
        fs.copy(a_path.to_str().unwrap(), b_path.to_str().unwrap())
            .await?;
        assert!(a_path.exists());
        assert!(b_path.exists());
        let mut f = File::open(&b_path)?;
        let mut actual_content = Vec::new();
        f.read_to_end(&mut actual_content)?;
        assert_eq!(actual_content, test_content);

        // Can copy a directory, and it will recursively copy contents
        let dest_path = tmp.join("v");
        fs.copy(y_path.to_str().unwrap(), dest_path.to_str().unwrap())
            .await?;
        assert!(y_path.exists());
        assert!(dest_path.exists());
        assert!(dest_path.join("c.txt").exists());
        assert!(dest_path.join("z").join("d.txt").exists());

        // Copy errors if it would overwrite a non-empty directory
        let res = fs
            .copy(dest_path.to_str().unwrap(), x_path.to_str().unwrap())
            .await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);

        // Copy errors if it would overwrite a non-empty directory
        let res = fs
            .copy(dest_path.to_str().unwrap(), empty_path.to_str().unwrap())
            .await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);

        Ok(())
    }

    test_object_store!(LocalFileSystem::default, tempdir);
}
