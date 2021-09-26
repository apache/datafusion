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

//! Parquet related things for hdfs object store
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use parquet::file::reader::{ChunkReader, Length};
use parquet::util::io::{FileSource, TryClone};

use crate::datasource::object_store::hdfs::{to_error, HadoopFileSystem};
use crate::datasource::object_store::ObjectReader;
use crate::datasource::parquet::dynamic_reader::{DynChunkReader, DynRead};

use super::HadoopFile;
use crate::error::Result;

/// Parquet file reader for hdfs object store, by which we can get ``DynChunkReader``
#[derive(Clone)]
pub struct HadoopParquetFileReader {
    file: HadoopFile,
}

impl HadoopParquetFileReader {
    /// Based on the trait object ``ObjectReader`` for local object store,
    /// get an abstraction of the trait object ``ChunkReader``
    pub fn get_dyn_chunk_reader(
        obj_reader: Arc<dyn ObjectReader>,
    ) -> Result<DynChunkReader> {
        let file_meta = obj_reader.get_file_meta();
        let fs = HadoopFileSystem::new(file_meta.path.as_str())?;
        Ok(DynChunkReader::new(Box::new(HadoopParquetFileReader {
            file: fs.open(file_meta.path.as_str())?,
        })))
    }
}

impl Seek for HadoopParquetFileReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let offset = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) | SeekFrom::Current(offset) => offset as u64,
        };
        if self.file.inner.seek(offset) {
            Ok(offset)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Fail to seek to {} for file {}",
                    offset,
                    self.file.inner.path()
                ),
            ))
        }
    }
}

impl Length for HadoopParquetFileReader {
    fn len(&self) -> u64 {
        let status = self.file.inner.get_file_status().ok().unwrap();
        status.len() as u64
    }
}

impl Read for HadoopParquetFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file
            .inner
            .read(buf)
            .map(|read_len| read_len as usize)
            .map_err(|e| to_error(e))
    }
}

impl TryClone for HadoopParquetFileReader {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(self.clone())
    }
}

impl ChunkReader for HadoopParquetFileReader {
    type T = DynRead;

    fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
        Ok(DynRead::new(Box::new(FileSource::new(self, start, length))))
    }
}
