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

//! Parquet related things for local object store
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use parquet::file::reader::{ChunkReader, Length};
use parquet::util::io::{FileSource, TryClone};

use crate::datasource::object_store::ObjectReader;
use crate::datasource::parquet::dynamic_reader::{DynChunkReader, DynRead};

/// Parquet file reader for local object store, by which we can get ``DynChunkReader``
pub struct LocalParquetFileReader {
    file: File,
}

impl LocalParquetFileReader {
    /// Based on the trait object ``ObjectReader`` for local object store,
    /// get an abstraction of the trait object ``ChunkReader``
    pub fn get_dyn_chunk_reader(
        obj_reader: Arc<dyn ObjectReader>,
    ) -> std::io::Result<DynChunkReader> {
        let file_meta = obj_reader.get_file_meta();
        Ok(DynChunkReader::new(Box::new(LocalParquetFileReader {
            file: File::open(file_meta.path.as_str())?,
        })))
    }
}

impl Seek for LocalParquetFileReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Length for LocalParquetFileReader {
    fn len(&self) -> u64 {
        self.file.metadata().unwrap().len()
    }
}

impl Read for LocalParquetFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl TryClone for LocalParquetFileReader {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(LocalParquetFileReader {
            file: self.file.try_clone()?,
        })
    }
}

impl ChunkReader for LocalParquetFileReader {
    type T = DynRead;

    fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
        Ok(DynRead::new(Box::new(FileSource::new(self, start, length))))
    }
}
