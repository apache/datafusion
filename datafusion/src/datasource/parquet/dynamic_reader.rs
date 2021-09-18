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

//! It's for the abstraction of the object traits, like ``Read`` and ``ChunkReader``
use std::io::Read;

use parquet::file::reader::{ChunkReader, Length};

/// It's an abstraction of the ``Read`` object trait with related trait implemented
pub struct DynRead {
    inner: Box<dyn Read>,
}

impl DynRead {
    /// New a DynRead for wrapping a trait object of Read
    pub fn new(reader: Box<dyn Read>) -> Self {
        DynRead { inner: reader }
    }
}

impl Read for DynRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

/// It's an abstraction of the ``ChunkReader`` object trait with related trait implemented
pub struct DynChunkReader {
    inner: Box<dyn ChunkReader<T = DynRead>>,
}

impl DynChunkReader {
    /// New a DynChunkReader for wrapping a trait object of ChunkReader
    pub fn new(reader: Box<dyn ChunkReader<T = DynRead>>) -> Self {
        DynChunkReader { inner: reader }
    }
}

impl Length for DynChunkReader {
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

impl ChunkReader for DynChunkReader {
    type T = DynRead;

    fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
        self.inner.get_read(start, length)
    }
}
