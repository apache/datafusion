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

use crate::context::StorageContext;
use crate::file_metadata::StorageFileMetadata;
use crate::list::{StorageFileLister, StorageListOptions};
use crate::read::StorageFileReader;
use crate::stat::StorageStatOptions;
use crate::StorageFileWriter;
use crate::{StorageReadOptions, StorageWriteOptions, StorageWriteResult};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_common::Result;
use futures::Stream;
use std::fmt::Debug;
use std::ops::Range;

#[async_trait]
pub trait Storage: std::fmt::Display + Send + Sync + Debug + 'static {
    async fn write_opts(
        &self,
        ctx: &StorageContext,
        location: &str,
        options: StorageWriteOptions,
    ) -> Result<StorageFileWriter>;

    async fn read_opts(
        &self,
        ctx: &StorageContext,
        location: &str,
        options: StorageReadOptions,
    ) -> Result<StorageFileReader>;

    async fn stat_opts(
        &self,
        ctx: &StorageContext,
        location: &str,
        options: StorageStatOptions,
    ) -> Result<StorageFileMetadata>;

    async fn list_opts(
        &self,
        ctx: &StorageContext,
        location: &str,
        options: StorageListOptions,
    ) -> Result<StorageFileLister>;
}

#[async_trait]
pub trait StorageExt: Storage {
    async fn write(
        &self,
        ctx: &StorageContext,
        location: &str,
        payload: Bytes,
    ) -> Result<StorageWriteResult> {
        todo!()
    }

    async fn write_iter(
        &self,
        ctx: &StorageContext,
        location: &str,
        payload: impl Iterator<Item = Result<Bytes>> + Send,
    ) -> Result<StorageWriteResult> {
        todo!()
    }

    async fn write_stream(
        &self,
        ctx: &StorageContext,
        location: &str,
        stream: impl Stream<Item = Result<Bytes>> + Send,
    ) -> Result<StorageWriteResult> {
        todo!()
    }

    async fn read(&self, ctx: &StorageContext, location: &str) -> Result<Bytes> {
        todo!()
    }

    async fn read_range(
        &self,
        ctx: &StorageContext,
        location: &str,
        range: Range<usize>,
    ) -> Result<Bytes> {
        todo!()
    }

    async fn read_ranges(
        &self,
        ctx: &StorageContext,
        location: &str,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        todo!()
    }
}
