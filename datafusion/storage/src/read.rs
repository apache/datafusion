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
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use datafusion_common::Result;
use futures::stream::BoxStream;
use futures::{stream, Stream, StreamExt};
use std::ops::{Range, RangeBounds};

/// Request only a portion of an object's bytes
///
/// Implementations may wish to inspect [`ReadResult`] for the exact byte
/// range returned.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReadRange {
    /// Request a specific range of bytes
    ///
    /// If the given range is zero-length or starts after the end of the object,
    /// an error will be returned. Additionally, if the range ends after the end
    /// of the object, the entire remainder of the object will be returned.
    /// Otherwise, the exact requested range will be returned.
    Bounded(Range<u64>),
    /// Request all bytes starting from a given byte offset
    Offset(u64),
    /// Request up to the last n bytes
    Suffix(u64),
}

impl<T: RangeBounds<u64>> From<T> for ReadRange {
    fn from(value: T) -> Self {
        use std::ops::Bound::*;
        let first = match value.start_bound() {
            Included(i) => *i,
            Excluded(i) => i + 1,
            Unbounded => 0,
        };
        match value.end_bound() {
            Included(i) => Self::Bounded(first..(i + 1)),
            Excluded(i) => Self::Bounded(first..*i),
            Unbounded => Self::Offset(first),
        }
    }
}

/// Options for a read request, such as range
#[derive(Debug, Default, Clone)]
pub struct StorageReadOptions {
    /// Request will succeed if the `StorageFileMetadata::e_tag` matches
    /// otherwise returning [`Error::Precondition`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-Match: "xyzzy"
    /// If-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-Match: *
    /// ```
    pub if_match: Option<String>,
    /// Request will succeed if the `StorageFileMetadata::e_tag` does not match
    /// otherwise returning [`Error::NotModified`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-None-Match: "xyzzy"
    /// If-None-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-None-Match: *
    /// ```
    pub if_none_match: Option<String>,
    /// Request will succeed if the object has been modified since
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
    pub if_modified_since: Option<DateTime<Utc>>,
    /// Request will succeed if the object has not been modified since
    /// otherwise returning [`Error::Precondition`]
    ///
    /// Some stores, such as S3, will only return `NotModified` for exact
    /// timestamp matches, instead of for any timestamp greater than or equal.
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
    pub if_unmodified_since: Option<DateTime<Utc>>,
    /// Request transfer of only the specified range of bytes
    /// otherwise returning [`Error::NotModified`]
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-range>
    pub range: Option<ReadRange>,
    /// Request a particular object version
    pub version: Option<String>,
}

#[async_trait]
pub trait StorageFileRead: Send + Sync + 'static {
    async fn read(&mut self, ctx: &StorageContext) -> Result<Bytes>;
}

pub struct StorageFileReader {
    meta: StorageFileMetadata,
    inner: Box<dyn StorageFileRead>,
}

/// Expose public API for reading from a file
impl StorageFileReader {
    pub fn metadata(&self) -> &StorageFileMetadata {
        &self.meta
    }

    pub fn into_futures_reader(self) -> StorageFileFuturesReader {
        todo!()
    }

    pub fn into_stream(self) -> StorageFileBytesStream {
        todo!()
    }

    pub fn into_tokio_reader(self) -> StorageFileTokioReader {
        todo!()
    }
}

/// Adapter to allow using `futures::io::AsyncRead` with `StorageFileReader`
pub struct StorageFileFuturesReader {}

/// Adapter to allow using `futures::io::Stream` with `StorageFileReader`
pub struct StorageFileBytesStream {}

/// Adapter to allow using `tokio::io::AsyncRead` with `StorageFileReader`
pub struct StorageFileTokioReader {}
