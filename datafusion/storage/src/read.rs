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

use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::{fmt::Debug, ops::Range, sync::Arc};

/// A bounded range or a suffix read, as used by existing file formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadRange {
    Bounded(Range<u64>),
    Suffix(u64),
}

impl From<Range<u64>> for ReadRange {
    fn from(range: Range<u64>) -> Self {
        Self::Bounded(range)
    }
}

/// A reusable reader for one file.
///
/// Single and batched reads remain separate so adapters can preserve native
/// implementations of both. Batch results correspond to input ranges in order.
/// Returned buffers own their bytes independently of this reader. Range and EOF
/// errors follow the backend; this interface does not enforce file revisions or
/// validate reads against previously observed metadata.
#[async_trait]
pub trait FileReader: Debug + Send + Sync {
    async fn read_range(&self, range: ReadRange) -> Result<Bytes>;
    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>>;
    /// Stream a range, or the full file when no range is supplied.
    fn stream(
        self: Arc<Self>,
        range: Option<ReadRange>,
    ) -> BoxStream<'static, Result<Bytes>>;
}
