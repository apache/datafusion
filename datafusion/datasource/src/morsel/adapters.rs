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

use crate::PartitionedFile;
use crate::file_stream::FileOpener;
use crate::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use arrow::array::RecordBatch;
use datafusion_common::Result;
use futures::FutureExt;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;

/// Adapt a legacy [`FileOpener`] to the morsel API.
///
/// This preserves backwards compatibility for file formats that have not yet
/// implemented a native [`Morselizer`].
pub struct FileOpenerMorselizer {
    file_opener: Arc<dyn FileOpener>,
}

impl Debug for FileOpenerMorselizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileOpenerMorselizer")
            .field("file_opener", &"...")
            .finish()
    }
}

impl FileOpenerMorselizer {
    pub fn new(file_opener: Arc<dyn FileOpener>) -> Self {
        Self { file_opener }
    }
}

impl Morselizer for FileOpenerMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        Ok(Box::new(FileOpenFutureMorselPlanner::new(
            Arc::clone(&self.file_opener),
            file,
        )))
    }
}

enum FileOpenFutureMorselPlanner {
    Unopened {
        file_opener: Arc<dyn FileOpener>,
        file: Box<PartitionedFile>,
    },
    ReadyStream(BoxStream<'static, Result<RecordBatch>>),
}

impl Debug for FileOpenFutureMorselPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unopened { .. } => f
                .debug_tuple("FileOpenFutureMorselPlanner::Unopened")
                .finish(),
            Self::ReadyStream(_) => f
                .debug_tuple("FileOpenFutureMorselPlanner::ReadyStream")
                .finish(),
        }
    }
}

impl FileOpenFutureMorselPlanner {
    fn new(file_opener: Arc<dyn FileOpener>, file: PartitionedFile) -> Self {
        Self::Unopened {
            file_opener,
            file: Box::new(file),
        }
    }
}

impl MorselPlanner for FileOpenFutureMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        match *self {
            Self::Unopened { file_opener, file } => {
                let io_future = async move {
                    let stream = file_opener.open(*file)?.await?;
                    Ok(Box::new(Self::ReadyStream(stream)) as Box<dyn MorselPlanner>)
                }
                .boxed();
                Ok(Some(MorselPlan::new().with_pending_planner(io_future)))
            }
            Self::ReadyStream(stream) => Ok(Some(
                MorselPlan::new()
                    .with_morsels(vec![Box::new(FileStreamMorsel { stream })]),
            )),
        }
    }
}

struct FileStreamMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl Debug for FileStreamMorsel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStreamMorsel").finish_non_exhaustive()
    }
}

impl Morsel for FileStreamMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }
}
