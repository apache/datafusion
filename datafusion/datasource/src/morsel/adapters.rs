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
use std::sync::{Arc, Mutex};

/// An adapter for `FileOpener` that allows it to be used as a `Morselizer` for
/// backwards compatibility.
///
/// This is useful for file formats that do not support morselization, where we
/// can treat the entire file as a single morsel.
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
    fn morselize(&self, file: PartitionedFile) -> Result<Vec<Box<dyn MorselPlanner>>> {
        let opener = Arc::clone(&self.file_opener);
        let planner = FileOpenFutureMorselPlanner::new(opener, file);
        Ok(vec![Box::new(planner)])
    }
}

/// Adapter for `FileOpenFuture` that allows it to be used as a `MorselPlanner`
/// for backwards compatibility.
struct FileOpenFutureMorselPlanner {
    file_opener: Arc<dyn FileOpener>,
    stream: Arc<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>,
    file: Mutex<Option<PartitionedFile>>,
}

impl Debug for FileOpenFutureMorselPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileOpenFutureMorselPlanner")
            .field("file_opener", &"...")
            .field("stream", &"...")
            .field("file", &self.file)
            .finish()
    }
}

impl FileOpenFutureMorselPlanner {
    pub fn new(file_opener: Arc<dyn FileOpener>, file: PartitionedFile) -> Self {
        Self {
            file_opener,
            stream: Arc::new(Mutex::new(None)),
            file: Mutex::new(Some(file)),
        }
    }
}

impl MorselPlanner for FileOpenFutureMorselPlanner {
    fn plan(&mut self) -> Result<Option<MorselPlan>> {
        let mut morsel_plan = MorselPlan::new();
        let mut made_progress = false;

        // Note that plan should **not** do IO work so setup a callback if needed
        if let Some(file) = self.file.lock().unwrap().take() {
            let file_opener = Arc::clone(&self.file_opener);
            let output_stream = Arc::clone(&self.stream);
            let load_future = async move {
                let stream = file_opener
                    // open the file to get a stream
                    .open(file)?
                    // create the stream
                    .await?;
                // store the stream for later retrieval
                *(output_stream.lock().unwrap()) = Some(stream);
                Ok(())
            };
            morsel_plan = morsel_plan.with_io_future(load_future.boxed());
            made_progress = true;
        }

        // If the stream is ready, return it as a morsel
        if let Some(stream) = self.stream.lock().unwrap().take() {
            let morsel = FileStreamMorsel::new(stream);
            morsel_plan = morsel_plan.with_morsels(vec![Box::new(morsel)]);
            made_progress = true;
        }

        Ok(made_progress.then_some(morsel_plan))
    }
}

struct FileStreamMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl Debug for FileStreamMorsel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStreamMorsel")
            .field("stream", &"...")
            .finish()
    }
}

impl FileStreamMorsel {
    pub fn new(stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { stream }
    }
}

impl Morsel for FileStreamMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }

    fn split(&mut self) -> Result<Vec<Box<dyn Morsel>>> {
        Ok(vec![]) // no splitting supported
    }
}
