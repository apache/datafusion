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

use std::sync::Arc;

use crate::file_scan_config::FileScanConfig;
use datafusion_common::{Result, internal_err};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};

use super::{FileOpener, FileStream, FileStreamMetrics, FileStreamState, OnError};

/// Builder for constructing a [`FileStream`].
pub struct FileStreamBuilder<'a> {
    config: &'a FileScanConfig,
    partition: Option<usize>,
    file_opener: Option<Arc<dyn FileOpener>>,
    metrics: Option<&'a ExecutionPlanMetricsSet>,
    on_error: OnError,
}

impl<'a> FileStreamBuilder<'a> {
    /// Create a new builder.
    pub fn new(config: &'a FileScanConfig) -> Self {
        Self {
            config,
            partition: None,
            file_opener: None,
            metrics: None,
            on_error: OnError::Fail,
        }
    }

    /// Configure the partition to scan.
    pub fn with_partition(mut self, partition: usize) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Configure the [`FileOpener`] used to open files.
    pub fn with_file_opener(mut self, file_opener: Arc<dyn FileOpener>) -> Self {
        self.file_opener = Some(file_opener);
        self
    }

    /// Configure the metrics set used by the stream.
    pub fn with_metrics(mut self, metrics: &'a ExecutionPlanMetricsSet) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Configure the behavior when opening or scanning a file fails.
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Build the configured [`FileStream`].
    pub fn build(self) -> Result<FileStream> {
        let partition = match self.partition {
            Some(partition) => partition,
            None => {
                return internal_err!(
                    "FileStreamBuilder missing required field: partition"
                );
            }
        };
        let file_opener = match self.file_opener {
            Some(file_opener) => file_opener,
            None => {
                return internal_err!(
                    "FileStreamBuilder missing required field: file_opener"
                );
            }
        };
        let metrics = match self.metrics {
            Some(metrics) => metrics,
            None => {
                return internal_err!(
                    "FileStreamBuilder missing required field: metrics"
                );
            }
        };
        let projected_schema = self.config.projected_schema()?;
        let file_group = match self.config.file_groups.get(partition).cloned() {
            Some(file_group) => file_group,
            None => {
                return internal_err!(
                    "FileStreamBuilder invalid partition index: {partition}"
                );
            }
        };

        Ok(FileStream {
            file_iter: file_group.into_inner().into_iter().collect(),
            projected_schema,
            remain: self.config.limit,
            file_opener,
            state: FileStreamState::Idle,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: self.on_error,
        })
    }
}
