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

use crate::{
    file::FileSource, file_scan_config::FileScanConfig, file_stream::FileOpener,
};

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, Statistics};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone, Default)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl FileSource for MockSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.projected_statistics = Some(statistics);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self
            .projected_statistics
            .as_ref()
            .expect("projected_statistics must be set")
            .clone())
    }

    fn file_type(&self) -> &str {
        "mock"
    }
}
