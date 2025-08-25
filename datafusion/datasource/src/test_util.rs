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
    schema_adapter::SchemaAdapterFactory,
};

use std::sync::Arc;

use crate::source::DataSource;
use arrow::datatypes::Schema;
use datafusion_common::Result;
use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::DisplayFormatType;
use object_store::ObjectStore;

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,

    config: FileScanConfig,
}

impl MockSource {
    pub fn new(config: FileScanConfig) -> Self {
        Self {
            metrics: Default::default(),
            schema_adapter_factory: None,
            config,
        }
    }
}

impl FileSource for MockSource {
    fn config(&self) -> &FileScanConfig {
        &self.config
    }

    fn with_config(&self, config: FileScanConfig) -> Arc<dyn FileSource> {
        let mut this = self.clone();
        this.config = config;

        Arc::new(this)
    }

    fn as_data_source(&self) -> Arc<dyn DataSource> {
        Arc::new(self.clone())
    }

    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn metrics_inner(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "mock"
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

impl std::fmt::Debug for MockSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{ ", self.file_type())?;
        write!(f, "statistics={:?}, ", self.file_source_statistics())?;
        <Self as DataSource>::fmt_as(self, DisplayFormatType::Verbose, f)?;
        write!(f, " }}")
    }
}

/// Create a column expression
pub(crate) fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
}
