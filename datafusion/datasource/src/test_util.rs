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

use arrow::datatypes::Schema;
use datafusion_common::Result;
use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    table_schema: crate::table_schema::TableSchema,
    projection: crate::projection::SplitProjection,
}

impl Default for MockSource {
    fn default() -> Self {
        let table_schema =
            crate::table_schema::TableSchema::new(Arc::new(Schema::empty()), vec![]);
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
            filter: None,
            projection: crate::projection::SplitProjection::unprojected(&table_schema),
            table_schema,
        }
    }
}

impl MockSource {
    pub fn new(table_schema: impl Into<crate::table_schema::TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
            filter: None,
            projection: crate::projection::SplitProjection::unprojected(&table_schema),
            table_schema,
        }
    }

    pub fn with_filter(mut self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl FileSource for MockSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.filter.clone()
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
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

    fn table_schema(&self) -> &crate::table_schema::TableSchema {
        &self.table_schema
    }

    fn try_pushdown_projection(
        &self,
        projection: &datafusion_physical_plan::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection = crate::projection::SplitProjection::new(
            self.table_schema.file_schema(),
            &new_projection,
        );
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(
        &self,
    ) -> Option<&datafusion_physical_plan::projection::ProjectionExprs> {
        Some(&self.projection.source)
    }
}

/// Create a column expression
pub(crate) fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
}
