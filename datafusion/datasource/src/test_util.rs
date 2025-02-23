use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Statistics;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

use crate::{
    file::FileSource, file_scan_config::FileScanConfig, file_stream::FileOpener,
};
use datafusion_common::Result;

/// Minimal [`FileSource`] implementation for use in tests.
#[derive(Clone, Default)]
pub struct MockSource {
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
