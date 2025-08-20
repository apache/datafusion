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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::sync::Arc;

use crate::avro_to_arrow::Reader as AvroReader;

use arrow::datatypes::SchemaRef;
use datafusion_common::error::Result;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use object_store::ObjectStore;

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone, Default)]
pub struct AvroSource {
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl AvroSource {
    /// Initialize an AvroSource with default values
    pub fn new() -> Self {
        Self::default()
    }

    fn open<R: std::io::Read>(
        &self,
        reader: R,
        file_schema: SchemaRef,
        batch_size: Option<usize>,
        projected_file_column_names: Option<Vec<String>>,
    ) -> Result<AvroReader<'static, R>> {
        AvroReader::try_new(
            reader,
            file_schema,
            batch_size.expect("Batch size must set before open"),
            projected_file_column_names,
        )
    }
}

impl FileSource for AvroSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(private::AvroOpener {
            source: Arc::new(self.clone()),
            object_store,
            file_schema: base_config.file_schema.clone(),
            batch_size: base_config.batch_size,
            projected_file_column_names: base_config.projected_file_column_names(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "avro"
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        Ok(None)
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

mod private {
    use super::*;

    use bytes::Buf;
    use datafusion_datasource::{
        file_meta::FileMeta, file_stream::FileOpenFuture, PartitionedFile,
    };
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroOpener {
        pub source: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,

        pub file_schema: SchemaRef,
        pub batch_size: Option<usize>,
        pub projected_file_column_names: Option<Vec<String>>,
    }

    impl FileOpener for AvroOpener {
        fn open(
            &self,
            file_meta: FileMeta,
            _file: PartitionedFile,
        ) -> Result<FileOpenFuture> {
            let source = Arc::clone(&self.source);
            let object_store = Arc::clone(&self.object_store);
            let file_schema = Arc::clone(&self.file_schema);
            let batch_size = self.batch_size;
            let projected_file_names = self.projected_file_column_names.clone();

            Ok(Box::pin(async move {
                let r = object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = source.open(
                            file,
                            file_schema,
                            batch_size,
                            projected_file_names,
                        )?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = source.open(
                            bytes.reader(),
                            file_schema,
                            batch_size,
                            projected_file_names,
                        )?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }
}
