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

//! Apache Avro [`FileFormat`] abstractions

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::avro_to_arrow::read_avro_schema_from_reader;
use crate::source::AvroSource;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use datafusion_common::internal_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::GetExt;
use datafusion_common::DEFAULT_AVRO_EXTENSION;
use datafusion_common::{Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_session::Session;

use async_trait::async_trait;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

#[derive(Default)]
/// Factory struct used to create [`AvroFormat`]
pub struct AvroFormatFactory;

impl AvroFormatFactory {
    /// Creates an instance of [`AvroFormatFactory`]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for AvroFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AvroFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Debug for AvroFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroFormatFactory").finish()
    }
}

impl GetExt for AvroFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".avro" -> "avro"
        DEFAULT_AVRO_EXTENSION[1..].to_string()
    }
}

/// Avro [`FileFormat`] implementation.
#[derive(Default, Debug)]
pub struct AvroFormat;

#[async_trait]
impl FileFormat for AvroFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        AvroFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => internal_err!("Avro FileFormat does not support compression."),
        }
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(mut file, _) => {
                    read_avro_schema_from_reader(&mut file)?
                }
                GetResultPayload::Stream(_) => {
                    // TODO: Fetching entire file to get schema is potentially wasteful
                    let data = r.bytes().await?;
                    read_avro_schema_from_reader(&mut data.as_ref())?
                }
            };
            schemas.push(schema);
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = FileScanConfigBuilder::from(conf)
            .with_source(self.file_source())
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AvroSource::new())
    }
}
