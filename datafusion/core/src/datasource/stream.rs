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

//! TableProvider for stream sources, such as FIFO files

use std::any::Any;
use std::fmt::Formatter;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader, RecordBatchWriter};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::task::spawn_blocking;

use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_plan::common::AbortOnDropSingle;
use datafusion_physical_plan::insert::{DataSink, FileSinkExec};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

use crate::datasource::provider::TableProviderFactory;
use crate::datasource::{create_ordering, TableProvider};
use crate::execution::context::SessionState;

/// A [`TableProviderFactory`] for [`StreamTable`]
#[derive(Debug, Default)]
pub struct StreamTableFactory {}

#[async_trait]
impl TableProviderFactory for StreamTableFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
        let location = cmd.location.clone();
        let encoding = cmd.file_type.parse()?;

        let config = StreamConfig::new_file(schema, location.into())
            .with_encoding(encoding)
            .with_order(cmd.order_exprs.clone())
            .with_header(cmd.has_header);

        Ok(Arc::new(StreamTable(Arc::new(config))))
    }
}

/// The data encoding for [`StreamTable`]
#[derive(Debug, Clone)]
pub enum StreamEncoding {
    /// CSV records
    Csv,
    /// Newline-delimited JSON records
    Json,
}

impl FromStr for StreamEncoding {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => plan_err!("Unrecognised StreamEncoding {}", s),
        }
    }
}

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct StreamConfig {
    schema: SchemaRef,
    location: PathBuf,
    encoding: StreamEncoding,
    header: bool,
    order: Vec<Vec<Expr>>,
}

impl StreamConfig {
    /// Stream data from the file at `location`
    pub fn new_file(schema: SchemaRef, location: PathBuf) -> Self {
        Self {
            schema,
            location,
            encoding: StreamEncoding::Csv,
            order: vec![],
            header: false,
        }
    }

    /// Specify a sort order for the stream
    pub fn with_order(mut self, order: Vec<Vec<Expr>>) -> Self {
        self.order = order;
        self
    }

    /// Specify whether the file has a header (only applicable for [`StreamEncoding::Csv`])
    pub fn with_header(mut self, header: bool) -> Self {
        self.header = header;
        self
    }

    /// Specify an encoding for the stream
    pub fn with_encoding(mut self, encoding: StreamEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    fn reader(&self) -> Result<Box<dyn RecordBatchReader>> {
        let file = File::open(&self.location)?;
        let schema = self.schema.clone();
        match &self.encoding {
            StreamEncoding::Csv => {
                let reader = arrow::csv::ReaderBuilder::new(schema)
                    .with_header(self.header)
                    .build(file)?;

                Ok(Box::new(reader))
            }
            StreamEncoding::Json => {
                let reader = arrow::json::ReaderBuilder::new(schema)
                    .build(BufReader::new(file))?;

                Ok(Box::new(reader))
            }
        }
    }

    fn writer(&self) -> Result<Box<dyn RecordBatchWriter>> {
        match &self.encoding {
            StreamEncoding::Csv => {
                let header = self.header && !self.location.exists();
                let file = OpenOptions::new().write(true).open(&self.location)?;
                let writer = arrow::csv::WriterBuilder::new()
                    .with_header(header)
                    .build(file);

                Ok(Box::new(writer))
            }
            StreamEncoding::Json => {
                let file = OpenOptions::new().write(true).open(&self.location)?;
                Ok(Box::new(arrow::json::LineDelimitedWriter::new(file)))
            }
        }
    }
}

/// A [`TableProvider`] for a stream source, such as a FIFO file
pub struct StreamTable(Arc<StreamConfig>);

impl StreamTable {
    /// Create a new [`StreamTable`] for the given `StreamConfig`
    pub fn new(config: Arc<StreamConfig>) -> Self {
        Self(config)
    }
}

#[async_trait]
impl TableProvider for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => {
                let projected = self.0.schema.project(p)?;
                create_ordering(&projected, &self.0.order)?
            }
            None => create_ordering(self.0.schema.as_ref(), &self.0.order)?,
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            self.0.schema.clone(),
            vec![Arc::new(StreamRead(self.0.clone())) as _],
            projection,
            projected_schema,
            true,
        )?))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ordering = match self.0.order.first() {
            Some(x) => {
                let schema = self.0.schema.as_ref();
                let orders = create_ordering(schema, std::slice::from_ref(x))?;
                let ordering = orders.into_iter().next().unwrap();
                Some(ordering.into_iter().map(Into::into).collect())
            }
            None => None,
        };

        Ok(Arc::new(FileSinkExec::new(
            input,
            Arc::new(StreamWrite(self.0.clone())),
            self.0.schema.clone(),
            ordering,
        )))
    }
}

struct StreamRead(Arc<StreamConfig>);

impl PartitionStream for StreamRead {
    fn schema(&self) -> &SchemaRef {
        &self.0.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.0.clone();
        let schema = self.0.schema.clone();
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 2);
        let tx = builder.tx();
        builder.spawn_blocking(move || {
            let reader = config.reader()?;
            for b in reader {
                if tx.blocking_send(b.map_err(Into::into)).is_err() {
                    break;
                }
            }
            Ok(())
        });
        builder.build()
    }
}

#[derive(Debug)]
struct StreamWrite(Arc<StreamConfig>);

impl DisplayAs for StreamWrite {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[async_trait]
impl DataSink for StreamWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let config = self.0.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<RecordBatch>(2);
        // Note: FIFO Files support poll so this could use AsyncFd
        let write = AbortOnDropSingle::new(spawn_blocking(move || {
            let mut count = 0_u64;
            let mut writer = config.writer()?;
            while let Some(batch) = receiver.blocking_recv() {
                count += batch.num_rows() as u64;
                writer.write(&batch)?;
            }
            Ok(count)
        }));

        while let Some(b) = data.next().await.transpose()? {
            if sender.send(b).await.is_err() {
                break;
            }
        }
        drop(sender);
        write.await.unwrap()
    }
}
