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

use crate::datasource::provider::TableProviderFactory;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use arrow::csv::{ReaderBuilder, WriterBuilder};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_plan::common::AbortOnDropSingle;
use datafusion_physical_plan::insert::{DataSink, FileSinkExec};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// A [`TableProviderFactory`] for [`StreamTable`]
#[derive(Default)]
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
        Ok(Arc::new(StreamTable(Arc::new(StreamConfig {
            schema,
            sort: None, // TODO
            location: location.into(),
        }))))
    }
}

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct StreamConfig {
    schema: SchemaRef,
    location: PathBuf,
    sort: Option<LexOrdering>,
}

impl StreamConfig {
    /// Stream data from the file at `location`
    pub fn new_file(schema: SchemaRef, location: PathBuf) -> Self {
        Self {
            schema,
            location,
            sort: None,
        }
    }

    /// Specify a sort order for the stream
    pub fn with_sort(mut self, sort: Option<LexOrdering>) -> Self {
        self.sort = sort;
        self
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
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            self.0.schema.clone(),
            vec![Arc::new(StreamRead(self.0.clone())) as _],
            projection,
            self.0.sort.clone(),
            true,
        )?))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sort = self.0.sort.as_ref();
        let ordering = sort.map(|o| o.iter().map(|e| e.clone().into()).collect());

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
            let file = File::open(&config.location)?;
            let reader = ReaderBuilder::new(config.schema.clone()).build(file)?;
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
            let file = OpenOptions::new().write(true).open(&config.location)?;
            let mut count = 0_u64;
            let mut writer = WriterBuilder::new().with_header(false).build(file);
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
