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

#![allow(missing_docs)]

use std::time::Duration;

use super::{DataFrame, LogicalPlanBuilder};
use crate::error::{DataFusionError, Result};
use crate::execution::SendableRecordBatchStream;
use crate::logical_expr::Expr;

use futures::StreamExt;

use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::fs::File;
use std::sync::Arc;

impl DataFrame {
    /// Return a new DataFrame that adds the result of evaluating one or more
    /// window functions ([`Expr::WindowFunction`]) to the existing columns
    ///
    pub fn franz_window(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_length: Duration,
    ) -> Result<Self> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .franz_window(group_expr, aggr_expr, window_length)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// TODO
    pub async fn sink(self, mut sink: Box<dyn FranzSink>) -> Result<DataFusionError> {
        let mut stream: SendableRecordBatchStream = self.execute_stream().await.unwrap();

        loop {
            let rb = stream.next().await.transpose();
            if let Ok(Some(batch)) = rb {
                // println!(
                //     "{}",
                //     arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
                // );

                // let mut writer = LineDelimitedWriter::new(std::io::stdout().lock());
                // let _ = writer.write(&batch);

                let _ = sink.write_stuff(batch).await;
            }
            // println!("<<<<< window end >>>>>>");
        }
    }
}

#[async_trait]
pub trait FranzSink {
    async fn write_stuff(&mut self, batch: RecordBatch) -> Result<(), DataFusionError>;
}

pub struct FileWriter {
    file: Arc<tokio::sync::Mutex<LineDelimitedWriter<File>>>,
}

// todo
impl FileWriter {
    // todo
    pub fn new(fname: &str) -> Result<Self> {
        // todo
        let file = File::create(fname)?;
        let writer = LineDelimitedWriter::new(file);
        Ok(Self {
            file: Arc::new(tokio::sync::Mutex::new(writer)),
        })
    }
}

#[async_trait]
impl FranzSink for FileWriter {
    async fn write_stuff(&mut self, batch: RecordBatch) -> Result<()> {
        let mut file = self.file.lock().await;

        file.write(&batch).map_err(DataFusionError::from)?;
        file.finish().map_err(DataFusionError::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
