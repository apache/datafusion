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

use crate::franz_sinks::FranzSink;

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
        // todo: replace with execute_stream_partitioned()
        let mut stream: SendableRecordBatchStream = self.execute_stream().await.unwrap();

        loop {
            let rb = stream.next().await.transpose();
            match rb {
                Ok(result) => {
                    if let Some(batch) = result {
                        if batch.num_rows() > 0 {
                            let _ = sink.write_records(batch).await;
                        }
                    } else {
                        log::warn!("No RecordBatch in stream");
                    }
                }
                Err(err) => {
                    log::error!("Error reading stream {:?}", err);
                }
            }
        }
    }
}
