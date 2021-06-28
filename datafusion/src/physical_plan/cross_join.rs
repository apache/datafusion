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

//! Defines the cross join plan for loading the left side of the cross join
//! and producing batches in parallel for the right partitions

use futures::{lock::Mutex, StreamExt};
use std::{any::Any, sync::Arc, task::Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use futures::{Stream, TryStreamExt};

use super::{
    coalesce_partitions::CoalescePartitionsExec, hash_utils::check_join_is_valid,
};
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use async_trait::async_trait;
use std::time::Instant;

use super::{
    coalesce_batches::concat_batches, memory::MemoryStream, DisplayFormatType,
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use log::debug;

/// Data of the left side
type JoinLeftData = RecordBatch;

/// executes partitions in parallel and combines them into a set of
/// partitions by combining all values from the left with all values on the right
#[derive(Debug)]
pub struct CrossJoinExec {
    /// left (build) side which gets loaded in memory
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    build_side: Arc<Mutex<Option<JoinLeftData>>>,
}

impl CrossJoinExec {
    /// Tries to create a new [CrossJoinExec].
    /// # Error
    /// This function errors when left and right schema's can't be combined
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;

        let left_schema = left.schema();
        let left_fields = left_schema.fields().iter();
        let right_schema = right.schema();

        let right_fields = right_schema.fields().iter();

        // left then right
        let all_columns = left_fields.chain(right_fields).cloned().collect();

        let schema = Arc::new(Schema::new(all_columns));

        Ok(CrossJoinExec {
            left,
            right,
            schema,
            build_side: Arc::new(Mutex::new(None)),
        })
    }

    /// left (build) side which gets loaded in memory
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side which gets combined with left side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

#[async_trait]
impl ExecutionPlan for CrossJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            2 => Ok(Arc::new(CrossJoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "CrossJoinExec wrong number of children".to_string(),
            )),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // we only want to compute the build side once
        let left_data = {
            let mut build_side = self.build_side.lock().await;

            match build_side.as_ref() {
                Some(stream) => stream.clone(),
                None => {
                    let start = Instant::now();

                    // merge all left parts into a single stream
                    let merge = CoalescePartitionsExec::new(self.left.clone());
                    let stream = merge.execute(0).await?;

                    // Load all batches and count the rows
                    let (batches, num_rows) = stream
                        .try_fold((Vec::new(), 0usize), |mut acc, batch| async {
                            acc.1 += batch.num_rows();
                            acc.0.push(batch);
                            Ok(acc)
                        })
                        .await?;
                    let merged_batch =
                        concat_batches(&self.left.schema(), &batches, num_rows)?;
                    *build_side = Some(merged_batch.clone());

                    debug!(
                        "Built build-side of cross join containing {} rows in {} ms",
                        num_rows,
                        start.elapsed().as_millis()
                    );

                    merged_batch
                }
            }
        };

        let stream = self.right.execute(partition).await?;

        if left_data.num_rows() == 0 {
            return Ok(Box::pin(MemoryStream::try_new(
                vec![],
                self.schema.clone(),
                None,
            )?));
        }

        Ok(Box::pin(CrossJoinStream {
            schema: self.schema.clone(),
            left_data,
            right: stream,
            right_batch: Arc::new(std::sync::Mutex::new(None)),
            left_index: 0,
            num_input_batches: 0,
            num_input_rows: 0,
            num_output_batches: 0,
            num_output_rows: 0,
            join_time: 0,
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CrossJoinExec")
            }
        }
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct CrossJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// data from the left side
    left_data: JoinLeftData,
    /// right
    right: SendableRecordBatchStream,
    /// Current value on the left
    left_index: usize,
    /// Current batch being processed from the right side
    right_batch: Arc<std::sync::Mutex<Option<RecordBatch>>>,
    /// number of input batches
    num_input_batches: usize,
    /// number of input rows
    num_input_rows: usize,
    /// number of batches produced
    num_output_batches: usize,
    /// number of rows produced
    num_output_rows: usize,
    /// total time for joining probe-side batches to the build-side batches
    join_time: usize,
}

impl RecordBatchStream for CrossJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
fn build_batch(
    left_index: usize,
    batch: &RecordBatch,
    left_data: &RecordBatch,
    schema: &Schema,
) -> ArrowResult<RecordBatch> {
    // Repeat value on the left n times
    let arrays = left_data
        .columns()
        .iter()
        .map(|arr| {
            let scalar = ScalarValue::try_from_array(arr, left_index)?;
            Ok(scalar.to_array_of_size(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|x| x.into_arrow_external_error())?;

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        arrays
            .iter()
            .chain(batch.columns().iter())
            .cloned()
            .collect(),
    )
}

#[async_trait]
impl Stream for CrossJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.left_index > 0 && self.left_index < self.left_data.num_rows() {
            let start = Instant::now();
            let right_batch = {
                let right_batch = self.right_batch.lock().unwrap();
                right_batch.clone().unwrap()
            };
            let result =
                build_batch(self.left_index, &right_batch, &self.left_data, &self.schema);
            self.num_input_rows += right_batch.num_rows();
            if let Ok(ref batch) = result {
                self.join_time += start.elapsed().as_millis() as usize;
                self.num_output_batches += 1;
                self.num_output_rows += batch.num_rows();
            }
            self.left_index += 1;
            return Poll::Ready(Some(result));
        }
        self.left_index = 0;
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let start = Instant::now();
                    let result = build_batch(
                        self.left_index,
                        &batch,
                        &self.left_data,
                        &self.schema,
                    );
                    self.num_input_batches += 1;
                    self.num_input_rows += batch.num_rows();
                    if let Ok(ref batch) = result {
                        self.join_time += start.elapsed().as_millis() as usize;
                        self.num_output_batches += 1;
                        self.num_output_rows += batch.num_rows();
                    }
                    self.left_index = 1;

                    let mut right_batch = self.right_batch.lock().unwrap();
                    *right_batch = Some(batch);

                    Some(result)
                }
                other => {
                    debug!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {} ms",
                        self.num_input_batches,
                        self.num_input_rows,
                        self.num_output_batches,
                        self.num_output_rows,
                        self.join_time
                    );
                    other
                }
            })
    }
}
