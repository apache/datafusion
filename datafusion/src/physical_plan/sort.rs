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

//! Defines the SORT plan

use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::{
    common, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
};
pub use arrow::compute::SortOptions;
use arrow::compute::{lexsort_to_indices, take, SortColumn, TakeOptions};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, error::ArrowError};
use async_trait::async_trait;
use futures::stream::Stream;
use futures::Future;
use pin_project_lite::pin_project;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Sort execution plan
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan
    preserve_partitioning: bool,
}

impl SortExec {
    /// Create a new sort execution plan
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        Ok(Self::new_with_partitioning(expr, input, false))
    }

    /// Create a new sort execution plan with the option to preserve
    /// the partitioning of the input plan
    pub fn new_with_partitioning(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Self {
        Self {
            expr,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }
}

#[async_trait]
impl ExecutionPlan for SortExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        if self.preserve_partitioning {
            self.input.output_partitioning()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        if self.preserve_partitioning {
            Distribution::UnspecifiedDistribution
        } else {
            Distribution::SinglePartition
        }
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SortExec::try_new(
                self.expr.clone(),
                children[0].clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "SortExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if !self.preserve_partitioning {
            if 0 != partition {
                return Err(DataFusionError::Internal(format!(
                    "SortExec invalid partition {}",
                    partition
                )));
            }

            // sort needs to operate on a single partition currently
            if 1 != self.input.output_partitioning().partition_count() {
                return Err(DataFusionError::Internal(
                    "SortExec requires a single input partition".to_owned(),
                ));
            }
        }

        let input = self.input.execute(partition).await?;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);

        let elapsed_compute =
            MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        Ok(Box::pin(SortStream::new(
            input,
            self.expr.clone(),
            output_rows,
            elapsed_compute,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortExec: [{}]", expr.join(","))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn sort_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    expr: &[PhysicalSortExpr],
) -> ArrowResult<RecordBatch> {
    // TODO: pushup the limit expression to sort
    let indices = lexsort_to_indices(
        &expr
            .iter()
            .map(|e| e.evaluate_to_sort_column(&batch))
            .collect::<Result<Vec<SortColumn>>>()
            .map_err(DataFusionError::into_arrow_external_error)?,
        None,
    )?;

    // reorder all rows based on sorted indices
    RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .map(|column| {
                take(
                    column.as_ref(),
                    &indices,
                    // disable bound check overhead since indices are already generated from
                    // the same record batch
                    Some(TakeOptions {
                        check_bounds: false,
                    }),
                )
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?,
    )
}

pin_project! {
    /// stream for sort plan
    struct SortStream {
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<Option<RecordBatch>>>,
        finished: bool,
        schema: SchemaRef,
        output_rows: metrics::Count
    }
}

impl SortStream {
    fn new(
        input: SendableRecordBatchStream,
        expr: Vec<PhysicalSortExpr>,
        output_rows: metrics::Count,
        sort_time: metrics::Time,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();
        let schema = input.schema();
        tokio::spawn(async move {
            let schema = input.schema();
            let sorted_batch = common::collect(input)
                .await
                .map_err(DataFusionError::into_arrow_external_error)
                .and_then(move |batches| {
                    let timer = sort_time.timer();
                    // combine all record batches into one for each column
                    let combined = common::combine_batches(&batches, schema.clone())?;
                    // sort combined record batch
                    let result = combined
                        .map(|batch| sort_batch(batch, schema, &expr))
                        .transpose()?;
                    timer.done();
                    Ok(result)
                });

            tx.send(sorted_batch)
        });

        Self {
            output: rx,
            finished: false,
            schema,
            output_rows,
        }
    }
}

impl Stream for SortStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let output_rows = self.output_rows.clone();

        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Some(Err(ArrowError::ExternalError(Box::new(e)))), // error receiving
                    Ok(result) => result.transpose(),
                };

                if let Some(Ok(batch)) = &result {
                    output_rows.add(batch.num_rows());
                }

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::{
        collect,
        csv::{CsvExec, CsvReadOptions},
    };
    use crate::test;
    use arrow::array::*;
    use arrow::datatypes::*;

    #[tokio::test]
    async fn test_sort() -> Result<()> {
        let schema = test::aggr_test_schema();
        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;

        let sort_exec = Arc::new(SortExec::try_new(
            vec![
                // c1 string column
                PhysicalSortExpr {
                    expr: col("c1", &schema)?,
                    options: SortOptions::default(),
                },
                // c2 uin32 column
                PhysicalSortExpr {
                    expr: col("c2", &schema)?,
                    options: SortOptions::default(),
                },
                // c7 uin8 column
                PhysicalSortExpr {
                    expr: col("c7", &schema)?,
                    options: SortOptions::default(),
                },
            ],
            Arc::new(CoalescePartitionsExec::new(Arc::new(csv))),
        )?);

        let result: Vec<RecordBatch> = collect(sort_exec).await?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    None,
                    None,
                    Some(f32::NAN),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(200.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::try_new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ],
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
        )?);

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> = collect(sort_exec.clone()).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0]);
        let b = as_primitive_array::<Float64Type>(&columns[1]);

        // convert result to strings to allow comparing to expected result containing NaN
        let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
            .map(|i| {
                let aval = if a.is_valid(i) {
                    Some(a.value(i).to_string())
                } else {
                    None
                };
                let bval = if b.is_valid(i) {
                    Some(b.value(i).to_string())
                } else {
                    None
                };
                (aval, bval)
            })
            .collect();

        let expected: Vec<(Option<String>, Option<String>)> = vec![
            (None, Some("10".to_owned())),
            (None, Some("20".to_owned())),
            (Some("NaN".to_owned()), Some("100".to_owned())),
            (Some("NaN".to_owned()), Some("200".to_owned())),
            (Some("3".to_owned()), Some("NaN".to_owned())),
            (Some("2".to_owned()), None),
            (Some("1".to_owned()), Some("NaN".to_owned())),
            (Some("1".to_owned()), None),
        ];

        assert_eq!(expected, result);

        Ok(())
    }
}
