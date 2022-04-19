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

//! TableFun (UDTFs) Physical Node

use std::any::Any;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    ColumnStatistics, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
};
use arrow::array::{
    make_array, Array, ArrayData, ArrayRef, Capacities, MutableArrayData,
};
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::TableFunctionExpr;

use super::expressions::{Column, PhysicalSortExpr};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{RecordBatchStream, SendableRecordBatchStream, Statistics};
use crate::execution::context::TaskContext;
use async_trait::async_trait;
use futures::stream::Stream;
use futures::stream::StreamExt;

/// Execution plan for a table_fun
#[derive(Debug)]
pub struct TableFunExec {
    //
    // schema
    // input
    /// The expressions stored as tuples of (expression, output column name)
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the node has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl TableFunExec {
    /// Create a TableFun
    pub fn try_new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields: Result<Vec<Field>> = expr
            .iter()
            .map(|(e, name)| {
                let mut field = Field::new(
                    name,
                    e.data_type(&input_schema)?,
                    e.nullable(&input_schema)?,
                );
                field.set_metadata(get_field_metadata(e, &input_schema));

                Ok(field)
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ));

        Ok(Self {
            expr,
            schema,
            input: input.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The expressions stored as tuples of (expression, output column name)
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

#[async_trait]
impl ExecutionPlan for TableFunExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> bool {
        // tell optimizer this operator doesn't reorder its input
        true
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(TableFunExec::try_new(
                self.expr.clone(),
                children[0].clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "TableFunExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TableFunStream {
            schema: self.schema.clone(),
            expr: self.expr.iter().map(|x| x.0.clone()).collect(),
            input: self.input.execute(partition, context).await?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self
                    .expr
                    .iter()
                    .map(|(e, alias)| {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{} as {}", e, alias)
                        } else {
                            e
                        }
                    })
                    .collect();

                write!(f, "TableFunExec: expr=[{}]", expr.join(", "))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        stats_table_fun(
            self.input.statistics(),
            self.expr.iter().map(|(e, _)| Arc::clone(e)),
        )
    }
}

/// If e is a direct column reference, returns the field level
/// metadata for that field, if any. Otherwise returns None
fn get_field_metadata(
    e: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Option<BTreeMap<String, String>> {
    let name = if let Some(column) = e.as_any().downcast_ref::<Column>() {
        column.name()
    } else {
        return None;
    };

    input_schema
        .field_with_name(name)
        .ok()
        .and_then(|f| f.metadata().as_ref().cloned())
}

fn stats_table_fun(
    stats: Statistics,
    exprs: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
) -> Statistics {
    let column_statistics = stats.column_statistics.map(|input_col_stats| {
        exprs
            .map(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    input_col_stats[col.index()].clone()
                } else {
                    // TODO stats: estimate more statistics from expressions
                    // (expressions should compute their statistics themselves)
                    ColumnStatistics::default()
                }
            })
            .collect()
    });

    Statistics {
        is_exact: stats.is_exact,
        num_rows: stats.num_rows,
        column_statistics,
        // TODO stats: knowing the type of the new columns we can guess the output size
        total_byte_size: None,
    }
}

impl TableFunStream {
    fn batch(&self, batch: &RecordBatch) -> ArrowResult<RecordBatch> {
        let arrays: Vec<((ColumnarValue, Vec<usize>), bool)> = self
            .expr
            .iter()
            .map(|expr| {
                let t_expr = expr.as_any().clone().downcast_ref::<TableFunctionExpr>();
                if t_expr.is_some() {
                    (t_expr.unwrap().evaluate_table(batch).unwrap(), true)
                } else {
                    ((expr.evaluate(batch).unwrap(), Vec::new()), false)
                }
            })
            .collect();

        // Detect count sizes of batch sections
        let mut batches_sizes: Vec<usize> = Vec::new();
        for ((_, indexes), _) in arrays.iter() {
            let mut prev_value: usize = 0;
            for (i, end_of_batch) in indexes.iter().enumerate() {
                let batch_size = end_of_batch - prev_value + 1;
                if batches_sizes.len() > i {
                    if batches_sizes[i] < batch_size {
                        batches_sizes[i] = batch_size;
                    }
                } else {
                    batches_sizes.push(batch_size);
                }
                prev_value = end_of_batch.clone();
            }
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        // Iterate arrays to fill columns
        for (_, ((arr, indexes), is_fun)) in arrays.iter().enumerate() {
            let col_arr = match arr {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(_) => panic!(""),
            };

            let mut sections: Vec<ArrayRef> = Vec::new();
            let mut lengths: Vec<usize> = Vec::new();

            for (i_batch, size) in batches_sizes.iter().enumerate() {
                let (start_i_of_batch, current_batch_size) =
                    index_and_size_of_sec(indexes.clone(), i_batch, *is_fun);
                if *is_fun {
                    sections.push(col_arr.slice(start_i_of_batch, current_batch_size));
                    lengths.push(current_batch_size);
                } else {
                    let mut concat: Vec<_> = Vec::new();
                    let arr = col_arr.slice(start_i_of_batch, current_batch_size);
                    for _ in 0..=size - 1 {
                        concat.push(arr.as_ref());
                    }

                    sections.push(kernels::concat::concat(&concat).unwrap());
                    lengths.push(size.clone());
                }
            }

            let sections: Vec<&ArrayData> = sections.iter().map(|s| s.data()).collect();

            let mut mutable = match sections[0].data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    MutableArrayData::with_capacities(
                        sections,
                        true,
                        Capacities::Binary(1, None),
                    )
                }
                _ => MutableArrayData::new(sections, true, 1),
            };

            for (i, len) in lengths.iter().enumerate() {
                mutable.extend(i, 0, *len);
                mutable.extend_nulls(batches_sizes[i] - len);
            }

            columns.push(make_array(mutable.freeze()));
        }

        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

/// TableFun iterator
struct TableFunStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl Stream for TableFunStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch(&batch)),
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for TableFunStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn index_and_size_of_sec(
    batch_mark: Vec<usize>,
    section_index: usize,
    is_fun: bool,
) -> (usize, usize) {
    let start_index = if section_index > 0 && is_fun {
        batch_mark[section_index - 1]
    } else {
        0
    };
    let section_size: usize = if is_fun {
        batch_mark[section_index] - start_index + 1
    } else {
        1
    };

    (start_index, section_size)
}
