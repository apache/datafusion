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

//! Stream and channel implementations for window function expressions.

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::{
    Column, ColumnStatistics, DisplayFormatType, Distribution, EquivalenceProperties,
    ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics, WindowExpr,
};
use arrow::compute::concat_batches;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion_physical_expr::rewrite::TreeNodeRewritable;
use datafusion_physical_expr::EquivalentClass;
use futures::stream::Stream;
use futures::{ready, StreamExt};
use log::debug;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Window execution plan
#[derive(Debug)]
pub struct WindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Schema before the window
    input_schema: SchemaRef,
    /// Partition Keys
    pub partition_keys: Vec<Arc<dyn PhysicalExpr>>,
    /// Sort Keys
    pub sort_keys: Option<Vec<PhysicalSortExpr>>,
    /// The output ordering
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        sort_keys: Option<Vec<PhysicalSortExpr>>,
    ) -> Result<Self> {
        let schema = create_schema(&input_schema, &window_expr)?;
        let schema = Arc::new(schema);
        let window_expr_len = window_expr.len();
        // Although WindowAggExec does not change the output ordering from the input, but can not return the output ordering
        // from the input directly, need to adjust the column index to align with the new schema.
        let output_ordering = input
            .output_ordering()
            .map(|sort_exprs| {
                let new_sort_exprs: Result<Vec<PhysicalSortExpr>> = sort_exprs
                    .iter()
                    .map(|e| {
                        let new_expr = e.expr.clone().transform_down(&|e| {
                            Ok(e.as_any().downcast_ref::<Column>().map(|col| {
                                Arc::new(Column::new(
                                    col.name(),
                                    window_expr_len + col.index(),
                                ))
                                    as Arc<dyn PhysicalExpr>
                            }))
                        })?;
                        Ok(PhysicalSortExpr {
                            expr: new_expr,
                            options: e.options,
                        })
                    })
                    .collect();
                new_sort_exprs
            })
            .map_or(Ok(None), |v| v.map(Some))?;

        Ok(Self {
            input,
            window_expr,
            schema,
            input_schema,
            partition_keys,
            sort_keys,
            output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any window functions are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

impl ExecutionPlan for WindowAggExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // Although WindowAggExec does not change the output partitioning from the input, but can not return the output partitioning
        // from the input directly, need to adjust the column index to align with the new schema.
        let window_expr_len = self.window_expr.len();
        let input_partitioning = self.input.output_partitioning();
        match input_partitioning {
            Partitioning::RoundRobinBatch(size) => Partitioning::RoundRobinBatch(size),
            Partitioning::UnknownPartitioning(size) => {
                Partitioning::UnknownPartitioning(size)
            }
            Partitioning::Hash(exprs, size) => {
                let new_exprs = exprs
                    .into_iter()
                    .map(|expr| {
                        expr.transform_down(&|e| {
                            Ok(e.as_any().downcast_ref::<Column>().map(|col| {
                                Arc::new(Column::new(
                                    col.name(),
                                    window_expr_len + col.index(),
                                ))
                                    as Arc<dyn PhysicalExpr>
                            }))
                        })
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                Partitioning::Hash(new_exprs, size)
            }
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn maintains_input_order(&self) -> bool {
        true
    }

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        let sort_keys = self.sort_keys.as_deref();
        vec![sort_keys]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys.is_empty() {
            debug!("No partition defined for WindowAggExec!!!");
            vec![Distribution::SinglePartition]
        } else {
            //TODO support PartitionCollections if there is no common partition columns in the window_expr
            vec![Distribution::HashPartitioned(self.partition_keys.clone())]
        }
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        // Although WindowAggExec does not change the equivalence properties from the input, but can not return the equivalence properties
        // from the input directly, need to adjust the column index to align with the new schema.
        let window_expr_len = self.window_expr.len();
        let mut new_properties = EquivalenceProperties::new(self.schema());
        let new_eq_classes = self
            .input
            .equivalence_properties()
            .classes()
            .iter()
            .map(|prop| {
                let new_head = Column::new(
                    prop.head().name(),
                    window_expr_len + prop.head().index(),
                );
                let new_others = prop
                    .others()
                    .iter()
                    .map(|col| Column::new(col.name(), window_expr_len + col.index()))
                    .collect::<Vec<_>>();
                EquivalentClass::new(new_head, new_others)
            })
            .collect::<Vec<_>>();
        new_properties.extend(new_eq_classes);
        new_properties
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(WindowAggExec::try_new(
            self.window_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.partition_keys.clone(),
            self.sort_keys.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(WindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
        ));
        Ok(stream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "WindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
        }
        Ok(())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stat = self.input.statistics();
        let win_cols = self.window_expr.len();
        let input_cols = self.input_schema.fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = vec![ColumnStatistics::default(); win_cols];
        if let Some(input_col_stats) = input_stat.column_statistics {
            column_statistics.extend(input_col_stats);
        } else {
            column_statistics.extend(vec![ColumnStatistics::default(); input_cols]);
        }
        Statistics {
            is_exact: input_stat.is_exact,
            num_rows: input_stat.num_rows,
            column_statistics: Some(column_statistics),
            total_byte_size: None,
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    fields.extend_from_slice(input_schema.fields());
    Ok(Schema::new(fields))
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    baseline_metrics: BaselineMetrics,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            schema,
            input,
            batches: vec![],
            finished: false,
            window_expr,
            baseline_metrics,
        }
    }

    fn compute_aggregates(&self) -> ArrowResult<RecordBatch> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        if self.batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let batch = concat_batches(&self.input.schema(), &self.batches)?;

        // calculate window cols
        let mut columns = compute_window_aggregates(&self.window_expr, &batch)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expressions are always prepended to the columns
        columns.extend_from_slice(batch.columns());
        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

impl Stream for WindowAggStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            let result = match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.batches.push(batch);
                    continue;
                }
                Some(Err(e)) => Err(e),
                None => self.compute_aggregates(),
            };

            self.finished = true;

            return Poll::Ready(Some(result));
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
