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
    make_builder, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray,
    BooleanBuilder, DecimalArray, DecimalBuilder, FixedSizeBinaryBuilder,
    LargeBinaryArray, LargeBinaryBuilder, LargeStringArray, LargeStringBuilder,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, StructBuilder,
};
use arrow::datatypes::{self, Field, Int64Type, Schema, SchemaRef};
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
        // Table UDFs return Vec<ColumnarValue> otherwise Column always is ColumnarValue
        // We wrap Column result to Vec (to have the same structure with udtfs results)
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

        // We are going to map udtf results with simple columns
        // 1. Create ArrayBuilder for each column
        // 2. Detect count of batches && sizes of batches
        let mut columns: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        let mut batches_sizes: Vec<usize> = Vec::new();
        for ((val, indexes), _) in arrays.iter() {
            columns.push(make_builder(&val.data_type(), 1));
            let mut prev_value: usize = 0;
            for (i, end_of_batch) in indexes.iter().enumerate() {
                if batches_sizes.len() > i {
                    let batch_size = end_of_batch - prev_value + 1;
                    if  batches_sizes[i] < batch_size {
                        batches_sizes[i] = batch_size;
                    }
                } else {
                    batches_sizes.push(end_of_batch - prev_value + 1);
                }
                prev_value = end_of_batch.clone();
            }
        }

        // Iterate arrays to fill columns
        for (i_column, ((arr, indexes), is_fun)) in arrays.iter().enumerate() {
            let column_arr = match arr {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(_) => todo!(),
            };
            let builder = columns[i_column].as_any_mut();

            // Iterate batches
            for (i_batch, max_batch_size) in batches_sizes.iter().enumerate() {
                let start_i_of_batch = if i_batch > 0 && *is_fun {
                    indexes[i_batch-1]
                } else {
                    0
                };
                let current_batch_size: usize = if *is_fun {
                    indexes[i_batch] - start_i_of_batch + 1
                } else {
                    1
                };

                // Iterate rows
                for i_row in 0..=*max_batch_size-1 {
                    // Now we fill up the current column with data from the current batch
                    // For column - copy scalar value
                    // For UDTF - ARR[Index] or NULL

                    // Example: SELECT asd, generate_series(1,asd), generate_series(1,2) FROM (select 1 asd UNION ALL select 2 asd) x;
                    // Select unused, generate_series(1,asd) from (select 8 as unused, 1 asd union all select 25 as unused, 3 asd)
                    // Result:
                    // 1  1   1
                    // 1 NULL 2
                    // 2  1   1
                    // 2  2   2
                    if *is_fun {
                        if i_row >= current_batch_size {
                            append_null(builder);
                        } else {
                            append_value_from_array(column_arr, start_i_of_batch+i_row, builder);
                        }
                    } else {
                        append_value_from_array(column_arr, i_batch, builder);
                    }
                }
            }
        }

        let columns: Vec<ArrayRef> = columns
            .iter_mut()
            .map(|c| Arc::new(c.finish()) as ArrayRef)
            .collect::<_>();

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

fn append_null(to: &mut dyn Any) {
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int64Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int8Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int16Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int32Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt8Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt16Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt32Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt64Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float16Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float32Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float64Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampSecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampMillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampMicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampNanosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date32Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date64Type>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time32SecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time32MillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time64MicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time64NanosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalYearMonthType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalDayTimeType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalMonthDayNanoType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationSecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationNanosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<BooleanBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<BinaryBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<LargeBinaryBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<StringBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<LargeStringBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<FixedSizeBinaryBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<DecimalBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to.downcast_mut::<StructBuilder>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }

    panic!("MutableArrayBuilder - try to use unsupported type")
}

fn append_value_from_array(array: &ArrayRef, index: usize, to: &mut dyn Any) {
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int64Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int8Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Int8Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int16Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Int16Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int32Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Int32Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt8Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::UInt8Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt16Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::UInt16Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt32Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::UInt32Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt64Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::UInt64Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float16Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Float16Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float32Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Float32Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float64Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Float64Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampSecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::TimestampSecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampMillisecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::TimestampMillisecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampMicrosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::TimestampMicrosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampNanosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::TimestampNanosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date32Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Date32Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date64Type>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Date64Type>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time32SecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Time32SecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time32MillisecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Time32MillisecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time64MicrosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Time64MicrosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time64NanosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::Time64NanosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalYearMonthType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::IntervalYearMonthType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalDayTimeType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::IntervalDayTimeType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalMonthDayNanoType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::IntervalMonthDayNanoType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationSecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::DurationSecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMillisecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::DurationMillisecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMicrosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::DurationMicrosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::DurationNanosecondType>>();
    if arr.is_some() {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<datatypes::DurationNanosecondType>>()
            .unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<BooleanBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<BinaryBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<LargeBinaryBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<StringBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<LargeStringBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<FixedSizeBinaryBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }
    let arr = to.downcast_mut::<DecimalBuilder>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<DecimalArray>().unwrap();
        arr.unwrap().append_value(array.value(index)).unwrap();
        return;
    }

    // TODO: Support Struct
    let arr = to.downcast_mut::<StructBuilder>();
    // if arr.is_some() {
    //     let array = array.as_any().downcast_ref::<StructArray>().unwrap();
    //     arr.unwrap().append(array.value(index), true).unwrap();
    //     return;
    // }

    panic!("MutableArrayBuilder - try to use unsupported type")
}
