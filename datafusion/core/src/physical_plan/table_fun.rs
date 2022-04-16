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

//! Defines the projection execution plan. A projection determines which columns or expressions
//! are returned from a query. The SQL statement `SELECT a, b, a+b FROM t1` is an example
//! of a projection on table `t1` where the expressions `a`, `b`, and `a+b` are the
//! projection expressions. `SELECT` without `FROM` will only evaluate expressions.

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
    make_builder, ArrayBuilder, ArrayRef, Int64Array, PrimitiveArray, PrimitiveBuilder,
    StringBuilder, BooleanBuilder, BinaryBuilder, StructBuilder, DecimalBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder, LargeStringBuilder, StringArray, LargeStringArray, BooleanArray, BinaryArray, LargeBinaryArray, DecimalArray, StructArray,
};
use arrow::datatypes::{
    self, ArrowPrimitiveType, DataType, Field, Int32Type, Int64Type, Schema, SchemaRef,
};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::TableFunctionExpr;

use super::expressions::{Column, PhysicalSortExpr};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{RecordBatchStream, SendableRecordBatchStream, Statistics};
use crate::execution::context::TaskContext;
use async_trait::async_trait;
use futures::stream::Stream;
use futures::stream::StreamExt;

/// Execution plan for a projection
#[derive(Debug)]
pub struct TableFunExec {
    //
    // schema
    // input
    /// The projection expressions stored as tuples of (expression, output column name)
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

// select asd, my_func(1, asd) from table

impl TableFunExec {
    /// Create a projection on an input
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

    /// The projection expressions stored as tuples of (expression, output column name)
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
        &self,
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
        // records time on drop

        // let formatted = arrow::util::pretty::pretty_format_batches(&[batch.clone()])
        //     .unwrap()
        //     .to_string();

        // println!("{}", formatted);

        let arrays: Vec<(Vec<ColumnarValue>, bool)> = self
            .expr
            .iter()
            .map(|expr| {
                let t_expr = expr.as_any().clone().downcast_ref::<TableFunctionExpr>();
                if t_expr.is_some() {
                    // let qwe = t_expr.unwrap().evaluate_table(batch).unwrap();
                    (t_expr.unwrap().evaluate_table(batch).unwrap(), true)
                } else {
                    let mut vec = Vec::new();
                    vec.push(expr.evaluate(batch).unwrap());

                    (vec, false)
                }
            })
            .collect();

        // TODO: builder types
        // !!!!!!!!!!!!!!!!!!!!
        // let mut columns: Vec<PrimitiveBuilder<_>> = arrays
        let mut columns: Vec<Box<dyn ArrayBuilder>> = arrays
            .iter()
            .map(|(v, _)| {
                make_builder(&v.first().unwrap().data_type(), 1)
                // PrimitiveBuilder::<Int64Type>::new(1)
            })
            .collect();

        let batches_count = arrays.iter().map(|(a, _)| a.len()).max().unwrap_or(0);
        for (i_column, (arr, if_fun)) in arrays.iter().enumerate() {
            for i in 0..=batches_count - 1 {
                let rows_count: usize = arrays
                    .iter()
                    .map(|(a, is_fun)| {
                        if *is_fun {
                            let a = if a.len() > i { &a[i] } else { &a[a.len() - 1] };
                            if let ColumnarValue::Array(arr) = &a {
                                // if let ColumnarValue::Array(arr) = &a[i] {
                                return arr.len();
                            }
                        }

                        1
                    })
                    .max()
                    .unwrap_or(0);

                let batch = if *if_fun {
                    if arr.len() > i {
                        &arr[i]
                    } else {
                        &arr[arr.len() - 1]
                    }
                } else {
                    &arr[0]
                };

                // TODO: ColumnarValue::Scalar
                for i_row in 0..=rows_count - 1 {
                    let index = if *if_fun { i_row } else { i };

                    let mut builder = columns[i_column].as_any_mut();
                        
                    if let ColumnarValue::Array(arr) = batch {
                        // !!!!!!!!!!!!!!!!!!!!
                        // let arr =
                        //     arr.as_any().downcast_ref::<PrimitiveArray<Int64Type>>().unwrap();
                        if arr.len() > index {
                            // builder.append_value(arr.values()[index])?;

                            append_value_from_array(arr, index, builder);
                            // append_null(builder);
                            // columns[i_column].append_value(arr.values()[index])?;
                        } else {
                            append_null(builder);

                            // builder.append_null()?;
                            // columns[i_column].append_null()?;
                        }
                    // } else if let ColumnarValue::Scalar(val) = batch {
                    //     // !!!!!!!!!!!!!!!!!!!!
                    //     if let ScalarValue::Int64(val) = val {
                    //         if index == 0 && val.is_some() {
                    //             builder.append_value(val.unwrap())?;
                    //             // columns[i_column].append_value(val.unwrap())?;
                    //         } else {
                    //             builder.append_null()?;
                    //             // columns[i_column].append_null()?;
                    //         }
                    //     } else {
                    //         builder.append_null()?;
                    //         // columns[i_column].append_null()?;
                    //     }
                    } else {
                        append_null(builder);

                        // builder.append_null()?;
                        // columns[i_column].append_null()?;
                    }
                }
            }
        }

        // let arrays = kek.map(|r| r.map(|v| v.into_array(batch.num_rows())))
        // .collect::<Result<Vec<_>>>()?;

        let columns: Vec<ArrayRef> = columns
            .iter_mut()
            .map(|c| Arc::new(c.finish()) as ArrayRef)
            .collect::<_>();

        RecordBatch::try_new(self.schema.clone(), columns)
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
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampSecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::TimestampMillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::TimestampMicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampNanosecondType>>();
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
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time32MillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time64MicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time64NanosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalYearMonthType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalDayTimeType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::IntervalMonthDayNanoType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationSecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMillisecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMicrosecondType>>();
    if arr.is_some() {
        arr.unwrap().append_null().unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationNanosecondType>>();
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
        let array = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int8Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Int8Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int16Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Int16Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Int32Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Int32Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt8Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::UInt8Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt16Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::UInt16Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt32Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::UInt32Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::UInt64Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::UInt64Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float16Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Float16Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float32Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Float32Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Float64Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Float64Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampSecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::TimestampSecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::TimestampMillisecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::TimestampMillisecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::TimestampMicrosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::TimestampMicrosecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::TimestampNanosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::TimestampNanosecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date32Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Date32Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Date64Type>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Date64Type>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to.downcast_mut::<PrimitiveBuilder<datatypes::Time32SecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Time32SecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time32MillisecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Time32MillisecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time64MicrosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Time64MicrosecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::Time64NanosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::Time64NanosecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalYearMonthType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::IntervalYearMonthType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::IntervalDayTimeType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::IntervalDayTimeType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr = to
        .downcast_mut::<PrimitiveBuilder<datatypes::IntervalMonthDayNanoType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::IntervalMonthDayNanoType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationSecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::DurationSecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMillisecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::DurationMillisecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationMicrosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::DurationMicrosecondType>>().unwrap();
        arr.unwrap().append_value(array.values()[index]).unwrap();
        return;
    }
    let arr =
        to.downcast_mut::<PrimitiveBuilder<datatypes::DurationNanosecondType>>();
    if arr.is_some() {
        let array = array.as_any().downcast_ref::<PrimitiveArray<datatypes::DurationNanosecondType>>().unwrap();
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

    // let arr = to.downcast_mut::<StructBuilder>();
    // if arr.is_some() {
    //     let array = array.as_any().downcast_ref::<StructArray>().unwrap();
    //     arr.unwrap().append_value(array.value(index)).unwrap();
    //     return;
    // }
    
    panic!("MutableArrayBuilder - try to use unsupported type")
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
