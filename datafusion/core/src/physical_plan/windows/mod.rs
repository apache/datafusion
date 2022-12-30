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

//! Physical expressions for window functions

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    aggregates,
    expressions::{
        cume_dist, dense_rank, lag, lead, percent_rank, rank, Literal, NthValue, Ntile,
        PhysicalSortExpr, RowNumber,
    },
    type_coercion::coerce,
    udaf, PhysicalExpr,
};
use crate::scalar::ScalarValue;
use arrow::datatypes::Schema;
use datafusion_expr::{
    window_function::{signature_for_built_in, BuiltInWindowFunction, WindowFunction},
    WindowFrame,
};
use datafusion_physical_expr::window::{
    BuiltInWindowFunctionExpr, SlidingAggregateWindowExpr,
};
use std::convert::TryInto;
use std::sync::Arc;

mod bounded_window_agg_exec;
mod window_agg_exec;

pub use bounded_window_agg_exec::BoundedWindowAggExec;
pub use datafusion_physical_expr::window::{
    AggregateWindowExpr, BuiltInWindowExpr, WindowExpr,
};
pub use window_agg_exec::WindowAggExec;

/// Create a physical expression for window function
pub fn create_window_expr(
    fun: &WindowFunction,
    name: String,
    args: &[Arc<dyn PhysicalExpr>],
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    Ok(match fun {
        WindowFunction::AggregateFunction(fun) => {
            let aggregate =
                aggregates::create_aggregate_expr(fun, false, args, input_schema, name)?;
            if !window_frame.start_bound.is_unbounded() {
                Arc::new(SlidingAggregateWindowExpr::new(
                    aggregate,
                    partition_by,
                    order_by,
                    window_frame,
                ))
            } else {
                Arc::new(AggregateWindowExpr::new(
                    aggregate,
                    partition_by,
                    order_by,
                    window_frame,
                ))
            }
        }
        WindowFunction::BuiltInWindowFunction(fun) => Arc::new(BuiltInWindowExpr::new(
            create_built_in_window_expr(fun, args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
        WindowFunction::AggregateUDF(fun) => Arc::new(AggregateWindowExpr::new(
            udaf::create_aggregate_expr(fun.as_ref(), args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
    })
}

fn get_scalar_value_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    index: usize,
) -> Result<Option<ScalarValue>> {
    Ok(if let Some(field) = args.get(index) {
        let tmp = field
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| DataFusionError::NotImplemented(
                format!("There is only support Literal types for field at idx: {index} in Window Function"),
            ))?
            .value()
            .clone();
        Some(tmp)
    } else {
        None
    })
}

fn create_built_in_window_expr(
    fun: &BuiltInWindowFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn BuiltInWindowFunctionExpr>> {
    Ok(match fun {
        BuiltInWindowFunction::RowNumber => Arc::new(RowNumber::new(name)),
        BuiltInWindowFunction::Rank => Arc::new(rank(name)),
        BuiltInWindowFunction::DenseRank => Arc::new(dense_rank(name)),
        BuiltInWindowFunction::PercentRank => Arc::new(percent_rank(name)),
        BuiltInWindowFunction::CumeDist => Arc::new(cume_dist(name)),
        BuiltInWindowFunction::Ntile => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let n: i64 = get_scalar_value_from_args(&coerced_args, 0)?
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "NTILE requires at least 1 argument".to_string(),
                    )
                })?
                .try_into()?;
            let n: u64 = n as u64;
            Arc::new(Ntile::new(name, n))
        }
        BuiltInWindowFunction::Lag => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(&coerced_args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(&coerced_args, 2)?;
            Arc::new(lag(name, data_type, arg, shift_offset, default_value))
        }
        BuiltInWindowFunction::Lead => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(&coerced_args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(&coerced_args, 2)?;
            Arc::new(lead(name, data_type, arg, shift_offset, default_value))
        }
        BuiltInWindowFunction::NthValue => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let n = coerced_args[1]
                .as_any()
                .downcast_ref::<Literal>()
                .unwrap()
                .value();
            let n: i64 = n
                .clone()
                .try_into()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            let n: u32 = n as u32;
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::nth(name, arg, data_type, n)?)
        }
        BuiltInWindowFunction::FirstValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::first(name, arg, data_type))
        }
        BuiltInWindowFunction::LastValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::last(name, arg, data_type))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::aggregates::AggregateFunction;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::CsvExec;
    use crate::physical_plan::{collect, ExecutionPlan};
    use crate::prelude::SessionContext;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::{self, assert_is_pending};
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::cast::as_primitive_array;
    use datafusion_expr::{create_udaf, Accumulator, Volatility};
    use futures::FutureExt;

    fn create_test_schema(partitions: usize) -> Result<(Arc<CsvExec>, SchemaRef)> {
        let csv = test::scan_partitioned_csv(partitions)?;
        let schema = csv.schema();
        Ok((csv, schema))
    }

    #[tokio::test]
    async fn window_function_with_udaf() -> Result<()> {
        #[derive(Debug)]
        struct MyCount(i64);

        impl Accumulator for MyCount {
            fn state(&self) -> Result<Vec<ScalarValue>> {
                Ok(vec![ScalarValue::Int64(Some(self.0))])
            }

            fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
                let array = &values[0];
                self.0 += (array.len() - array.data().null_count()) as i64;
                Ok(())
            }

            fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
                let counts: &Int64Array = arrow::array::as_primitive_array(&states[0]);
                if let Some(c) = &arrow::compute::sum(counts) {
                    self.0 += *c;
                }
                Ok(())
            }

            fn evaluate(&self) -> Result<ScalarValue> {
                Ok(ScalarValue::Int64(Some(self.0)))
            }

            fn size(&self) -> usize {
                std::mem::size_of_val(self)
            }
        }

        let my_count = create_udaf(
            "my_count",
            DataType::Int64,
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::new(MyCount(0)))),
            Arc::new(vec![DataType::Int64]),
        );

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (input, schema) = create_test_schema(1)?;

        let window_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateUDF(Arc::new(my_count)),
                "my_count".to_owned(),
                &[col("c3", &schema)?],
                &[],
                &[],
                Arc::new(WindowFrame::new(false)),
                schema.as_ref(),
            )?],
            input,
            schema.clone(),
            vec![],
            None,
        )?);

        let result: Vec<RecordBatch> = collect(window_exec, task_ctx).await?;
        assert_eq!(result.len(), 1);

        let n_schema_fields = schema.fields().len();
        let columns = result[0].columns();

        let count: &Int64Array = as_primitive_array(&columns[n_schema_fields])?;
        assert_eq!(count.value(0), 100);
        assert_eq!(count.value(99), 100);
        Ok(())
    }

    #[tokio::test]
    async fn window_function() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (input, schema) = create_test_schema(1)?;

        let window_exec = Arc::new(WindowAggExec::try_new(
            vec![
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Count),
                    "count".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Arc::new(WindowFrame::new(false)),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Max),
                    "max".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Arc::new(WindowFrame::new(false)),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Min),
                    "min".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Arc::new(WindowFrame::new(false)),
                    schema.as_ref(),
                )?,
            ],
            input,
            schema.clone(),
            vec![],
            None,
        )?);

        let result: Vec<RecordBatch> = collect(window_exec, task_ctx).await?;
        assert_eq!(result.len(), 1);

        let n_schema_fields = schema.fields().len();
        let columns = result[0].columns();

        // c3 is small int

        let count: &Int64Array = as_primitive_array(&columns[n_schema_fields])?;
        assert_eq!(count.value(0), 100);
        assert_eq!(count.value(99), 100);

        let max: &Int8Array = as_primitive_array(&columns[n_schema_fields + 1])?;
        assert_eq!(max.value(0), 125);
        assert_eq!(max.value(99), 125);

        let min: &Int8Array = as_primitive_array(&columns[n_schema_fields + 2])?;
        assert_eq!(min.value(0), -117);
        assert_eq!(min.value(99), -117);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let window_agg_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col("a", &schema)?],
                &[],
                &[],
                Arc::new(WindowFrame::new(false)),
                schema.as_ref(),
            )?],
            blocking_exec,
            schema,
            vec![],
            None,
        )?);

        let fut = collect(window_agg_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
