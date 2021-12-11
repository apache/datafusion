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
use crate::logical_plan::window_frames::WindowFrame;
use crate::physical_plan::{
    aggregates,
    expressions::{
        cume_dist, dense_rank, lag, lead, percent_rank, rank, Literal, NthValue,
        PhysicalSortExpr, RowNumber,
    },
    type_coercion::coerce,
    window_functions::{
        signature_for_built_in, BuiltInWindowFunction, BuiltInWindowFunctionExpr,
        WindowFunction,
    },
    PhysicalExpr, WindowExpr,
};
use crate::scalar::ScalarValue;
use arrow::datatypes::Schema;
use std::convert::TryInto;
use std::ops::Range;
use std::sync::Arc;

mod aggregate;
mod built_in;
mod window_agg_exec;

pub use aggregate::AggregateWindowExpr;
pub use built_in::BuiltInWindowExpr;
pub use window_agg_exec::WindowAggExec;

/// Create a physical expression for window function
pub fn create_window_expr(
    fun: &WindowFunction,
    name: String,
    args: &[Arc<dyn PhysicalExpr>],
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Option<WindowFrame>,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    Ok(match fun {
        WindowFunction::AggregateFunction(fun) => Arc::new(AggregateWindowExpr::new(
            aggregates::create_aggregate_expr(fun, false, args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
        WindowFunction::BuiltInWindowFunction(fun) => Arc::new(BuiltInWindowExpr::new(
            create_built_in_window_expr(fun, args, input_schema, name)?,
            partition_by,
            order_by,
        )),
    })
}

fn get_scalar_value_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    index: usize,
) -> Option<ScalarValue> {
    args.get(index).map(|v| {
        v.as_any()
            .downcast_ref::<Literal>()
            .unwrap()
            .value()
            .clone()
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
        BuiltInWindowFunction::Lag => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(&coerced_args, 1)
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(&coerced_args, 2);
            Arc::new(lag(name, data_type, arg, shift_offset, default_value))
        }
        BuiltInWindowFunction::Lead => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(&coerced_args, 1)
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(&coerced_args, 2);
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
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
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
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Window function with {:?} not yet implemented",
                fun
            )))
        }
    })
}

/// Given a partition range, and the full list of sort partition points, given that the sort
/// partition points are sorted using [partition columns..., order columns...], the split
/// boundaries would align (what's sorted on [partition columns...] would definitely be sorted
/// on finer columns), so this will use binary search to find ranges that are within the
/// partition range and return the valid slice.
pub(crate) fn find_ranges_in_range<'a>(
    partition_range: &Range<usize>,
    sort_partition_points: &'a [Range<usize>],
) -> &'a [Range<usize>] {
    let start_idx = sort_partition_points
        .partition_point(|sort_range| sort_range.start < partition_range.start);
    let end_idx = start_idx
        + sort_partition_points[start_idx..]
            .partition_point(|sort_range| sort_range.end <= partition_range.end);
    &sort_partition_points[start_idx..end_idx]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::object_store::local::LocalFileSystem;
    use crate::physical_plan::aggregates::AggregateFunction;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{CsvExec, PhysicalPlanConfig};
    use crate::physical_plan::{collect, Statistics};
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::{self, assert_is_pending};
    use crate::test_util::{self, aggr_test_schema};
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use futures::FutureExt;

    fn create_test_schema(partitions: usize) -> Result<(Arc<CsvExec>, SchemaRef)> {
        let schema = test_util::aggr_test_schema();
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: aggr_test_schema(),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        let input = Arc::new(csv);
        Ok((input, schema))
    }

    #[tokio::test]
    async fn window_function() -> Result<()> {
        let (input, schema) = create_test_schema(1)?;

        let window_exec = Arc::new(WindowAggExec::try_new(
            vec![
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Count),
                    "count".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Max),
                    "max".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Min),
                    "min".to_owned(),
                    &[col("c3", &schema)?],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
            ],
            input,
            schema.clone(),
        )?);

        let result: Vec<RecordBatch> = collect(window_exec).await?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        // c3 is small int

        let count: &UInt64Array = as_primitive_array(&columns[0]);
        assert_eq!(count.value(0), 100);
        assert_eq!(count.value(99), 100);

        let max: &Int8Array = as_primitive_array(&columns[1]);
        assert_eq!(max.value(0), 125);
        assert_eq!(max.value(99), 125);

        let min: &Int8Array = as_primitive_array(&columns[2]);
        assert_eq!(min.value(0), -117);
        assert_eq!(min.value(99), -117);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
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
                Some(WindowFrame::default()),
                schema.as_ref(),
            )?],
            blocking_exec,
            schema,
        )?);

        let fut = collect(window_agg_exec);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
