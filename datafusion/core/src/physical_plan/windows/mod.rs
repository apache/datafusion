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

use crate::physical_plan::{
    aggregates,
    expressions::{
        cume_dist, dense_rank, lag, lead, percent_rank, rank, Literal, NthValue, Ntile,
        PhysicalSortExpr, RowNumber,
    },
    udaf, ExecutionPlan, PhysicalExpr,
};
use arrow::datatypes::Schema;
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    window_function::{BuiltInWindowFunction, WindowFunction},
    PartitionEvaluator, WindowFrame, WindowUDF,
};
use datafusion_physical_expr::{
    window::{BuiltInWindowFunctionExpr, SlidingAggregateWindowExpr},
    AggregateExpr,
};
use std::borrow::Borrow;
use std::convert::TryInto;
use std::sync::Arc;

mod bounded_window_agg_exec;
mod window_agg_exec;

pub use bounded_window_agg_exec::BoundedWindowAggExec;
pub use bounded_window_agg_exec::PartitionSearchMode;
use datafusion_common::utils::longest_consecutive_prefix;
use datafusion_physical_expr::equivalence::OrderingEquivalenceBuilder;
use datafusion_physical_expr::utils::{convert_to_expr, get_indices_of_matching_exprs};
pub use datafusion_physical_expr::window::{
    BuiltInWindowExpr, PlainAggregateWindowExpr, WindowExpr,
};
use datafusion_physical_expr::{OrderingEquivalenceProperties, PhysicalSortRequirement};
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
            let aggregate = aggregates::create_aggregate_expr(
                fun,
                false,
                args,
                &[],
                input_schema,
                name,
            )?;
            window_expr_from_aggregate_expr(
                partition_by,
                order_by,
                window_frame,
                aggregate,
            )
        }
        WindowFunction::BuiltInWindowFunction(fun) => Arc::new(BuiltInWindowExpr::new(
            create_built_in_window_expr(fun, args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
        WindowFunction::AggregateUDF(fun) => {
            let aggregate =
                udaf::create_aggregate_expr(fun.as_ref(), args, input_schema, name)?;
            window_expr_from_aggregate_expr(
                partition_by,
                order_by,
                window_frame,
                aggregate,
            )
        }
        WindowFunction::WindowUDF(fun) => Arc::new(BuiltInWindowExpr::new(
            create_udwf_window_expr(fun, args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
    })
}

/// Creates an appropriate [`WindowExpr`] based on the window frame and
fn window_expr_from_aggregate_expr(
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    aggregate: Arc<dyn AggregateExpr>,
) -> Arc<dyn WindowExpr> {
    // Is there a potentially unlimited sized window frame?
    let unbounded_window = window_frame.start_bound.is_unbounded();

    if !unbounded_window {
        Arc::new(SlidingAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
        ))
    } else {
        Arc::new(PlainAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
        ))
    }
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
            let n: i64 = get_scalar_value_from_args(args, 0)?
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
            let arg = args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(args, 2)?;
            Arc::new(lag(name, data_type, arg, shift_offset, default_value))
        }
        BuiltInWindowFunction::Lead => {
            let arg = args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            let shift_offset = get_scalar_value_from_args(args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value = get_scalar_value_from_args(args, 2)?;
            Arc::new(lead(name, data_type, arg, shift_offset, default_value))
        }
        BuiltInWindowFunction::NthValue => {
            let arg = args[0].clone();
            let n = args[1].as_any().downcast_ref::<Literal>().unwrap().value();
            let n: i64 = n
                .clone()
                .try_into()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            let n: u32 = n as u32;
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::nth(name, arg, data_type, n)?)
        }
        BuiltInWindowFunction::FirstValue => {
            let arg = args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::first(name, arg, data_type))
        }
        BuiltInWindowFunction::LastValue => {
            let arg = args[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Arc::new(NthValue::last(name, arg, data_type))
        }
    })
}

/// Creates a `BuiltInWindowFunctionExpr` suitable for a user defined window function
fn create_udwf_window_expr(
    fun: &Arc<WindowUDF>,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn BuiltInWindowFunctionExpr>> {
    // need to get the types into an owned vec for some reason
    let input_types: Vec<_> = args
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<_>>()?;

    // figure out the output type
    let data_type = (fun.return_type)(&input_types)?;
    Ok(Arc::new(WindowUDFExpr {
        fun: Arc::clone(fun),
        args: args.to_vec(),
        name,
        data_type,
    }))
}

/// Implements [`BuiltInWindowFunctionExpr`] for [`WindowUDF`]
#[derive(Clone, Debug)]
struct WindowUDFExpr {
    fun: Arc<WindowUDF>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Display name
    name: String,
    /// result type
    data_type: Arc<DataType>,
}

impl BuiltInWindowFunctionExpr for WindowUDFExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        Ok(Field::new(
            &self.name,
            self.data_type.as_ref().clone(),
            nullable,
        ))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        (self.fun.partition_evaluator_factory)()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        None
    }
}

pub(crate) fn calc_requirements<
    T: Borrow<Arc<dyn PhysicalExpr>>,
    S: Borrow<PhysicalSortExpr>,
>(
    partition_by_exprs: impl IntoIterator<Item = T>,
    orderby_sort_exprs: impl IntoIterator<Item = S>,
) -> Option<Vec<PhysicalSortRequirement>> {
    let mut sort_reqs = partition_by_exprs
        .into_iter()
        .map(|partition_by| {
            PhysicalSortRequirement::new(partition_by.borrow().clone(), None)
        })
        .collect::<Vec<_>>();
    for element in orderby_sort_exprs.into_iter() {
        let PhysicalSortExpr { expr, options } = element.borrow();
        if !sort_reqs.iter().any(|e| e.expr.eq(expr)) {
            sort_reqs.push(PhysicalSortRequirement::new(expr.clone(), Some(*options)));
        }
    }
    // Convert empty result to None. Otherwise wrap result inside Some()
    (!sort_reqs.is_empty()).then_some(sort_reqs)
}

/// This function calculates the indices such that when partition by expressions reordered with this indices
/// resulting expressions define a preset for existing ordering.
// For instance, if input is ordered by a, b, c and PARTITION BY b, a is used
// This vector will be [1, 0]. It means that when we iterate b,a columns with the order [1, 0]
// resulting vector (a, b) is a preset of the existing ordering (a, b, c).
pub(crate) fn get_ordered_partition_by_indices(
    partition_by_exprs: &[Arc<dyn PhysicalExpr>],
    input: &Arc<dyn ExecutionPlan>,
) -> Vec<usize> {
    let input_ordering = input.output_ordering().unwrap_or(&[]);
    let input_ordering_exprs = convert_to_expr(input_ordering);
    let equal_properties = || input.equivalence_properties();
    let input_places = get_indices_of_matching_exprs(
        &input_ordering_exprs,
        partition_by_exprs,
        equal_properties,
    );
    let mut partition_places = get_indices_of_matching_exprs(
        partition_by_exprs,
        &input_ordering_exprs,
        equal_properties,
    );
    partition_places.sort();
    let first_n = longest_consecutive_prefix(partition_places);
    input_places[0..first_n].to_vec()
}

pub(crate) fn window_ordering_equivalence(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
    window_expr: &[Arc<dyn WindowExpr>],
) -> OrderingEquivalenceProperties {
    // We need to update the schema, so we can not directly use
    // `input.ordering_equivalence_properties()`.
    let mut builder = OrderingEquivalenceBuilder::new(schema.clone())
        .with_equivalences(input.equivalence_properties())
        .with_existing_ordering(input.output_ordering().map(|elem| elem.to_vec()))
        .extend(input.ordering_equivalence_properties());

    for expr in window_expr {
        if let Some(builtin_window_expr) =
            expr.as_any().downcast_ref::<BuiltInWindowExpr>()
        {
            builtin_window_expr
                .add_equal_orderings(&mut builder, || input.equivalence_properties());
        }
    }
    builder.build()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::physical_plan::CsvExec;
    use crate::physical_plan::aggregates::AggregateFunction;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::{collect, ExecutionPlan};
    use crate::prelude::SessionContext;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::{self, assert_is_pending, csv_exec_sorted};
    use arrow::array::*;
    use arrow::compute::SortOptions;
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

    fn create_test_schema2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    /// make PhysicalSortExpr with default options
    fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        sort_expr_options(name, schema, SortOptions::default())
    }

    /// PhysicalSortExpr with specified options
    fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    #[tokio::test]
    async fn test_get_partition_by_ordering() -> Result<()> {
        let test_schema = create_test_schema2()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        // Input is ordered by a,b,c,d
        let input = csv_exec_sorted(&test_schema, sort_exprs, true);
        let test_data = vec![
            (vec!["a", "b"], vec![0, 1]),
            (vec!["b", "a"], vec![1, 0]),
            (vec!["b", "a", "c"], vec![1, 0, 2]),
            (vec!["d", "b", "a"], vec![2, 1]),
            (vec!["d", "e", "a"], vec![2]),
        ];
        for (pb_names, expected) in test_data {
            let pb_exprs = pb_names
                .iter()
                .map(|name| col(name, &test_schema))
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(
                get_ordered_partition_by_indices(&pb_exprs, &input),
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_calc_requirements() -> Result<()> {
        let schema = create_test_schema2()?;
        let test_data = vec![
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", true, true)],
                vec![("a", None), ("b", Some((true, true)))],
            ),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (vec!["a"], vec![("a", true, true)], vec![("a", None)]),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a"],
                vec![("b", true, true), ("c", false, false)],
                vec![
                    ("a", None),
                    ("b", Some((true, true))),
                    ("c", Some((false, false))),
                ],
            ),
            // PARTITION BY a, c, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a", "c"],
                vec![("b", true, true), ("c", false, false)],
                vec![("a", None), ("c", None), ("b", Some((true, true)))],
            ),
        ];
        for (pb_params, ob_params, expected_params) in test_data {
            let mut partitionbys = vec![];
            for col_name in pb_params {
                partitionbys.push(col(col_name, &schema)?);
            }

            let mut orderbys = vec![];
            for (col_name, descending, nulls_first) in ob_params {
                let expr = col(col_name, &schema)?;
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                orderbys.push(PhysicalSortExpr { expr, options });
            }

            let mut expected: Option<Vec<PhysicalSortRequirement>> = None;
            for (col_name, reqs) in expected_params {
                let options = reqs.map(|(descending, nulls_first)| SortOptions {
                    descending,
                    nulls_first,
                });
                let expr = col(col_name, &schema)?;
                let res = PhysicalSortRequirement::new(expr, options);
                if let Some(expected) = &mut expected {
                    expected.push(res);
                } else {
                    expected = Some(vec![res]);
                }
            }
            assert_eq!(calc_requirements(partitionbys, orderbys), expected);
        }
        Ok(())
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
                self.0 += (array.len() - array.null_count()) as i64;
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
        )?);

        let fut = collect(window_agg_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
