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

//! Defines NTH_VALUE aggregate expression which may specify ordering requirement
//! that can evaluated at runtime during query execution

use std::any::Any;
use std::collections::VecDeque;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, AsArray, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use datafusion_common::utils::{get_row_at_idx, SingleRowListArrayBuilder};
use datafusion_common::{
    assert_or_internal_err, exec_err, not_impl_err, Result, ScalarValue,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    lit, Accumulator, AggregateUDFImpl, Documentation, ExprFunctionExt, ReversedUDAF,
    Signature, SortExpr, Volatility,
};
use datafusion_functions_aggregate_common::merge_arrays::merge_ordered_arrays;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

create_func!(NthValueAgg, nth_value_udaf);

/// Returns the nth value in a group of values.
pub fn nth_value(
    expr: datafusion_expr::Expr,
    n: i64,
    order_by: Vec<SortExpr>,
) -> datafusion_expr::Expr {
    let args = vec![expr, lit(n)];
    if !order_by.is_empty() {
        nth_value_udaf()
            .call(args)
            .order_by(order_by)
            .build()
            .unwrap()
    } else {
        nth_value_udaf().call(args)
    }
}

#[user_doc(
    doc_section(label = "Statistical Functions"),
    description = "Returns the nth value in a group of values.",
    syntax_example = "nth_value(expression, n ORDER BY expression)",
    sql_example = r#"```sql
> SELECT dept_id, salary, NTH_VALUE(salary, 2) OVER (PARTITION BY dept_id ORDER BY salary ASC) AS second_salary_by_dept
  FROM employee;
+---------+--------+-------------------------+
| dept_id | salary | second_salary_by_dept   |
+---------+--------+-------------------------+
| 1       | 30000  | NULL                    |
| 1       | 40000  | 40000                   |
| 1       | 50000  | 40000                   |
| 2       | 35000  | NULL                    |
| 2       | 45000  | 45000                   |
+---------+--------+-------------------------+
```"#,
    argument(
        name = "expression",
        description = "The column or expression to retrieve the nth value from."
    ),
    argument(
        name = "n",
        description = "The position (nth) of the value to retrieve, based on the ordering."
    )
)]
/// Expression for a `NTH_VALUE(..., ... ORDER BY ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NthValueAgg {
    signature: Signature,
}

impl NthValueAgg {
    /// Create a new `NthValueAgg` aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Default for NthValueAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for NthValueAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nth_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let n = match acc_args.exprs[1]
            .as_any()
            .downcast_ref::<Literal>()
            .map(|lit| lit.value())
        {
            Some(ScalarValue::Int64(Some(value))) => {
                if acc_args.is_reversed {
                    -*value
                } else {
                    *value
                }
            }
            _ => {
                return not_impl_err!(
                    "{} not supported for n: {}",
                    self.name(),
                    &acc_args.exprs[1]
                )
            }
        };

        let Some(ordering) = LexOrdering::new(acc_args.order_bys.to_vec()) else {
            return TrivialNthValueAccumulator::try_new(
                n,
                acc_args.return_field.data_type(),
            )
            .map(|acc| Box::new(acc) as _);
        };
        let ordering_dtypes = ordering
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        let data_type = acc_args.expr_fields[0].data_type();
        NthValueAccumulator::try_new(n, data_type, &ordering_dtypes, ordering)
            .map(|acc| Box::new(acc) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields = vec![Field::new_list(
            format_state_name(self.name(), "nth_value"),
            // See COMMENTS.md to understand why nullable is set to true
            Field::new_list_field(args.input_fields[0].data_type().clone(), true),
            false,
        )];
        let orderings = args.ordering_fields.to_vec();
        if !orderings.is_empty() {
            fields.push(Field::new_list(
                format_state_name(self.name(), "nth_value_orderings"),
                Field::new_list_field(DataType::Struct(Fields::from(orderings)), true),
                false,
            ));
        }
        Ok(fields.into_iter().map(Arc::new).collect())
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Reversed(nth_value_udaf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct TrivialNthValueAccumulator {
    /// The `N` value.
    n: i64,
    /// Stores entries in the `NTH_VALUE` result.
    values: VecDeque<ScalarValue>,
    /// Data types of the value.
    datatype: DataType,
}

impl TrivialNthValueAccumulator {
    /// Create a new order-insensitive NTH_VALUE accumulator based on the given
    /// item data type.
    pub fn try_new(n: i64, datatype: &DataType) -> Result<Self> {
        // n cannot be 0
        assert_or_internal_err!(
            n != 0,
            "Nth value indices are 1 based. 0 is invalid index"
        );
        Ok(Self {
            n,
            values: VecDeque::new(),
            datatype: datatype.clone(),
        })
    }

    /// Updates state, with the `values`. Fetch contains missing number of entries for state to be complete
    /// None represents all of the new `values` need to be added to the state.
    fn append_new_data(
        &mut self,
        values: &[ArrayRef],
        fetch: Option<usize>,
    ) -> Result<()> {
        let n_row = values[0].len();
        let n_to_add = if let Some(fetch) = fetch {
            std::cmp::min(fetch, n_row)
        } else {
            n_row
        };
        for index in 0..n_to_add {
            let mut row = get_row_at_idx(values, index)?;
            self.values.push_back(row.swap_remove(0));
            // At index 1, we have n index argument, which is constant.
        }
        Ok(())
    }
}

impl Accumulator for TrivialNthValueAccumulator {
    /// Updates its state with the `values`. Assumes data in the `values` satisfies the required
    /// ordering for the accumulator (across consecutive batches, not just batch-wise).
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !values.is_empty() {
            let n_required = self.n.unsigned_abs() as usize;
            let from_start = self.n > 0;
            if from_start {
                // direction is from start
                let n_remaining = n_required.saturating_sub(self.values.len());
                self.append_new_data(values, Some(n_remaining))?;
            } else {
                // direction is from end
                self.append_new_data(values, None)?;
                let start_offset = self.values.len().saturating_sub(n_required);
                if start_offset > 0 {
                    self.values.drain(0..start_offset);
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if !states.is_empty() {
            // First entry in the state is the aggregation result.
            let n_required = self.n.unsigned_abs() as usize;
            let array_agg_res = ScalarValue::convert_array_to_scalar_vec(&states[0])?;
            for v in array_agg_res.into_iter().flatten() {
                self.values.extend(v);
                if self.values.len() > n_required {
                    // There is enough data collected, can stop merging:
                    break;
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut values_cloned = self.values.clone();
        let values_slice = values_cloned.make_contiguous();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            values_slice,
            &self.datatype,
        ))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        let nth_value_idx = if from_start {
            // index is from start
            let forward_idx = n_required - 1;
            (forward_idx < self.values.len()).then_some(forward_idx)
        } else {
            // index is from end
            self.values.len().checked_sub(n_required)
        };
        if let Some(idx) = nth_value_idx {
            Ok(self.values[idx].clone())
        } else {
            ScalarValue::try_from(self.datatype.clone())
        }
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_vec_deque(&self.values)
            - size_of_val(&self.values)
            + size_of::<DataType>()
    }
}

#[derive(Debug)]
pub struct NthValueAccumulator {
    /// The `N` value.
    n: i64,
    /// Stores entries in the `NTH_VALUE` result.
    values: VecDeque<ScalarValue>,
    /// Stores values of ordering requirement expressions corresponding to each
    /// entry in `values`. This information is used when merging results from
    /// different partitions. For detailed information how merging is done, see
    /// [`merge_ordered_arrays`].
    ordering_values: VecDeque<Vec<ScalarValue>>,
    /// Stores datatypes of expressions inside values and ordering requirement
    /// expressions.
    datatypes: Vec<DataType>,
    /// Stores the ordering requirement of the `Accumulator`.
    ordering_req: LexOrdering,
}

impl NthValueAccumulator {
    /// Create a new order-sensitive NTH_VALUE accumulator based on the given
    /// item data type.
    pub fn try_new(
        n: i64,
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> Result<Self> {
        // n cannot be 0
        assert_or_internal_err!(
            n != 0,
            "Nth value indices are 1 based. 0 is invalid index"
        );
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            n,
            values: VecDeque::new(),
            ordering_values: VecDeque::new(),
            datatypes,
            ordering_req,
        })
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);

        let mut column_wise_ordering_values = vec![];
        let num_columns = fields.len();
        for i in 0..num_columns {
            let column_values = self
                .ordering_values
                .iter()
                .map(|x| x[i].clone())
                .collect::<Vec<_>>();
            let array = if column_values.is_empty() {
                new_empty_array(fields[i].data_type())
            } else {
                ScalarValue::iter_to_array(column_values.into_iter())?
            };
            column_wise_ordering_values.push(array);
        }

        let struct_field = Fields::from(fields);
        let ordering_array =
            StructArray::try_new(struct_field, column_wise_ordering_values, None)?;

        Ok(SingleRowListArrayBuilder::new(Arc::new(ordering_array)).build_list_scalar())
    }

    fn evaluate_values(&self) -> ScalarValue {
        let mut values_cloned = self.values.clone();
        let values_slice = values_cloned.make_contiguous();
        ScalarValue::List(ScalarValue::new_list_nullable(
            values_slice,
            &self.datatypes[0],
        ))
    }

    /// Updates state, with the `values`. Fetch contains missing number of entries for state to be complete
    /// None represents all of the new `values` need to be added to the state.
    fn append_new_data(
        &mut self,
        values: &[ArrayRef],
        fetch: Option<usize>,
    ) -> Result<()> {
        let n_row = values[0].len();
        let n_to_add = if let Some(fetch) = fetch {
            std::cmp::min(fetch, n_row)
        } else {
            n_row
        };
        for index in 0..n_to_add {
            let row = get_row_at_idx(values, index)?;
            self.values.push_back(row[0].clone());
            // At index 1, we have n index argument.
            // Ordering values cover starting from 2nd index to end
            self.ordering_values.push_back(row[2..].to_vec());
        }
        Ok(())
    }
}

impl Accumulator for NthValueAccumulator {
    /// Updates its state with the `values`. Assumes data in the `values` satisfies the required
    /// ordering for the accumulator (across consecutive batches, not just batch-wise).
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        if from_start {
            // direction is from start
            let n_remaining = n_required.saturating_sub(self.values.len());
            self.append_new_data(values, Some(n_remaining))?;
        } else {
            // direction is from end
            self.append_new_data(values, None)?;
            let start_offset = self.values.len().saturating_sub(n_required);
            if start_offset > 0 {
                self.values.drain(0..start_offset);
                self.ordering_values.drain(0..start_offset);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // Second entry stores values received for ordering requirement columns
        // for each aggregation value inside NTH_VALUE list. For each `StructArray`
        // inside this list, we will receive an `Array` that stores values received
        // from its ordering requirement expression. This information is necessary
        // during merging.
        let Some(agg_orderings) = states[1].as_list_opt::<i32>() else {
            return exec_err!("Expects to receive a list array");
        };

        // Stores NTH_VALUE results coming from each partition
        let mut partition_values = vec![self.values.clone()];
        // First entry in the state is the aggregation result.
        let array_agg_res = ScalarValue::convert_array_to_scalar_vec(&states[0])?;
        for v in array_agg_res.into_iter().flatten() {
            partition_values.push(v.into());
        }
        // Stores ordering requirement expression results coming from each partition:
        let mut partition_ordering_values = vec![self.ordering_values.clone()];
        let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;
        // Extract value from struct to ordering_rows for each group/partition:
        for partition_ordering_rows in orderings.into_iter().flatten() {
            let ordering_values = partition_ordering_rows.into_iter().map(|ordering_row| {
                let ScalarValue::Struct(s_array) = ordering_row else {
                    return exec_err!(
                        "Expects to receive ScalarValue::Struct(Some(..), _) but got: {:?}",
                        ordering_row.data_type()
                    );
                };
                s_array
                    .columns()
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, 0))
                    .collect()
            }).collect::<Result<VecDeque<_>>>()?;
            partition_ordering_values.push(ordering_values);
        }

        let sort_options = self
            .ordering_req
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();
        let (new_values, new_orderings) = merge_ordered_arrays(
            &mut partition_values,
            &mut partition_ordering_values,
            &sort_options,
        )?;
        self.values = new_values.into();
        self.ordering_values = new_orderings.into();
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate_values(), self.evaluate_orderings()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        let nth_value_idx = if from_start {
            // index is from start
            let forward_idx = n_required - 1;
            (forward_idx < self.values.len()).then_some(forward_idx)
        } else {
            // index is from end
            self.values.len().checked_sub(n_required)
        };
        if let Some(idx) = nth_value_idx {
            Ok(self.values[idx].clone())
        } else {
            ScalarValue::try_from(self.datatypes[0].clone())
        }
    }

    fn size(&self) -> usize {
        let mut total = size_of_val(self) + ScalarValue::size_of_vec_deque(&self.values)
            - size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total += size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total += dtype.size() - size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}
