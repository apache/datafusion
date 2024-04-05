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

use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::compute::{self, lexsort_to_indices, SortColumn, SortOptions};
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::{compare_rows, get_arrayref_at_indices, get_row_at_idx};
use datafusion_common::{
    arrow_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDFImpl, Expr,
    Signature, Volatility,
};
use datafusion_physical_expr_common::aggregate::utils::get_sort_options;
use datafusion_physical_expr_common::expressions;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use std::any::Any;
use std::fmt::Debug;

// TODO: macro udaf
// make_udf_function!(
//     FirstValue,
//     first_value,
//     value: Expr,
//     "Returns the first value in a group of values.",
//     first_value_fn
// );

pub struct FirstValue {
    signature: Signature,
    aliases: Vec<String>,
    accumulator: AccumulatorFactoryFunction,
}

impl Debug for FirstValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FirstValue")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl FirstValue {
    pub fn new(accumulator: AccumulatorFactoryFunction) -> Self {
        Self {
            aliases: vec![String::from("FIRST_VALUE")],
            signature: Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable),
            accumulator,
        }
    }
}

impl AggregateUDFImpl for FirstValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "FIRST_VALUE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(acc_args)
    }

    fn state_fields(
        &self,
        name: &str,
        value_type: DataType,
        ordering_fields: Vec<Field>,
    ) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(name, "first_value"),
            value_type,
            true,
        )];
        fields.extend(ordering_fields);
        fields.push(Field::new("is_set", DataType::Boolean, true));
        Ok(fields)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

// /// Creates a new UDAF with a specific signature, state type and return type.
// /// The signature and state type must match the `Accumulator's implementation`.
// /// TOOD: We plan to move aggregate function to its own crate. This function will be deprecated then.
// pub fn create_first_value() -> AggregateUDF {
//     let accumulator = Arc::new(create_first_value_accumulator);
//     AggregateUDF::from(FirstValue::new(accumulator))
// }

pub(crate) fn create_first_value_accumulator(
    acc_args: AccumulatorArgs,
) -> Result<Box<dyn Accumulator>> {
    let mut all_sort_orders = vec![];

    // Construct PhysicalSortExpr objects from Expr objects:
    let mut sort_exprs = vec![];
    for expr in acc_args.sort_exprs {
        if let Expr::Sort(sort) = expr {
            if let Expr::Column(col) = sort.expr.as_ref() {
                let name = &col.name;
                let e = expressions::column::col(name, acc_args.schema)?;
                sort_exprs.push(PhysicalSortExpr {
                    expr: e,
                    options: SortOptions {
                        descending: !sort.asc,
                        nulls_first: sort.nulls_first,
                    },
                });
            }
        }
    }
    if !sort_exprs.is_empty() {
        all_sort_orders.extend(sort_exprs);
    }

    let ordering_req = all_sort_orders;

    let ordering_dtypes = ordering_req
        .iter()
        .map(|e| e.expr.data_type(acc_args.schema))
        .collect::<Result<Vec<_>>>()?;

    let requirement_satisfied = ordering_req.is_empty();

    FirstValueAccumulator::try_new(
        acc_args.data_type,
        &ordering_dtypes,
        ordering_req,
        acc_args.ignore_nulls,
    )
    .map(|acc| Box::new(acc.with_requirement_satisfied(requirement_satisfied)) as _)
}

#[derive(Debug)]
pub struct FirstValueAccumulator {
    first: ScalarValue,
    // At the beginning, `is_set` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_set` flag and do not update `first` anymore.
    is_set: bool,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator. These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // Stores whether incoming data already satisfies the ordering requirement.
    requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let requirement_satisfied = ordering_req.is_empty();
        ScalarValue::try_from(data_type).map(|first| Self {
            first,
            is_set: false,
            orderings,
            ordering_req,
            requirement_satisfied,
            ignore_nulls,
        })
    }

    pub fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.first = row[0].clone();
        self.orderings = row[1..].to_vec();
        self.is_set = true;
    }

    fn get_first_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in FIRST_VALUE");
        };
        if self.requirement_satisfied {
            // Get first entry according to the pre-existing ordering (0th index):
            if self.ignore_nulls {
                // If ignoring nulls, find the first non-null value.
                for i in 0..value.len() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                // If not ignoring nulls, return the first value if it exists.
                return Ok((!value.is_empty()).then_some(0));
            }
        }
        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: values.clone(),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        if self.ignore_nulls {
            let indices = lexsort_to_indices(&sort_columns, None)?;
            // If ignoring nulls, find the first non-null value.
            for index in indices.iter().flatten() {
                if !value.is_null(index as usize) {
                    return Ok(Some(index as usize));
                }
            }
            Ok(None)
        } else {
            let indices = lexsort_to_indices(&sort_columns, Some(1))?;
            Ok((!indices.is_empty()).then_some(indices.value(0) as _))
        }
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.first.clone()];
        result.extend(self.orderings.iter().cloned());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                self.update_with_new_row(&row);
            }
        } else if !self.requirement_satisfied {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                let orderings = &row[1..];
                if compare_rows(
                    &self.orderings,
                    orderings,
                    &get_sort_options(&self.ordering_req),
                )?
                .is_gt()
                {
                    self.update_with_new_row(&row);
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        let filtered_states = filter_states_according_to_is_set(states, flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_cols =
            convert_to_sort_cols(&filtered_states[1..is_set_idx], &self.ordering_req);

        let ordered_states = if sort_cols.is_empty() {
            // When no ordering is given, use the existing state as is:
            filtered_states
        } else {
            let indices = lexsort_to_indices(&sort_cols, None)?;
            get_arrayref_at_indices(&filtered_states, &indices)?
        };
        if !ordered_states[0].is_empty() {
            let first_row = get_row_at_idx(&ordered_states, 0)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let first_ordering = &first_row[1..is_set_idx];
            let sort_options = get_sort_options(&self.ordering_req);
            // Either there is no existing value, or there is an earlier version in new data.
            if !self.is_set
                || compare_rows(&self.orderings, first_ordering, &sort_options)?.is_gt()
            {
                // Update with first value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                self.update_with_new_row(&first_row[0..is_set_idx]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - std::mem::size_of_val(&self.orderings)
    }
}

/// Filters states according to the `is_set` flag at the last column and returns
/// the resulting states.
///
/// TODO: This function can be private once the `LAST_VALUE` function is moved to the `aggregate-functions` crate.
pub fn filter_states_according_to_is_set(
    states: &[ArrayRef],
    flags: &BooleanArray,
) -> Result<Vec<ArrayRef>> {
    states
        .iter()
        .map(|state| compute::filter(state, flags).map_err(|e| arrow_datafusion_err!(e)))
        .collect::<Result<Vec<_>>>()
}

/// Combines array refs and their corresponding orderings to construct `SortColumn`s.
///
/// TODO: This function can be private once the `LAST_VALUE` function is moved to the `aggregate-functions` crate.
pub fn convert_to_sort_cols(
    arrs: &[ArrayRef],
    sort_exprs: &[PhysicalSortExpr],
) -> Vec<SortColumn> {
    arrs.iter()
        .zip(sort_exprs.iter())
        .map(|(item, sort_expr)| SortColumn {
            values: item.clone(),
            options: Some(sort_expr.options),
        })
        .collect::<Vec<_>>()
}
