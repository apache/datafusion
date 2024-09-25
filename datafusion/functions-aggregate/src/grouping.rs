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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::array::BooleanArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::UInt32Type;
use datafusion_common::internal_datafusion_err;
use datafusion_common::internal_err;
use datafusion_common::plan_err;
use datafusion_common::{not_impl_err, Result};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::EmitTo;
use datafusion_expr::GroupsAccumulator;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExpr;

make_udaf_expr_and_func!(
    Grouping,
    grouping,
    expression,
    "Returns a bitmap where bit i is 1 if this row is aggregated across the ith argument to GROUPING and 0 otherwise.",
    grouping_udaf
);

pub struct Grouping {
    signature: Signature,
}

impl fmt::Debug for Grouping {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        f.debug_struct("Grouping")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Grouping {
    fn default() -> Self {
        Self::new()
    }
}

impl Grouping {
    /// Create a new GROUPING aggregate function.
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    /// Create an accumulator for GROUPING(grouping_args) in a GROUP BY over group_exprs
    /// A special creation function is necessary because GROUPING has unusual input requirements.
    pub fn create_grouping_accumulator(
        &self,
        grouping_args: &[Arc<dyn PhysicalExpr>],
        group_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if grouping_args.len() > 32 {
            return plan_err!(
                "GROUPING is supported for up to 32 columns. Consider another \
                GROUPING statement if you need to aggregate over more columns."
            );
        }
        // The PhysicalExprs of grouping_exprs must be Column PhysicalExpr. Because if
        // the group by PhysicalExpr in SQL is non-Column PhysicalExpr, then there is
        // a ProjectionExec before AggregateExec to convert the non-column PhysicalExpr
        // to Column PhysicalExpr.
        let column_index =
            |expr: &Arc<dyn PhysicalExpr>| match expr.as_any().downcast_ref::<Column>() {
                Some(column) => Ok(column.index()),
                None => internal_err!("Grouping doesn't support expr: {}", expr),
            };
        let group_by_columns: Result<Vec<_>> =
            group_exprs.iter().map(|(e, _)| column_index(e)).collect();
        let group_by_columns = group_by_columns?;

        let arg_columns: Result<Vec<_>> =
            grouping_args.iter().map(column_index).collect();
        let expr_indices: Result<Vec<_>> = arg_columns?
            .iter()
            .map(|arg| {
                group_by_columns
                    .iter()
                    .position(|gb| arg == gb)
                    .ok_or_else(|| {
                        internal_datafusion_err!("Invalid grouping set indices.")
                    })
            })
            .collect();

        Ok(Box::new(GroupingAccumulator {
            grouping_ids: vec![],
            expr_indices: expr_indices?,
        }))
    }
}

impl AggregateUDFImpl for Grouping {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "grouping"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "grouping"),
            DataType::UInt32,
            true,
        )])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("The GROUPING function requires a GROUP BY context.")
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // Use `create_grouping_accumulator` instead.
        not_impl_err!("GROUPING is not supported when invoked this way.")
    }
}

struct GroupingAccumulator {
    // Grouping ID value for each group
    grouping_ids: Vec<u32>,
    // Indices of GROUPING arguments as they appear in the GROUPING SET
    expr_indices: Vec<usize>,
}

impl GroupingAccumulator {
    fn mask_to_id(&self, mask: &[bool]) -> Result<u32> {
        let mut id: u32 = 0;
        // rightmost entry is the LSB
        for (i, &idx) in self.expr_indices.iter().rev().enumerate() {
            match mask.get(idx) {
                Some(true) => id |= 1 << i,
                Some(false) => {}
                None => {
                    return internal_err!(
                        "Index out of bounds while calculating GROUPING id."
                    )
                }
            }
        }
        Ok(id)
    }
}

impl GroupsAccumulator for GroupingAccumulator {
    fn update_batch(
        &mut self,
        _values: &[ArrayRef],
        _group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        _total_num_groups: usize,
    ) -> Result<()> {
        // No-op since GROUPING doesn't care about values
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to merge_batch");
        self.grouping_ids.resize(total_num_groups, 0);
        let other_ids = values[0].as_primitive::<UInt32Type>();
        accumulate(group_indices, other_ids, None, |group_index, group_id| {
            self.grouping_ids[group_index] |= group_id;
        });
        Ok(())
    }

    fn update_groupings(
        &mut self,
        group_indices: &[usize],
        group_mask: &[bool],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grouping_ids.resize(total_num_groups, 0);
        let group_id = self.mask_to_id(group_mask)?;
        for &group_idx in group_indices {
            self.grouping_ids[group_idx] = group_id;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.grouping_ids);
        let values = UInt32Array::new(values.into(), None);
        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn size(&self) -> usize {
        self.grouping_ids.capacity() * std::mem::size_of::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use crate::grouping::GroupingAccumulator;

    #[test]
    fn test_group_ids() {
        let grouping = GroupingAccumulator {
            grouping_ids: vec![],
            expr_indices: vec![0, 1, 3, 2],
        };
        let cases = vec![
            (0b0000, vec![false, false, false, false]),
            (0b1000, vec![true, false, false, false]),
            (0b0100, vec![false, true, false, false]),
            (0b1010, vec![true, false, false, true]),
            (0b1001, vec![true, false, true, false]),
        ];
        for (expected, input) in cases {
            assert_eq!(expected, grouping.mask_to_id(&input).unwrap());
        }
    }
    #[test]
    fn test_bad_index() {
        let grouping = GroupingAccumulator {
            grouping_ids: vec![],
            expr_indices: vec![5],
        };
        let res = grouping.mask_to_id(&[false]);
        assert!(res.is_err())
    }
}
