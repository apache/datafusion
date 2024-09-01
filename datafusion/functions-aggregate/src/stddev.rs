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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};

use datafusion_common::{internal_err, not_impl_err, Result};
use datafusion_common::{plan_err, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};
use datafusion_functions_aggregate_common::stats::StatsType;

use crate::variance::{VarianceAccumulator, VarianceGroupsAccumulator};

make_udaf_expr_and_func!(
    Stddev,
    stddev,
    expression,
    "Compute the standard deviation of a set of numbers",
    stddev_udaf
);

/// STDDEV and STDDEV_SAMP (standard deviation) aggregate expression
pub struct Stddev {
    signature: Signature,
    alias: Vec<String>,
}

impl Debug for Stddev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stddev")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Stddev {
    fn default() -> Self {
        Self::new()
    }
}

impl Stddev {
    /// Create a new STDDEV aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![DataType::Float64],
                Volatility::Immutable,
            ),
            alias: vec!["stddev_samp".to_string()],
        }
    }
}

impl AggregateUDFImpl for Stddev {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stddev"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(args.name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("STDDEV_POP(DISTINCT) aggregations are not available");
        }
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Sample)?))
    }

    fn aliases(&self) -> &[String] {
        &self.alias
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(StddevGroupsAccumulator::new(StatsType::Sample)))
    }
}

make_udaf_expr_and_func!(
    StddevPop,
    stddev_pop,
    expression,
    "Compute the population standard deviation of a set of numbers",
    stddev_pop_udaf
);

/// STDDEV_POP population aggregate expression
pub struct StddevPop {
    signature: Signature,
}

impl Debug for StddevPop {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StddevPop")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for StddevPop {
    fn default() -> Self {
        Self::new()
    }
}

impl StddevPop {
    /// Create a new STDDEV_POP aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for StddevPop {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stddev_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(args.name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("STDDEV_POP(DISTINCT) aggregations are not available");
        }
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Population)?))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("StddevPop requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(StddevGroupsAccumulator::new(
            StatsType::Population,
        )))
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    /// Creates a new `StddevAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            variance: VarianceAccumulator::try_new(s_type)?,
        })
    }

    pub fn get_m2(&self) -> f64 {
        self.variance.get_m2()
    }
}

impl Accumulator for StddevAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.variance.get_count()),
            ScalarValue::from(self.variance.get_mean()),
            ScalarValue::from(self.variance.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.update_batch(values)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.retract_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.variance.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let variance = self.variance.evaluate()?;
        match variance {
            ScalarValue::Float64(e) => {
                if e.is_none() {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(e.map(|f| f.sqrt())))
                }
            }
            _ => internal_err!("Variance should be f64"),
        }
    }

    fn size(&self) -> usize {
        std::mem::align_of_val(self) - std::mem::align_of_val(&self.variance)
            + self.variance.size()
    }

    fn supports_retract_batch(&self) -> bool {
        self.variance.supports_retract_batch()
    }
}

#[derive(Debug)]
pub struct StddevGroupsAccumulator {
    variance: VarianceGroupsAccumulator,
}

impl StddevGroupsAccumulator {
    pub fn new(s_type: StatsType) -> Self {
        Self {
            variance: VarianceGroupsAccumulator::new(s_type),
        }
    }
}

impl GroupsAccumulator for StddevGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.variance
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.variance
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let (mut variances, nulls) = self.variance.variance(emit_to);
        variances.iter_mut().for_each(|v| *v = v.sqrt());
        Ok(Arc::new(Float64Array::new(variances.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        self.variance.state(emit_to)
    }

    fn size(&self) -> usize {
        self.variance.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::*, datatypes::*};
    use datafusion_expr::AggregateUDF;
    use datafusion_functions_aggregate_common::utils::get_accum_scalar_values_as_arrays;
    use datafusion_physical_expr::expressions::col;
    use std::sync::Arc;

    #[test]
    fn stddev_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = stddev_pop_udaf();
        let agg2 = stddev_pop_udaf();

        let actual = merge(&batch1, &batch2, agg1, agg2, &schema)?;
        assert_eq!(actual, ScalarValue::from(std::f64::consts::SQRT_2));

        Ok(())
    }

    #[test]
    fn stddev_f64_merge_2() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        let b = Arc::new(Float64Array::from(vec![None]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = stddev_pop_udaf();
        let agg2 = stddev_pop_udaf();

        let actual = merge(&batch1, &batch2, agg1, agg2, &schema)?;
        assert_eq!(actual, ScalarValue::from(std::f64::consts::SQRT_2));

        Ok(())
    }

    fn merge(
        batch1: &RecordBatch,
        batch2: &RecordBatch,
        agg1: Arc<AggregateUDF>,
        agg2: Arc<AggregateUDF>,
        schema: &Schema,
    ) -> Result<ScalarValue> {
        let args1 = AccumulatorArgs {
            return_type: &DataType::Float64,
            schema,
            ignore_nulls: false,
            ordering_req: &[],
            name: "a",
            is_distinct: false,
            is_reversed: false,
            exprs: &[col("a", schema)?],
        };

        let args2 = AccumulatorArgs {
            return_type: &DataType::Float64,
            schema,
            ignore_nulls: false,
            ordering_req: &[],
            name: "a",
            is_distinct: false,
            is_reversed: false,
            exprs: &[col("a", schema)?],
        };

        let mut accum1 = agg1.accumulator(args1)?;
        let mut accum2 = agg2.accumulator(args2)?;

        let value1 = vec![col("a", schema)?
            .evaluate(batch1)
            .and_then(|v| v.into_array(batch1.num_rows()))?];
        let value2 = vec![col("a", schema)?
            .evaluate(batch2)
            .and_then(|v| v.into_array(batch2.num_rows()))?];

        accum1.update_batch(&value1)?;
        accum2.update_batch(&value2)?;
        let state2 = get_accum_scalar_values_as_arrays(accum2.as_mut())?;
        accum1.merge_batch(&state2)?;
        let result = accum1.evaluate()?;
        Ok(result)
    }
}
