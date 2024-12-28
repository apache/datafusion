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

use std::{any::Any, sync::Arc};

use crate::variance::VarianceAccumulator;
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion::logical_expr::Accumulator;
use datafusion_common::types::NativeType;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion_physical_expr::expressions::format_state_name;
use datafusion_physical_expr::expressions::StatsType;

/// STDDEV and STDDEV_SAMP (standard deviation) aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug)]
pub struct Stddev {
    name: String,
    signature: Signature,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl Stddev {
    /// Create a new STDDEV aggregate function
    pub fn new(
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of stddev just support FLOAT64.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::coercible(
                vec![
                    datafusion_expr_common::signature::TypeSignatureClass::Native(Arc::new(
                        NativeType::Float64,
                    )),
                ],
                Volatility::Immutable,
            ),
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Stddev {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(&self.name, "m2"), DataType::Float64, true),
        ])
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }
}

/// An accumulator to compute the standard deviation
#[derive(Debug)]
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    /// Creates a new `StddevAccumulator`
    pub fn try_new(s_type: StatsType, null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            variance: VarianceAccumulator::try_new(s_type, null_on_divide_by_zero)?,
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
            ScalarValue::Float64(Some(e)) => Ok(ScalarValue::Float64(Some(e.sqrt()))),
            ScalarValue::Float64(None) => Ok(ScalarValue::Float64(None)),
            _ => internal_err!("Variance should be f64"),
        }
    }

    fn size(&self) -> usize {
        std::mem::align_of_val(self) - std::mem::align_of_val(&self.variance) + self.variance.size()
    }
}
