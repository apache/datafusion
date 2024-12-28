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

use arrow::compute::{and, filter, is_not_null};

use std::{any::Any, sync::Arc};

use crate::covariance::CovarianceAccumulator;
use crate::stddev::StddevAccumulator;
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion_physical_expr::expressions::format_state_name;
use datafusion_physical_expr::expressions::StatsType;

/// CORR aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug)]
pub struct Correlation {
    name: String,
    signature: Signature,
    null_on_divide_by_zero: bool,
}

impl Correlation {
    pub fn new(name: impl Into<String>, data_type: DataType, null_on_divide_by_zero: bool) -> Self {
        // the result of correlation just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable),
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Correlation {
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
    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelationAccumulator::try_new(
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
                format_state_name(&self.name, "mean1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean2"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "algo_const"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "m2_1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "m2_2"),
                DataType::Float64,
                true,
            ),
        ])
    }
}

/// An accumulator to compute correlation
#[derive(Debug)]
pub struct CorrelationAccumulator {
    covar: CovarianceAccumulator,
    stddev1: StddevAccumulator,
    stddev2: StddevAccumulator,
    null_on_divide_by_zero: bool,
}

impl CorrelationAccumulator {
    /// Creates a new `CorrelationAccumulator`
    pub fn try_new(null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            stddev1: StddevAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            stddev2: StddevAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            null_on_divide_by_zero,
        })
    }
}

impl Accumulator for CorrelationAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.covar.get_count()),
            ScalarValue::from(self.covar.get_mean1()),
            ScalarValue::from(self.covar.get_mean2()),
            ScalarValue::from(self.covar.get_algo_const()),
            ScalarValue::from(self.stddev1.get_m2()),
            ScalarValue::from(self.stddev2.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        if !values[0].is_empty() && !values[1].is_empty() {
            self.covar.update_batch(&values)?;
            self.stddev1.update_batch(&values[0..1])?;
            self.stddev2.update_batch(&values[1..2])?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.retract_batch(&values)?;
        self.stddev1.retract_batch(&values[0..1])?;
        self.stddev2.retract_batch(&values[1..2])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_c = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
            Arc::clone(&states[3]),
        ];
        let states_s1 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[4]),
        ];
        let states_s2 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[2]),
            Arc::clone(&states[5]),
        ];

        if states[0].len() > 0 && states[1].len() > 0 && states[2].len() > 0 {
            self.covar.merge_batch(&states_c)?;
            self.stddev1.merge_batch(&states_s1)?;
            self.stddev2.merge_batch(&states_s2)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let covar = self.covar.evaluate()?;
        let stddev1 = self.stddev1.evaluate()?;
        let stddev2 = self.stddev2.evaluate()?;

        match (covar, stddev1, stddev2) {
            (
                ScalarValue::Float64(Some(c)),
                ScalarValue::Float64(Some(s1)),
                ScalarValue::Float64(Some(s2)),
            ) if s1 != 0.0 && s2 != 0.0 => Ok(ScalarValue::Float64(Some(c / (s1 * s2)))),
            _ if self.null_on_divide_by_zero => Ok(ScalarValue::Float64(None)),
            _ => {
                if self.covar.get_count() == 1.0 {
                    return Ok(ScalarValue::Float64(Some(f64::NAN)));
                }
                Ok(ScalarValue::Float64(None))
            }
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
            - std::mem::size_of_val(&self.stddev1)
            + self.stddev1.size()
            - std::mem::size_of_val(&self.stddev2)
            + self.stddev2.size()
    }
}
