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

use crate::first_last::last_value_udaf;
use arrow_schema::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::{AggregateFunction, Sort};
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Expr, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;

make_udaf_expr_and_func!(
    MaxByFunction,
    max_by,
    x y,
    "Returns the value of the first column corresponding to the maximum value in the second column.",
    max_by_udaf
);

pub struct MaxByFunction {
    signature: Signature,
}

impl Debug for MaxByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(input_types: &[DataType]) -> Result<Vec<DataType>> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl AggregateUDFImpl for MaxByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        exec_err!("should not reach here")
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(
        &self,
    ) -> Option<datafusion_expr::function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: datafusion_expr::expr::AggregateFunction,
                        _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.order_by.unwrap_or_else(Vec::new);
            let (second_arg, first_arg) =
                (aggr_func.args.remove(1), aggr_func.args.remove(0));

            order_by.push(Sort::new(second_arg, true, false));

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.distinct,
                aggr_func.filter,
                Some(order_by),
                aggr_func.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }
}

make_udaf_expr_and_func!(
    MinByFunction,
    min_by,
    x y,
    "Returns the value of the first column corresponding to the minimum value in the second column.",
    min_by_udaf
);

pub struct MinByFunction {
    signature: Signature,
}

impl Debug for MinByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MinByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for MinByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        exec_err!("should not reach here")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(
        &self,
    ) -> Option<datafusion_expr::function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: datafusion_expr::expr::AggregateFunction,
                        _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.order_by.unwrap_or_else(Vec::new);
            let (second_arg, first_arg) =
                (aggr_func.args.remove(1), aggr_func.args.remove(0));

            order_by.push(Sort::new(second_arg, false, false)); // false for ascending sort

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.distinct,
                aggr_func.filter,
                Some(order_by),
                aggr_func.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }
}
