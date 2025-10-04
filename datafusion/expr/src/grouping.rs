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

use arrow::{
    array::{Array, Int32Array},
    datatypes::DataType,
};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr_common::{
    accumulator::Accumulator,
    signature::{Signature, Volatility},
};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;

use crate::{
    expr::{AggregateFunction, ScalarFunction},
    utils::grouping_set_to_exprlist,
    Aggregate, AggregateUDF, AggregateUDFImpl, Expr,
};

// To avoid adding datafusion-functions-aggregate dependency, implement a DummyGroupingUDAF here
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DummyGroupingUDAF {
    signature: Signature,
}

impl Default for DummyGroupingUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl DummyGroupingUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    pub fn from_scalar_function(
        func: &ScalarFunction,
        agg: &Aggregate,
    ) -> Result<AggregateFunction> {
        if func.args.len() != 1 && func.args.len() != 2 {
            return internal_err!("Grouping function must have one or two arguments");
        }
        let grouping_expr = grouping_set_to_exprlist(&agg.group_expr)?;
        let args = if func.args.len() == 1 {
            grouping_expr.iter().map(|e| (*e).clone()).collect()
        } else if let Expr::Literal(ScalarValue::List(list), _) = &func.args[1] {
            if list.len() != 1 {
                return internal_err!("The second argument of grouping function must be a list with exactly one element");
            }

            let grouping_expr = grouping_expr.into_iter().rev().collect::<Vec<_>>();
            let values = list
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            values
                .iter()
                .map(|i: &i32| grouping_expr[*i as usize].clone())
                .collect()
        } else {
            return internal_err!(
                "The second argument of grouping function must be a list"
            );
        };
        Ok(AggregateFunction::new_udf(
            Arc::new(AggregateUDF::from(Self::new())),
            args,
            false,
            None,
            vec![],
            None,
        ))
    }
}

impl AggregateUDFImpl for DummyGroupingUDAF {
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
        Ok(DataType::Int32)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        todo!()
    }
}
