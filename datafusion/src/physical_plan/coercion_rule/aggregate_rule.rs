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

//! Support the coercion rule for aggregate function.

use crate::arrow::datatypes::Schema;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::aggregates::AggregateFunction;
use crate::physical_plan::expressions::{
    is_avg_support_arg_type, is_sum_support_arg_type, try_cast,
};
use crate::physical_plan::functions::{Signature, TypeSignature};
use crate::physical_plan::PhysicalExpr;
use arrow::datatypes::DataType;
use std::ops::Deref;
use std::sync::Arc;

pub fn coerce_types(
    agg_fun: &AggregateFunction,
    input_types: &[DataType],
    signature: &Signature,
) -> Result<Vec<DataType>> {
    match signature.type_signature {
        TypeSignature::Uniform(agg_count, _) | TypeSignature::Any(agg_count) => {
            if input_types.len() != agg_count {
                return Err(DataFusionError::Plan(format!("The function {:?} expect argument number is {:?}, but the input argument number is {:?}",
                                                         agg_fun, agg_count, input_types.len())));
            }
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "The aggregate coercion rule don't support this {:?}",
                signature
            )));
        }
    };
    match agg_fun {
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Ok(input_types.to_vec())
        }
        AggregateFunction::ArrayAgg => Ok(input_types.to_vec()),
        AggregateFunction::Min | AggregateFunction::Max => {
            // min and max support the dictionary data type
            // unpack the dictionary to get the value
            get_min_max_result_type(input_types)
        }
        AggregateFunction::Sum => {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval.
            if !is_sum_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} do not support the {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Avg => {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval
            if !is_avg_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} do not support the {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
    }
}

fn get_min_max_result_type(input_types: &[DataType]) -> Result<Vec<DataType>> {
    // min and max support the dictionary data type
    // unpack the dictionary to get the value
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        // TODO add checker for datatype which min and max supported
        // For example, the `Struct` and `Map` type are not supported in the MIN and MAX function
        _ => Ok(input_types.to_vec()),
    }
}

pub fn coerce_exprs(
    agg_fun: &AggregateFunction,
    input_exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    signature: &Signature,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let input_types = input_exprs
        .iter()
        .map(|e| e.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    // get the coerced data types
    let coerced_types = coerce_types(agg_fun, &input_types, signature)?;

    // try cast if need
    input_exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| try_cast(expr.clone(), schema, coerced_types[i].clone()))
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::aggregates;
    use crate::physical_plan::aggregates::{signature, AggregateFunction};
    use crate::physical_plan::coercion_rule::aggregate_rule::coerce_types;
    use arrow::datatypes::DataType;

    #[test]
    fn test_aggregate_coerce_types() {
        // test input args with error number input types
        let fun = AggregateFunction::Min;
        let input_types = vec![DataType::Int64, DataType::Int32];
        let signature = signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!("Error during planning: The function Min expect argument number is 1, but the input argument number is 2", result.unwrap_err().to_string());

        // test input args is invalid data type for sum or avg
        let fun = AggregateFunction::Sum;
        let input_types = vec![DataType::Utf8];
        let signature = aggregates::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Sum do not support the Utf8.",
            result.unwrap_err().to_string()
        );
        let fun = AggregateFunction::Avg;
        let signature = aggregates::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Avg do not support the Utf8.",
            result.unwrap_err().to_string()
        );

        // test count, array_agg, approx_distinct, min, max.
        // the coerced types is same with input types
        let funs = vec![
            AggregateFunction::Count,
            AggregateFunction::ArrayAgg,
            AggregateFunction::ApproxDistinct,
            AggregateFunction::Min,
            AggregateFunction::Max,
        ];
        let input_types = vec![
            vec![DataType::Int32],
            // vec![DataType::Decimal(10, 2)],
            vec![DataType::Utf8],
        ];
        for fun in funs {
            for input_type in &input_types {
                let signature = aggregates::signature(&fun);
                let result = coerce_types(&fun, input_type, &signature);
                assert_eq!(*input_type, result.unwrap());
            }
        }
        // test sum, avg
        let funs = vec![AggregateFunction::Sum, AggregateFunction::Avg];
        let input_types = vec![
            vec![DataType::Int32],
            vec![DataType::Float32],
            // vec![DataType::Decimal(20, 3)],
        ];
        for fun in funs {
            for input_type in &input_types {
                let signature = aggregates::signature(&fun);
                let result = coerce_types(&fun, input_type, &signature);
                assert_eq!(*input_type, result.unwrap());
            }
        }
    }
}
