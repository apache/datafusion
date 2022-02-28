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

pub use datafusion_physical_expr::coercion_rule::aggregate_rule::{
    coerce_exprs, coerce_types,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::aggregates;
    use arrow::datatypes::DataType;
    use datafusion_expr::AggregateFunction;

    #[test]
    fn test_aggregate_coerce_types() {
        // test input args with error number input types
        let fun = AggregateFunction::Min;
        let input_types = vec![DataType::Int64, DataType::Int32];
        let signature = aggregates::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!("Error during planning: The function Min expects 1 arguments, but 2 were provided", result.unwrap_err().to_string());

        // test input args is invalid data type for sum or avg
        let fun = AggregateFunction::Sum;
        let input_types = vec![DataType::Utf8];
        let signature = aggregates::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Sum does not support inputs of type Utf8.",
            result.unwrap_err().to_string()
        );
        let fun = AggregateFunction::Avg;
        let signature = aggregates::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Avg does not support inputs of type Utf8.",
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
            // support the decimal data type for min/max agg
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
            vec![DataType::Decimal(20, 3)],
        ];
        for fun in funs {
            for input_type in &input_types {
                let signature = aggregates::signature(&fun);
                let result = coerce_types(&fun, input_type, &signature);
                assert_eq!(*input_type, result.unwrap());
            }
        }

        // ApproxPercentileCont input types
        let input_types = vec![
            vec![DataType::Int8, DataType::Float64],
            vec![DataType::Int16, DataType::Float64],
            vec![DataType::Int32, DataType::Float64],
            vec![DataType::Int64, DataType::Float64],
            vec![DataType::UInt8, DataType::Float64],
            vec![DataType::UInt16, DataType::Float64],
            vec![DataType::UInt32, DataType::Float64],
            vec![DataType::UInt64, DataType::Float64],
            vec![DataType::Float32, DataType::Float64],
            vec![DataType::Float64, DataType::Float64],
        ];
        for input_type in &input_types {
            let signature =
                aggregates::signature(&AggregateFunction::ApproxPercentileCont);
            let result = coerce_types(
                &AggregateFunction::ApproxPercentileCont,
                input_type,
                &signature,
            );
            assert_eq!(*input_type, result.unwrap());
        }
    }
}
