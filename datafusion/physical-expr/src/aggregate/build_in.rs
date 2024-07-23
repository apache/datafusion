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

//! Declaration of built-in (aggregate) functions.
//! This module contains built-in aggregates' enumeration and metadata.
//!
//! Generally, an aggregate has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for min, ([f32]) -> f32, ([f64]) -> f64.

use std::sync::Arc;

use arrow::datatypes::Schema;

use datafusion_common::Result;
use datafusion_expr::AggregateFunction;

use crate::expressions::{self};
use crate::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};

/// Create a physical aggregation expression.
/// This function errors when `input_phy_exprs`' can't be coerced to a valid argument type of the aggregation function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    distinct: bool,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    _ordering_req: &[PhysicalSortExpr],
    input_schema: &Schema,
    name: impl Into<String>,
    _ignore_nulls: bool,
) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    // get the result data type for this aggregate function
    let input_phy_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let data_type = input_phy_types[0].clone();
    let input_phy_exprs = input_phy_exprs.to_vec();
    Ok(match (fun, distinct) {
        (AggregateFunction::Min, _) => Arc::new(expressions::Min::new(
            Arc::clone(&input_phy_exprs[0]),
            name,
            data_type,
        )),
        (AggregateFunction::Max, _) => Arc::new(expressions::Max::new(
            Arc::clone(&input_phy_exprs[0]),
            name,
            data_type,
        )),
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use datafusion_common::plan_err;
    use datafusion_expr::{type_coercion, Signature};

    use crate::expressions::{try_cast, Max, Min};

    use super::*;

    #[test]
    fn test_min_max_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Min, AggregateFunction::Max];
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Utf8,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_physical_agg_expr_for_test(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Min => {
                        assert!(result_agg_phy_exprs.as_any().is::<Min>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Max => {
                        assert!(result_agg_phy_exprs.as_any().is::<Max>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = AggregateFunction::Min.return_type(&[DataType::Utf8], &[true])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = AggregateFunction::Max.return_type(&[DataType::Int32], &[true])?;
        assert_eq!(DataType::Int32, observed);

        // test decimal for min
        let observed = AggregateFunction::Min
            .return_type(&[DataType::Decimal128(10, 6)], &[true])?;
        assert_eq!(DataType::Decimal128(10, 6), observed);

        // test decimal for max
        let observed = AggregateFunction::Max
            .return_type(&[DataType::Decimal128(28, 13)], &[true])?;
        assert_eq!(DataType::Decimal128(28, 13), observed);

        Ok(())
    }

    // Helper function
    // Create aggregate expr with type coercion
    fn create_physical_agg_expr_for_test(
        fun: &AggregateFunction,
        distinct: bool,
        input_phy_exprs: &[Arc<dyn PhysicalExpr>],
        input_schema: &Schema,
        name: impl Into<String>,
    ) -> Result<Arc<dyn AggregateExpr>> {
        let name = name.into();
        let coerced_phy_exprs =
            coerce_exprs_for_test(fun, input_phy_exprs, input_schema, &fun.signature())?;
        if coerced_phy_exprs.is_empty() {
            return plan_err!(
                "Invalid or wrong number of arguments passed to aggregate: '{name}'"
            );
        }
        create_aggregate_expr(
            fun,
            distinct,
            &coerced_phy_exprs,
            &[],
            input_schema,
            name,
            false,
        )
    }

    // Returns the coerced exprs for each `input_exprs`.
    // Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
    // data type of `input_exprs` need to be coerced.
    fn coerce_exprs_for_test(
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
        let coerced_types =
            type_coercion::aggregates::coerce_types(agg_fun, &input_types, signature)?;

        // try cast if need
        input_exprs
            .iter()
            .zip(coerced_types)
            .map(|(expr, coerced_type)| try_cast(Arc::clone(expr), schema, coerced_type))
            .collect::<Result<Vec<_>>>()
    }
}
