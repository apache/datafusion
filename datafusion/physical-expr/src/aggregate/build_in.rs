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

use datafusion_common::{exec_err, not_impl_err, Result};
use datafusion_expr::AggregateFunction;

use crate::aggregate::regr::RegrType;
use crate::expressions::{self, Literal};
use crate::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};

/// Create a physical aggregation expression.
/// This function errors when `input_phy_exprs`' can't be coerced to a valid argument type of the aggregation function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    distinct: bool,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    ordering_req: &[PhysicalSortExpr],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    // get the result data type for this aggregate function
    let input_phy_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let data_type = input_phy_types[0].clone();
    let ordering_types = ordering_req
        .iter()
        .map(|e| e.expr.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let input_phy_exprs = input_phy_exprs.to_vec();
    Ok(match (fun, distinct) {
        (AggregateFunction::Count, false) => Arc::new(
            expressions::Count::new_with_multiple_exprs(input_phy_exprs, name, data_type),
        ),
        (AggregateFunction::Count, true) => Arc::new(expressions::DistinctCount::new(
            data_type,
            input_phy_exprs[0].clone(),
            name,
        )),
        (AggregateFunction::Grouping, _) => Arc::new(expressions::Grouping::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BitAnd, _) => Arc::new(expressions::BitAnd::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BitOr, _) => Arc::new(expressions::BitOr::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BitXor, false) => Arc::new(expressions::BitXor::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BitXor, true) => Arc::new(expressions::DistinctBitXor::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BoolAnd, _) => Arc::new(expressions::BoolAnd::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::BoolOr, _) => Arc::new(expressions::BoolOr::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Sum, false) => Arc::new(expressions::Sum::new(
            input_phy_exprs[0].clone(),
            name,
            input_phy_types[0].clone(),
        )),
        (AggregateFunction::Sum, true) => Arc::new(expressions::DistinctSum::new(
            vec![input_phy_exprs[0].clone()],
            name,
            data_type,
        )),
        (AggregateFunction::ApproxDistinct, _) => Arc::new(
            expressions::ApproxDistinct::new(input_phy_exprs[0].clone(), name, data_type),
        ),
        (AggregateFunction::ArrayAgg, false) => {
            let expr = input_phy_exprs[0].clone();
            let nullable = expr.nullable(input_schema)?;

            if ordering_req.is_empty() {
                Arc::new(expressions::ArrayAgg::new(expr, name, data_type, nullable))
            } else {
                Arc::new(expressions::OrderSensitiveArrayAgg::new(
                    expr,
                    name,
                    data_type,
                    nullable,
                    ordering_types,
                    ordering_req.to_vec(),
                ))
            }
        }
        (AggregateFunction::ArrayAgg, true) => {
            if !ordering_req.is_empty() {
                return not_impl_err!(
                    "ARRAY_AGG(DISTINCT ORDER BY a ASC) order-sensitive aggregations are not available"
                );
            }
            let expr = input_phy_exprs[0].clone();
            let is_expr_nullable = expr.nullable(input_schema)?;
            Arc::new(expressions::DistinctArrayAgg::new(
                expr,
                name,
                data_type,
                is_expr_nullable,
            ))
        }
        (AggregateFunction::Min, _) => Arc::new(expressions::Min::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Max, _) => Arc::new(expressions::Max::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Avg, false) => Arc::new(expressions::Avg::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Avg, true) => {
            return not_impl_err!("AVG(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::Variance, false) => Arc::new(expressions::Variance::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Variance, true) => {
            return not_impl_err!("VAR(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::VariancePop, false) => Arc::new(
            expressions::VariancePop::new(input_phy_exprs[0].clone(), name, data_type),
        ),
        (AggregateFunction::VariancePop, true) => {
            return not_impl_err!("VAR_POP(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::Covariance, false) => Arc::new(expressions::Covariance::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Covariance, true) => {
            return not_impl_err!("COVAR(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::CovariancePop, false) => {
            Arc::new(expressions::CovariancePop::new(
                input_phy_exprs[0].clone(),
                input_phy_exprs[1].clone(),
                name,
                data_type,
            ))
        }
        (AggregateFunction::CovariancePop, true) => {
            return not_impl_err!("COVAR_POP(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::Stddev, false) => Arc::new(expressions::Stddev::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Stddev, true) => {
            return not_impl_err!("STDDEV(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::StddevPop, false) => Arc::new(expressions::StddevPop::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::StddevPop, true) => {
            return not_impl_err!("STDDEV_POP(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::Correlation, false) => {
            Arc::new(expressions::Correlation::new(
                input_phy_exprs[0].clone(),
                input_phy_exprs[1].clone(),
                name,
                data_type,
            ))
        }
        (AggregateFunction::Correlation, true) => {
            return not_impl_err!("CORR(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::RegrSlope, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::Slope,
            data_type,
        )),
        (AggregateFunction::RegrIntercept, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::Intercept,
            data_type,
        )),
        (AggregateFunction::RegrCount, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::Count,
            data_type,
        )),
        (AggregateFunction::RegrR2, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::R2,
            data_type,
        )),
        (AggregateFunction::RegrAvgx, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::AvgX,
            data_type,
        )),
        (AggregateFunction::RegrAvgy, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::AvgY,
            data_type,
        )),
        (AggregateFunction::RegrSXX, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::SXX,
            data_type,
        )),
        (AggregateFunction::RegrSYY, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::SYY,
            data_type,
        )),
        (AggregateFunction::RegrSXY, false) => Arc::new(expressions::Regr::new(
            input_phy_exprs[0].clone(),
            input_phy_exprs[1].clone(),
            name,
            RegrType::SXY,
            data_type,
        )),
        (
            AggregateFunction::RegrSlope
            | AggregateFunction::RegrIntercept
            | AggregateFunction::RegrCount
            | AggregateFunction::RegrR2
            | AggregateFunction::RegrAvgx
            | AggregateFunction::RegrAvgy
            | AggregateFunction::RegrSXX
            | AggregateFunction::RegrSYY
            | AggregateFunction::RegrSXY,
            true,
        ) => {
            return not_impl_err!("{}(DISTINCT) aggregations are not available", fun);
        }
        (AggregateFunction::ApproxPercentileCont, false) => {
            if input_phy_exprs.len() == 2 {
                Arc::new(expressions::ApproxPercentileCont::new(
                    // Pass in the desired percentile expr
                    input_phy_exprs,
                    name,
                    data_type,
                )?)
            } else {
                Arc::new(expressions::ApproxPercentileCont::new_with_max_size(
                    // Pass in the desired percentile expr
                    input_phy_exprs,
                    name,
                    data_type,
                )?)
            }
        }
        (AggregateFunction::ApproxPercentileCont, true) => {
            return not_impl_err!(
                "approx_percentile_cont(DISTINCT) aggregations are not available"
            );
        }
        (AggregateFunction::ApproxPercentileContWithWeight, false) => {
            Arc::new(expressions::ApproxPercentileContWithWeight::new(
                // Pass in the desired percentile expr
                input_phy_exprs,
                name,
                data_type,
            )?)
        }
        (AggregateFunction::ApproxPercentileContWithWeight, true) => {
            return not_impl_err!(
                "approx_percentile_cont_with_weight(DISTINCT) aggregations are not available"
            );
        }
        (AggregateFunction::ApproxMedian, false) => {
            Arc::new(expressions::ApproxMedian::try_new(
                input_phy_exprs[0].clone(),
                name,
                data_type,
            )?)
        }
        (AggregateFunction::ApproxMedian, true) => {
            return not_impl_err!(
                "APPROX_MEDIAN(DISTINCT) aggregations are not available"
            );
        }
        (AggregateFunction::Median, false) => Arc::new(expressions::Median::new(
            input_phy_exprs[0].clone(),
            name,
            data_type,
        )),
        (AggregateFunction::Median, true) => {
            return not_impl_err!("MEDIAN(DISTINCT) aggregations are not available");
        }
        (AggregateFunction::FirstValue, _) => Arc::new(expressions::FirstValue::new(
            input_phy_exprs[0].clone(),
            name,
            input_phy_types[0].clone(),
            ordering_req.to_vec(),
            ordering_types,
        )),
        (AggregateFunction::LastValue, _) => Arc::new(expressions::LastValue::new(
            input_phy_exprs[0].clone(),
            name,
            input_phy_types[0].clone(),
            ordering_req.to_vec(),
            ordering_types,
        )),
        (AggregateFunction::NthValue, _) => {
            let expr = &input_phy_exprs[0];
            let Some(n) = input_phy_exprs[1]
                .as_any()
                .downcast_ref::<Literal>()
                .map(|literal| literal.value())
            else {
                return exec_err!("Second argument of NTH_VALUE needs to be a literal");
            };
            let nullable = expr.nullable(input_schema)?;
            Arc::new(expressions::NthValueAgg::new(
                expr.clone(),
                n.clone().try_into()?,
                name,
                input_phy_types[0].clone(),
                nullable,
                ordering_types,
                ordering_req.to_vec(),
            ))
        }
        (AggregateFunction::StringAgg, false) => {
            if !ordering_req.is_empty() {
                return not_impl_err!(
                    "STRING_AGG(ORDER BY a ASC) order-sensitive aggregations are not available"
                );
            }
            Arc::new(expressions::StringAgg::new(
                input_phy_exprs[0].clone(),
                input_phy_exprs[1].clone(),
                name,
                data_type,
            ))
        }
        (AggregateFunction::StringAgg, true) => {
            return not_impl_err!("STRING_AGG(DISTINCT) aggregations are not available");
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use datafusion_common::{plan_err, DataFusionError, ScalarValue};
    use datafusion_expr::type_coercion::aggregates::NUMERICS;
    use datafusion_expr::{type_coercion, Signature};

    use crate::expressions::{
        try_cast, ApproxDistinct, ApproxMedian, ApproxPercentileCont, ArrayAgg, Avg,
        BitAnd, BitOr, BitXor, BoolAnd, BoolOr, Correlation, Count, Covariance,
        DistinctArrayAgg, DistinctCount, Max, Min, Stddev, Sum, Variance,
    };

    use super::*;

    #[test]
    fn test_count_arragg_approx_expr() -> Result<()> {
        let funcs = vec![
            AggregateFunction::Count,
            AggregateFunction::ArrayAgg,
            AggregateFunction::ApproxDistinct,
        ];
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
                    AggregateFunction::Count => {
                        assert!(result_agg_phy_exprs.as_any().is::<Count>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::Int64, true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ApproxDistinct => {
                        assert!(result_agg_phy_exprs.as_any().is::<ApproxDistinct>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, false),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ArrayAgg => {
                        assert!(result_agg_phy_exprs.as_any().is::<ArrayAgg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new_list(
                                "c1",
                                Field::new("item", data_type.clone(), true),
                                true,
                            ),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };

                let result_distinct = create_physical_agg_expr_for_test(
                    &fun,
                    true,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Count => {
                        assert!(result_distinct.as_any().is::<DistinctCount>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new("c1", DataType::Int64, true),
                            result_distinct.field().unwrap()
                        );
                    }
                    AggregateFunction::ApproxDistinct => {
                        assert!(result_distinct.as_any().is::<ApproxDistinct>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, false),
                            result_distinct.field().unwrap()
                        );
                    }
                    AggregateFunction::ArrayAgg => {
                        assert!(result_distinct.as_any().is::<DistinctArrayAgg>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new_list(
                                "c1",
                                Field::new("item", data_type.clone(), true),
                                true,
                            ),
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
    fn test_agg_approx_percentile_phy_expr() {
        for data_type in NUMERICS {
            let input_schema =
                Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
            let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                ),
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(0.2)))),
            ];
            let result_agg_phy_exprs = create_physical_agg_expr_for_test(
                &AggregateFunction::ApproxPercentileCont,
                false,
                &input_phy_exprs[..],
                &input_schema,
                "c1",
            )
            .expect("failed to create aggregate expr");

            assert!(result_agg_phy_exprs.as_any().is::<ApproxPercentileCont>());
            assert_eq!("c1", result_agg_phy_exprs.name());
            assert_eq!(
                Field::new("c1", data_type.clone(), false),
                result_agg_phy_exprs.field().unwrap()
            );
        }
    }

    #[test]
    fn test_agg_approx_percentile_invalid_phy_expr() {
        for data_type in NUMERICS {
            let input_schema =
                Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
            let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                ),
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(4.2)))),
            ];
            let err = create_physical_agg_expr_for_test(
                &AggregateFunction::ApproxPercentileCont,
                false,
                &input_phy_exprs[..],
                &input_schema,
                "c1",
            )
            .expect_err("should fail due to invalid percentile");

            assert!(matches!(err, DataFusionError::Plan(_)));
        }
    }

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
    fn test_bit_and_or_xor_expr() -> Result<()> {
        let funcs = vec![
            AggregateFunction::BitAnd,
            AggregateFunction::BitOr,
            AggregateFunction::BitXor,
        ];
        let data_types = vec![DataType::UInt64, DataType::Int64];
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
                    AggregateFunction::BitAnd => {
                        assert!(result_agg_phy_exprs.as_any().is::<BitAnd>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::BitOr => {
                        assert!(result_agg_phy_exprs.as_any().is::<BitOr>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::BitXor => {
                        assert!(result_agg_phy_exprs.as_any().is::<BitXor>());
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
    fn test_bool_and_or_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::BoolAnd, AggregateFunction::BoolOr];
        let data_types = vec![DataType::Boolean];
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
                    AggregateFunction::BoolAnd => {
                        assert!(result_agg_phy_exprs.as_any().is::<BoolAnd>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::BoolOr => {
                        assert!(result_agg_phy_exprs.as_any().is::<BoolOr>());
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
    fn test_sum_avg_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Sum, AggregateFunction::Avg];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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
                    AggregateFunction::Sum => {
                        assert!(result_agg_phy_exprs.as_any().is::<Sum>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        let expect_type = match data_type {
                            DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64 => DataType::UInt64,
                            DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64 => DataType::Int64,
                            DataType::Float32 | DataType::Float64 => DataType::Float64,
                            _ => data_type.clone(),
                        };

                        assert_eq!(
                            Field::new("c1", expect_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Avg => {
                        assert!(result_agg_phy_exprs.as_any().is::<Avg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::Float64, true),
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
    fn test_variance_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Variance];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Variance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_var_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::VariancePop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Variance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_stddev_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Stddev];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Stddev>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_stddev_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::StddevPop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Stddev>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_covar_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Covariance];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_physical_agg_expr_for_test(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Covariance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_covar_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::CovariancePop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_physical_agg_expr_for_test(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Covariance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_corr_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Correlation];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_physical_agg_expr_for_test(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Correlation>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_median_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::ApproxMedian];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
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

                if fun == AggregateFunction::ApproxMedian {
                    assert!(result_agg_phy_exprs.as_any().is::<ApproxMedian>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", data_type.clone(), true),
                        result_agg_phy_exprs.field().unwrap()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_median() -> Result<()> {
        let observed = AggregateFunction::ApproxMedian.return_type(&[DataType::Utf8]);
        assert!(observed.is_err());

        let observed = AggregateFunction::ApproxMedian.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);

        let observed =
            AggregateFunction::ApproxMedian.return_type(&[DataType::Decimal128(10, 6)]);
        assert!(observed.is_err());

        Ok(())
    }

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = AggregateFunction::Min.return_type(&[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = AggregateFunction::Max.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);

        // test decimal for min
        let observed =
            AggregateFunction::Min.return_type(&[DataType::Decimal128(10, 6)])?;
        assert_eq!(DataType::Decimal128(10, 6), observed);

        // test decimal for max
        let observed =
            AggregateFunction::Max.return_type(&[DataType::Decimal128(28, 13)])?;
        assert_eq!(DataType::Decimal128(28, 13), observed);

        Ok(())
    }

    #[test]
    fn test_sum_return_type() -> Result<()> {
        let observed = AggregateFunction::Sum.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Int64, observed);

        let observed = AggregateFunction::Sum.return_type(&[DataType::UInt8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = AggregateFunction::Sum.return_type(&[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Sum.return_type(&[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed =
            AggregateFunction::Sum.return_type(&[DataType::Decimal128(10, 5)])?;
        assert_eq!(DataType::Decimal128(20, 5), observed);

        let observed =
            AggregateFunction::Sum.return_type(&[DataType::Decimal128(35, 5)])?;
        assert_eq!(DataType::Decimal128(38, 5), observed);

        Ok(())
    }

    #[test]
    fn test_sum_no_utf8() {
        let observed = AggregateFunction::Sum.return_type(&[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_sum_upcasts() -> Result<()> {
        let observed = AggregateFunction::Sum.return_type(&[DataType::UInt32])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_count_return_type() -> Result<()> {
        let observed = AggregateFunction::Count.return_type(&[DataType::Utf8])?;
        assert_eq!(DataType::Int64, observed);

        let observed = AggregateFunction::Count.return_type(&[DataType::Int8])?;
        assert_eq!(DataType::Int64, observed);

        let observed =
            AggregateFunction::Count.return_type(&[DataType::Decimal128(28, 13)])?;
        assert_eq!(DataType::Int64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_return_type() -> Result<()> {
        let observed = AggregateFunction::Avg.return_type(&[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Avg.return_type(&[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Avg.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed =
            AggregateFunction::Avg.return_type(&[DataType::Decimal128(10, 6)])?;
        assert_eq!(DataType::Decimal128(14, 10), observed);

        let observed =
            AggregateFunction::Avg.return_type(&[DataType::Decimal128(36, 6)])?;
        assert_eq!(DataType::Decimal128(38, 10), observed);
        Ok(())
    }

    #[test]
    fn test_avg_no_utf8() {
        let observed = AggregateFunction::Avg.return_type(&[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_variance_return_type() -> Result<()> {
        let observed = AggregateFunction::Variance.return_type(&[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Variance.return_type(&[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Variance.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Variance.return_type(&[DataType::UInt32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Variance.return_type(&[DataType::Int64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_variance_no_utf8() {
        let observed = AggregateFunction::Variance.return_type(&[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_stddev_return_type() -> Result<()> {
        let observed = AggregateFunction::Stddev.return_type(&[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Stddev.return_type(&[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Stddev.return_type(&[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Stddev.return_type(&[DataType::UInt32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = AggregateFunction::Stddev.return_type(&[DataType::Int64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_stddev_no_utf8() {
        let observed = AggregateFunction::Stddev.return_type(&[DataType::Utf8]);
        assert!(observed.is_err());
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
        create_aggregate_expr(fun, distinct, &coerced_phy_exprs, &[], input_schema, name)
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
            .map(|(expr, coerced_type)| try_cast(expr.clone(), schema, coerced_type))
            .collect::<Result<Vec<_>>>()
    }
}
