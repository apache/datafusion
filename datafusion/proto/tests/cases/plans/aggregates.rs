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

//! `AggregateExec`.

use super::{roundtrip_test, roundtrip_test_with_context};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::Volatility;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, col, lit};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::Result;
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, Signature, SimpleAggregateUDF,
};
use datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion_functions_aggregate::array_agg::array_agg_udaf;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::nth_value::nth_value_udaf;
use datafusion_functions_aggregate::string_agg::string_agg_udaf;
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_aggregate() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let avg_expr = AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
        .schema(Arc::clone(&schema))
        .alias("AVG(b)")
        .build()?;
    let nth_expr =
        AggregateExprBuilder::new(nth_value_udaf(), vec![col("b", &schema)?, lit(1u64)])
            .schema(Arc::clone(&schema))
            .alias("NTH_VALUE(b, 1)")
            .build()?;
    let str_agg_expr =
        AggregateExprBuilder::new(string_agg_udaf(), vec![col("b", &schema)?, lit(1u64)])
            .schema(Arc::clone(&schema))
            .alias("NTH_VALUE(b, 1)")
            .build()?;

    let test_cases = vec![
        // AVG
        vec![Arc::new(avg_expr)],
        // NTH_VALUE
        vec![Arc::new(nth_expr)],
        // STRING_AGG
        vec![Arc::new(str_agg_expr)],
    ];

    for aggregates in test_cases {
        let schema = schema.clone();
        roundtrip_test(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
            aggregates,
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?))?;
    }

    Ok(())
}

#[test]
fn roundtrip_aggregate_with_limit() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates = vec![
        AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("AVG(b)")
            .build()
            .map(Arc::new)?,
    ];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    let agg = agg.with_limit_options(Some(LimitOptions::new_with_order(12, false)));
    roundtrip_test(Arc::new(agg))
}

#[test]
fn roundtrip_aggregate_with_approx_pencentile_cont() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates = vec![
        AggregateExprBuilder::new(
            approx_percentile_cont_udaf(),
            vec![col("b", &schema)?, lit(0.5)],
        )
        .schema(Arc::clone(&schema))
        .alias("APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b)")
        .build()
        .map(Arc::new)?,
    ];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    roundtrip_test(Arc::new(agg))
}

#[test]
fn roundtrip_aggregate_with_sort() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];
    let sort_exprs = vec![PhysicalSortExpr {
        expr: col("b", &schema)?,
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    }];

    let aggregates = vec![
        AggregateExprBuilder::new(array_agg_udaf(), vec![col("b", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("ARRAY_AGG(b)")
            .order_by(sort_exprs)
            .build()
            .map(Arc::new)?,
    ];

    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(groups.clone()),
        aggregates,
        vec![None],
        Arc::new(EmptyExec::new(schema.clone())),
        schema,
    )?;
    roundtrip_test(Arc::new(agg))
}

#[test]
fn roundtrip_aggregate_udaf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    #[derive(Debug)]
    struct Example;
    impl Accumulator for Example {
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Int64(Some(0))])
        }

        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int64(Some(0)))
        }

        fn size(&self) -> usize {
            0
        }
    }

    let return_type = DataType::Int64;
    let accumulator: AccumulatorFactoryFunction = Arc::new(|_| Ok(Box::new(Example)));

    let udaf = AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "example",
        Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        return_type,
        accumulator,
        vec![Field::new("value", DataType::Int64, true).into()],
    ));

    let ctx = SessionContext::new();
    ctx.register_udaf(udaf.clone());

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("a", &schema)?, "unused".to_string())];

    let aggregates = vec![
        AggregateExprBuilder::new(Arc::new(udaf), vec![col("b", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("example_agg")
            .build()
            .map(Arc::new)?,
    ];

    roundtrip_test_with_context(
        Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
            aggregates,
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?),
        &ctx,
    )
}
