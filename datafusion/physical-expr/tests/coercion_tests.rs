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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDF, Documentation, Signature, Volatility};
use datafusion_expr_common::type_coercion::aggregates::NUMERICS;
use datafusion_physical_expr::{
    apply_aggregate_coercion,
    expressions::{Column, Literal},
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::f32;
use std::f64;

/// A mock aggregate UDF implementation that will coerce numeric inputs to Float64
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct MockCorrelationUDF {
    signature: Signature,
}

impl MockCorrelationUDF {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl datafusion_expr::AggregateUDFImpl for MockCorrelationUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "mock_corr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, input_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![DataType::Float64; input_types.len()])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Err(datafusion_common::DataFusionError::NotImplemented(
            "Mock accumulator is not implemented".to_string(),
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

#[test]
fn test_apply_aggregate_coercion() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int32_col", DataType::Int32, false),
        Field::new("float32_col", DataType::Float32, false),
    ]));

    let int_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("int32_col", 0));
    let float_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("float32_col", 1));

    let mock_corr = Arc::new(AggregateUDF::from(MockCorrelationUDF::new()));

    // Test with int32 and float32 columns
    let physical_args: Vec<Arc<dyn PhysicalExpr>> =
        vec![int_expr.clone(), float_expr.clone()];

    let coerced_args = apply_aggregate_coercion(&mock_corr, &physical_args, &schema)?;

    let coerced_type1 = coerced_args[0].data_type(&schema)?;
    let coerced_type2 = coerced_args[1].data_type(&schema)?;

    assert_eq!(coerced_type1, DataType::Float64);
    assert_eq!(coerced_type2, DataType::Float64);

    // Test with literal values
    let lit_int: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Int64(Some(42))));
    let lit_float: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Float32(Some(f32::consts::PI))));

    let physical_args2: Vec<Arc<dyn PhysicalExpr>> = vec![lit_int, lit_float];

    let coerced_args2 = apply_aggregate_coercion(&mock_corr, &physical_args2, &schema)?;

    let coerced_type1 = coerced_args2[0].data_type(&schema)?;
    let coerced_type2 = coerced_args2[1].data_type(&schema)?;

    assert_eq!(coerced_type1, DataType::Float64);
    assert_eq!(coerced_type2, DataType::Float64);

    // Test with float64 literals
    let float64_expr: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Float64(Some(f64::consts::PI))));

    let physical_args3: Vec<Arc<dyn PhysicalExpr>> =
        vec![float64_expr.clone(), float64_expr.clone()];

    let coerced_args3 = apply_aggregate_coercion(&mock_corr, &physical_args3, &schema)?;

    // Should still be Float64, but should not create new expressions
    assert_eq!(coerced_args3[0].data_type(&schema)?, DataType::Float64);
    assert_eq!(coerced_args3[1].data_type(&schema)?, DataType::Float64);

    // The expressions should be the same references (not new cast expressions)
    assert!(
        std::ptr::eq(
            Arc::as_ptr(&physical_args3[0]),
            Arc::as_ptr(&coerced_args3[0])
        ),
        "No new expression should be created when type already matches"
    );

    Ok(())
}
