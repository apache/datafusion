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

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::test_util::batches_to_string;
use datafusion::common::{Result, ScalarValue, assert_batches_eq};
use datafusion::logical_expr::expr::{HigherOrderFunction, WindowFunction};
use datafusion::logical_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress,
    PartitionEvaluator, ValueOrLambda, Volatility, col, create_udaf, create_udf,
    create_udwf,
};
use datafusion::prelude::SessionContext;
use datafusion_functions_aggregate::average::AvgAccumulator;

static NEXT_VALUE: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, PartialEq, Eq, Hash)]
struct NextValue {
    signature: HigherOrderSignature,
}

impl HigherOrderUDFImpl for NextValue {
    fn name(&self) -> &str {
        "next_value"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        Ok(LambdaParametersProgress::Complete(vec![]))
    }

    fn return_field_from_args(
        &self,
        _args: HigherOrderReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("value", DataType::Int64, false)))
    }

    fn invoke_with_args(&self, _args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let value = if self.signature.volatility == Volatility::Volatile {
            NEXT_VALUE.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value))))
    }
}

#[test]
fn function_volatility() {
    #[derive(Debug)]
    struct IdentityEvaluator;

    impl PartitionEvaluator for IdentityEvaluator {
        fn evaluate_all(
            &mut self,
            values: &[ArrayRef],
            _num_rows: usize,
        ) -> Result<ArrayRef> {
            Ok(Arc::clone(&values[0]))
        }
    }

    for volatility in [
        Volatility::Immutable,
        Volatility::Stable,
        Volatility::Volatile,
    ] {
        let scalar = create_udf(
            "identity",
            vec![DataType::Float64],
            DataType::Float64,
            volatility,
            Arc::new(|args| Ok(args[0].clone())),
        );
        let aggregate = Arc::new(create_udaf(
            "average",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            volatility,
            Arc::new(|_| Ok(Box::<AvgAccumulator>::default())),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        ));
        let window = create_udwf(
            "identity_window",
            DataType::Float64,
            Arc::new(DataType::Float64),
            volatility,
            Arc::new(|| Ok(Box::new(IdentityEvaluator))),
        );
        let higher_order = Arc::new(HigherOrderUDF::new_from_impl(NextValue {
            signature: HigherOrderSignature::any(0, volatility),
        }));

        for expr in [
            scalar.call(vec![col("value")]),
            aggregate.call(vec![col("value")]),
            WindowFunction::new(Arc::clone(&aggregate), vec![col("value")]).into(),
            window.call(vec![col("value")]),
            Expr::HigherOrderFunction(HigherOrderFunction::new(higher_order, vec![])),
        ] {
            let expected = volatility == Volatility::Volatile;
            assert_eq!(expr.is_volatile_node(), expected, "{expr}");
            assert_eq!(expr.is_volatile(), expected, "{expr}");
            let aliased = expr.alias("result");
            assert!(!aliased.is_volatile_node());
            assert_eq!(aliased.is_volatile(), expected, "{aliased}");
        }
    }
}

#[tokio::test]
async fn volatile_higher_order_function_is_not_eliminated() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(
        NextValue {
            signature: HigherOrderSignature::any(0, Volatility::Volatile),
        },
    )));
    let mut results = Vec::new();
    let mut calls = Vec::new();
    for sql in [
        "SELECT next_value() AS first, next_value() AS second",
        "SELECT next_value() = next_value() AS equal",
    ] {
        NEXT_VALUE.store(0, Ordering::Relaxed);
        let batches = ctx.sql(sql).await?.collect().await?;
        let count = NEXT_VALUE.load(Ordering::Relaxed);
        println!("{sql}\n{}\ncalls={count}", batches_to_string(&batches));
        results.push(batches);
        calls.push(count);
    }

    let batch = &results[0][0];
    assert_ne!(
        batch.column(0).as_primitive::<Int64Type>().value(0),
        batch.column(1).as_primitive::<Int64Type>().value(0)
    );
    assert_batches_eq!(
        [
            "+-------+",
            "| equal |",
            "+-------+",
            "| false |",
            "+-------+"
        ],
        &results[1]
    );
    assert_eq!(calls, vec![2, 2]);
    Ok(())
}
