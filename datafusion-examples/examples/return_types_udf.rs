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

use datafusion::{
    arrow::{
        array::{Float32Array, Float64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    logical_expr::Volatility,
};

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{internal_err, DataFusionError, DFSchema, ExprSchema, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature
};
use std::{any::Any, sync::Arc};

// create local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::{Field, Schema};
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float32Array::from(vec![2.1, 3.1, 4.1, 5.1, 6.1])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

#[derive(Debug)]
struct UDFWithExprReturn {
    signature: Signature,
}

impl UDFWithExprReturn {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

//Implement the ScalarUDFImpl trait for UDFWithExprReturn
impl ScalarUDFImpl for UDFWithExprReturn {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "my_cast"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }
    // An example of how to use the exprs to determine the return type
    // If the third argument is '0', return the type of the first argument
    // If the third argument is '1', return the type of the second argument
    fn return_type_from_exprs(
        &self,
        arg_exprs: &[Expr],
        schema: &DFSchema,
    ) -> Option<Result<DataType>> {
        if arg_exprs.len() != 3 {
            return Some(internal_err!("The size of the args must be 3."));
        }
        let take_idx = match arg_exprs.get(2).unwrap() {
            Expr::Literal(ScalarValue::Int64(Some(idx))) if (idx == &0 || idx == &1) => {
                *idx as usize
            }
            _ => unreachable!(),
        };
        Some(arg_exprs.get(take_idx).unwrap().get_type(schema))
    }
    // The actual implementation would add one to the argument
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let take_idx = match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) if v < &2 => *v as usize,
            _ => unreachable!(),
        };
        match &args[take_idx] {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(array.clone())),
            ColumnarValue::Scalar(_) => unimplemented!(),
        }
    }
}



#[tokio::main]
async fn main() -> Result<()> {
    // Create a new ScalarUDF from the implementation
    let udf_with_expr_return = ScalarUDF::from(UDFWithExprReturn::new());

    let ctx = create_context()?;

    ctx.register_udf(udf_with_expr_return);

    // SELECT take(a, b, 0) AS take0, take(a, b, 1) AS take1 FROM t;
    let df = ctx.table("t").await?;
    let take = df.registry().udf("my_cast")?;
    let expr0 = take
        .call(vec![col("a"), col("b"), lit(0_i64)]);
    let expr1 = take
        .call(vec![col("a"), col("b"), lit(1_i64)]);

    let df = df.select(vec![expr0, expr1])?;
    let schema = df.schema();

    // Check output schema
    assert_eq!(schema.field(0).data_type(), &DataType::Float32);
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);

    df.show().await?;
    

    Ok(())
}
