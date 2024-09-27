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

use std::any::Any;

use arrow_schema::{DataType, Field};

use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::{error::Result, execution::options::CsvReadOptions};
use datafusion_expr::function::{WindowFunctionSimplification, WindowUDFFieldArgs};
use datafusion_expr::{
    expr::WindowFunction, simplify::SimplifyInfo, Expr, PartitionEvaluator, Signature,
    Volatility, WindowUDF, WindowUDFImpl,
};

/// This UDWF will show how to use the WindowUDFImpl::simplify() API
#[derive(Debug, Clone)]
struct SimplifySmoothItUdf {
    signature: Signature,
}

impl SimplifySmoothItUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                // this function will always take one arguments of type f64
                vec![DataType::Float64],
                // this function is deterministic and will always return the same
                // result for the same input
                Volatility::Immutable,
            ),
        }
    }
}
impl WindowUDFImpl for SimplifySmoothItUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "simplify_smooth_it"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        todo!()
    }

    /// this function will simplify `SimplifySmoothItUdf` to `SmoothItUdf`.
    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        let simplify = |window_function: datafusion_expr::expr::WindowFunction,
                        _: &dyn SimplifyInfo| {
            Ok(Expr::WindowFunction(WindowFunction {
                fun: datafusion_expr::WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                args: window_function.args,
                partition_by: window_function.partition_by,
                order_by: window_function.order_by,
                window_frame: window_function.window_frame,
                null_treatment: window_function.null_treatment,
            }))
        };

        Some(Box::new(simplify))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, true))
    }
}

// create local execution context with `cars.csv` registered as a table named `cars`
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQL session
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    println!("pwd: {}", std::env::current_dir().unwrap().display());
    let csv_path = "../../datafusion/core/tests/data/cars.csv".to_string();
    let read_options = CsvReadOptions::default().has_header(true);

    ctx.register_csv("cars", &csv_path, read_options).await?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context().await?;
    let simplify_smooth_it = WindowUDF::from(SimplifySmoothItUdf::new());
    ctx.register_udwf(simplify_smooth_it.clone());

    // Use SQL to run the new window function
    let df = ctx.sql("SELECT * from cars").await?;
    // print the results
    df.show().await?;

    let df = ctx
        .sql(
            "SELECT \
               car, \
               speed, \
               simplify_smooth_it(speed) OVER (PARTITION BY car ORDER BY time) AS smooth_speed,\
               time \
               from cars \
             ORDER BY \
               car",
        )
        .await?;
    // print the results
    df.show().await?;

    Ok(())
}
