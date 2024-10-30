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

use arrow_schema::{Field, Schema};

use datafusion::arrow::{array::Float32Array, record_batch::RecordBatch};
use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use datafusion::{assert_batches_eq, prelude::*};
use datafusion_common::cast::as_float64_array;
use datafusion_expr::function::{AggregateFunctionSimplification, StateFieldsArgs};
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::{
    expr::AggregateFunction, function::AccumulatorArgs, Accumulator, AggregateUDF,
    AggregateUDFImpl, GroupsAccumulator, Signature,
};

/// This example shows how to use the AggregateUDFImpl::simplify API to simplify/replace user
/// defined aggregate function with a different expression which is defined in the `simplify` method.

#[derive(Debug, Clone)]
struct BetterAvgUdaf {
    signature: Signature,
}

impl BetterAvgUdaf {
    /// Create a new instance of the GeoMeanUdaf struct
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BetterAvgUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "better_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        unimplemented!("should not be invoked")
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        unimplemented!("should not be invoked")
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        unimplemented!("should not get here");
    }

    // we override method, to return new expression which would substitute
    // user defined function call
    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        // as an example for this functionality we replace UDF function
        // with build-in aggregate function to illustrate the use
        let simplify = |aggregate_function: AggregateFunction, _: &dyn SimplifyInfo| {
            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                avg_udaf(),
                // yes it is the same Avg, `BetterAvgUdaf` was just a
                // marketing pitch :)
                aggregate_function.args,
                aggregate_function.distinct,
                aggregate_function.filter,
                aggregate_function.order_by,
                aggregate_function.null_treatment,
            )))
        };

        Some(Box::new(simplify))
    }
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float32, false),
    ]));

    // define data in two partitions
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0])),
            Arc::new(Float32Array::from(vec![2.0, 2.0, 2.0])),
        ],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![16.0])),
            Arc::new(Float32Array::from(vec![2.0])),
        ],
    )?;

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    let better_avg = AggregateUDF::from(BetterAvgUdaf::new());
    ctx.register_udaf(better_avg.clone());

    let result = ctx
        .sql("SELECT better_avg(a) FROM t group by b")
        .await?
        .collect()
        .await?;

    let expected = [
        "+-----------------+",
        "| better_avg(t.a) |",
        "+-----------------+",
        "| 7.5             |",
        "+-----------------+",
    ];

    assert_batches_eq!(expected, &result);

    let df = ctx.table("t").await?;
    let df = df.aggregate(vec![], vec![better_avg.call(vec![col("a")])])?;

    let results = df.collect().await?;
    let result = as_float64_array(results[0].column(0))?;

    assert!((result.value(0) - 7.5).abs() < f64::EPSILON);
    println!("The average of [2,4,8,16] is {}", result.value(0));

    Ok(())
}
