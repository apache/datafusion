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

use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, Float32Array},
    record_batch::RecordBatch,
};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{cast::as_float64_array, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature};

/// This example shows how to use the full AggregateUDFImpl API to implement a user
/// defined aggregate function. As in the `simple_udaf.rs` example, this struct implements
/// a function `accumulator` that returns the `Accumulator` instance.
///
/// To do so, we must implement the `AggregateUDFImpl` trait.
#[derive(Debug, Clone)]
struct GeoMeanUdf {
    signature: Signature,
}

impl GeoMeanUdf {
    /// Create a new instance of the GeoMeanUdf struct
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

impl AggregateUDFImpl for GeoMeanUdf {
    /// We implement as_any so that we can downcast the AggregateUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "geo_mean"
    }

    /// Return the "signature" of this function -- namely that types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What is the type of value that will be returned by this function.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    /// This is the accumulator factory; DataFusion uses it to create new accumulators.
    fn accumulator(&self, _arg: &DataType) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GeometricMean::new()))
    }

    /// This is the description of the state. accumulator's state() must match the types here.
    fn state_type(&self, _return_type: &DataType) -> Result<Vec<DataType>> {
        Ok(vec![DataType::Float64, DataType::UInt32])
    }
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::Float64(Some(value)) = v {
                self.prod *= value;
                self.n += 1;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    // Merge the output of `Self::state()` from other instances of this accumulator
    // into this accumulator's state
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) =
                (&v[0], &v[1])
            {
                self.prod *= prod;
                self.n += n;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(vec![64.0]))],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // create the AggregateUDF
    let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());
    ctx.register_udaf(geometric_mean.clone());

    let sql_df = ctx.sql("SELECT geo_mean(a) FROM t").await?;
    sql_df.show().await?;

    // get a DataFrame from the context
    // this table has 1 column `a` f32 with values {2,4,8,64}, whose geometric mean is 8.0.
    let df = ctx.table("t").await?;

    // perform the aggregation
    let df = df.aggregate(vec![], vec![geometric_mean.call(vec![col("a")])])?;

    // note that "a" is f32, not f64. DataFusion coerces it to match the UDAF's signature.

    // execute the query
    let results = df.collect().await?;

    // downcast the array to the expected type
    let result = as_float64_array(results[0].column(0))?;

    // verify that the calculation is correct
    assert!((result.value(0) - 8.0).abs() < f64::EPSILON);
    println!("The geometric mean of [2,4,8,64] is {}", result.value(0));

    Ok(())
}
