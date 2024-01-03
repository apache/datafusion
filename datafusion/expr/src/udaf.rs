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

//! [`AggregateUDF`]: User Defined Aggregate Functions

use crate::{Accumulator, Expr};
use crate::{
    AccumulatorFactoryFunction, ReturnTypeFunction, Signature, StateTypeFunction,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Logical representation of a user-defined [aggregate function] (UDAF).
///
/// An aggregate function combines the values from multiple input rows
/// into a single output "aggregate" (summary) row. It is different
/// from a scalar function because it is stateful across batches. User
/// defined aggregate functions can be used as normal SQL aggregate
/// functions (`GROUP BY` clause) as well as window functions (`OVER`
/// clause).
///
/// `AggregateUDF` provides DataFusion the information needed to plan
/// and call aggregate functions, including name, type information,
/// and a factory function to create [`Accumulator`], which peform the
/// actual aggregation.
///
/// For more information, please see [the examples].
///
/// [the examples]: https://github.com/apache/arrow-datafusion/tree/main/datafusion-examples#single-process
/// [aggregate function]: https://en.wikipedia.org/wiki/Aggregate_function
/// [`Accumulator`]: crate::Accumulator
#[derive(Clone)]
pub struct AggregateUDF {
    /// name
    name: String,
    /// Signature (input arguments)
    signature: Signature,
    /// Return type
    return_type: ReturnTypeFunction,
    /// actual implementation
    accumulator: AccumulatorFactoryFunction,
    /// the accumulator's state's description as a function of the return type
    state_type: StateTypeFunction,
}

impl Debug for AggregateUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for AggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for AggregateUDF {}

impl std::hash::Hash for AggregateUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
    }
}

impl AggregateUDF {
    /// Create a new AggregateUDF
    ///
    /// See  [`AggregateUDFImpl`] for a more convenient way to create a
    /// `AggregateUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement AggregateUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        accumulator: &AccumulatorFactoryFunction,
        state_type: &StateTypeFunction,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            accumulator: accumulator.clone(),
            state_type: state_type.clone(),
        }
    }

    /// Create a new `AggregateUDF` from a `[AggregateUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`AggregateUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> AggregateUDF
    where
        F: AggregateUDFImpl + Send + Sync + 'static,
    {
        let arc_fun = Arc::new(fun);
        let captured_self = arc_fun.clone();
        let return_type: ReturnTypeFunction = Arc::new(move |arg_types| {
            let return_type = captured_self.return_type(arg_types)?;
            Ok(Arc::new(return_type))
        });

        let captured_self = arc_fun.clone();
        let accumulator: AccumulatorFactoryFunction =
            Arc::new(move |arg| captured_self.accumulator(arg));

        let captured_self = arc_fun.clone();
        let state_type: StateTypeFunction = Arc::new(move |return_type| {
            let state_type = captured_self.state_type(return_type)?;
            Ok(Arc::new(state_type))
        });

        Self {
            name: arc_fun.name().to_string(),
            signature: arc_fun.signature().clone(),
            return_type: return_type.clone(),
            accumulator,
            state_type,
        }
    }

    /// creates an [`Expr`] that calls the aggregate function.
    ///
    /// This utility allows using the UDAF without requiring access to
    /// the registry, such as with the DataFrame API.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateFunction(crate::expr::AggregateFunction::new_udf(
            Arc::new(self.clone()),
            args,
            false,
            None,
            None,
        ))
    }

    /// Returns this function's name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns this function's signature (what input types are accepted)
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Return the type of the function given its input types
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        // Old API returns an Arc of the datatype for some reason
        let res = (self.return_type)(args)?;
        Ok(res.as_ref().clone())
    }

    /// Return an accumualator the given aggregate, given
    /// its return datatype.
    pub fn accumulator(&self, return_type: &DataType) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(return_type)
    }

    /// Return the type of the intermediate state used by this aggregator, given
    /// its return datatype. Supports multi-phase aggregations
    pub fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>> {
        // old API returns an Arc for some reason, try and unwrap it here
        let res = (self.state_type)(return_type)?;
        Ok(Arc::try_unwrap(res).unwrap_or_else(|res| res.as_ref().clone()))
    }
}

impl<F> From<F> for AggregateUDF
where
    F: AggregateUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`AggregateUDF`].
///
/// This trait exposes the full API for implementing user defined aggregate functions and
/// can be used to implement any function.
///
/// See [`advanced_udaf.rs`] for a full example with complete implementation and
/// [`AggregateUDF`] for other available options.
///
///
/// [`advanced_udaf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility};
/// # use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator};
/// struct GeoMeanUdf {
///   signature: Signature
/// };
///
/// impl GeoMeanUdf {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the AggregateUDFImpl trait for GeoMeanUdf
/// impl AggregateUDFImpl for GeoMeanUdf {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "geo_mean" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Float64)) {
///        return plan_err!("add_one only accepts Float64 arguments");
///      }
///      Ok(DataType::Float64)
///    }
///    // This is the accumulator factory; DataFusion uses it to create new accumulators.
///    fn accumulator(&self, _arg: &DataType) -> Result<Box<dyn Accumulator>> { unimplemented!() }
///    fn state_type(&self, _return_type: &DataType) -> Result<Vec<DataType>> {
///        Ok(vec![DataType::Float64, DataType::UInt32])
///    }
/// }
///
/// // Create a new AggregateUDF from the implementation
/// let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());
///
/// // Call the function `geo_mean(col)`
/// let expr = geometric_mean.call(vec![col("a")]);
/// ```
pub trait AggregateUDFImpl {
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// This is the accumulator factory [`AccumulatorFactoryFunction`];
    /// DataFusion uses it to create new accumulators.
    fn accumulator(&self, arg: &DataType) -> Result<Box<dyn Accumulator>>;

    /// This is the description of the state.
    /// accumulator's state() must match the types here.
    fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>>;
}
