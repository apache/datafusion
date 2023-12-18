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

//! [`ScalarUDF`]: Scalar User Defined Functions

use crate::{
    ColumnarValue, Expr, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

/// Logical representation of a Scalar User Defined Function.
///
/// A scalar function produces a single row output for each row of input. This
/// struct contains the information DataFusion needs to plan and invoke
/// functions you supply such name, type signature, return type, and actual
/// implementation.
///
///
/// 1. For simple (less performant) use cases, use [`create_udf`] and [`simple_udf.rs`].
///
/// 2. For advanced use cases, use  [`ScalarUDFImpl`] and [`advanced_udf.rs`].
///
/// [`create_udf`]: crate::expr_fn::create_udf
/// [`simple_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udf.rs
/// [`advanced_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
#[derive(Clone)]
pub struct ScalarUDF {
    /// The name of the function
    name: String,
    /// The signature (the types of arguments that are supported)
    signature: Signature,
    /// Function that returns the return type given the argument types
    return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    fun: ScalarFunctionImplementation,
    /// Optional aliases for the function. This list should NOT include the value of `name` as well
    aliases: Vec<String>,
}

impl Debug for ScalarUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for ScalarUDF {}

impl std::hash::Hash for ScalarUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF from low level details.
    ///
    /// See  [`ScalarUDFImpl`] for a more convenient way to create a
    /// `ScalarUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement ScalarUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
            aliases: vec![],
        }
    }

    /// Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`ScalarUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> ScalarUDF
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        // TODO change the internal implementation to use the trait object
        let arc_fun = Arc::new(fun);
        let captured_self = arc_fun.clone();
        let return_type: ReturnTypeFunction = Arc::new(move |arg_types| {
            let return_type = captured_self.return_type(arg_types)?;
            Ok(Arc::new(return_type))
        });

        let captured_self = arc_fun.clone();
        let func: ScalarFunctionImplementation =
            Arc::new(move |args| captured_self.invoke(args));

        Self {
            name: arc_fun.name().to_string(),
            signature: arc_fun.signature().clone(),
            return_type: return_type.clone(),
            fun: func,
            aliases: arc_fun.aliases().to_vec(),
        }
    }

    /// Adds additional names that can be used to invoke this function, in addition to `name`
    pub fn with_aliases(
        mut self,
        aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        self.aliases
            .extend(aliases.into_iter().map(|s| s.to_string()));
        self
    }

    /// Returns a [`Expr`] logical expression to call this UDF with specified
    /// arguments.
    ///
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(crate::expr::ScalarFunction::new_udf(
            Arc::new(self.clone()),
            args,
        ))
    }

    /// Returns this function's name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the aliases for this function. See [`ScalarUDF::with_aliases`] for more details
    pub fn aliases(&self) -> &[String] {
        &self.aliases
    }

    /// Returns this function's [`Signature`] (what input types are accepted)
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The datatype this function returns given the input argument input types
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        // Old API returns an Arc of the datatype for some reason
        let res = (self.return_type)(args)?;
        Ok(res.as_ref().clone())
    }

    /// Return an [`Arc`] to the function implementation
    pub fn fun(&self) -> ScalarFunctionImplementation {
        self.fun.clone()
    }
}

impl<F> From<F> for ScalarUDF
where
    F: ScalarUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`ScalarUDF`].
///
/// This trait exposes the full API for implementing user defined functions and
/// can be used to implement any function.
///
/// See [`advanced_udf.rs`] for a full example with complete implementation and
/// [`ScalarUDF`] for other available options.
///
///
/// [`advanced_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility};
/// # use datafusion_expr::{ScalarUDFImpl, ScalarUDF};
/// struct AddOne {
///   signature: Signature
/// };
///
/// impl AddOne {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the ScalarUDFImpl trait for AddOne
/// impl ScalarUDFImpl for AddOne {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "add_one" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Int32)) {
///        return plan_err!("add_one only accepts Int32 arguments");
///      }
///      Ok(DataType::Int32)
///    }
///    // The actual implementation would add one to the argument
///    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> { unimplemented!() }
/// }
///
/// // Create a new ScalarUDF from the implementation
/// let add_one = ScalarUDF::from(AddOne::new());
///
/// // Call the function `add_one(col)`
/// let expr = add_one.call(vec![col("a")]);
/// ```
pub trait ScalarUDFImpl {
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

    /// Invoke the function on `args`, returning the appropriate result
    ///
    /// The function will be invoked passed with the slice of [`ColumnarValue`]
    /// (either scalar or array).
    ///
    /// # Zero Argument Functions
    /// If the function has zero parameters (e.g. `now()`) it will be passed a
    /// single element slice which is a a null array to indicate the batch's row
    /// count (so the function can know the resulting array size).
    ///
    /// # Performance
    ///
    /// For the best performance, the implementations of `invoke` should handle
    /// the common case when one or more of their arguments are constant values
    /// (aka  [`ColumnarValue::Scalar`]). Calling [`ColumnarValue::into_array`]
    /// and treating all arguments as arrays will work, but will be slower.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue>;

    /// Returns any aliases (alternate names) for this function. This should not
    /// include the value of [`Self::name`]. Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }
}
