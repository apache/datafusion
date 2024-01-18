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

//! [`WindowUDF`]: User Defined Window Functions

use crate::{
    Expr, PartitionEvaluator, PartitionEvaluatorFactory, ReturnTypeFunction, Signature,
    WindowFrame,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::{
    any::Any,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

/// Logical representation of a user-defined window function (UDWF)
/// A UDWF is different from a UDF in that it is stateful across batches.
///
/// See the documetnation on [`PartitionEvaluator`] for more details
///
/// 1. For simple (less performant) use cases, use [`create_udwf`] and [`simple_udwf.rs`].
///
/// 2. For advanced use cases, use [`WindowUDFImpl`] and [`advanced_udwf.rs`].
///
/// # API Note
/// This is a separate struct from `WindowUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [`PartitionEvaluator`]: crate::PartitionEvaluator
/// [`create_udwf`]: crate::expr_fn::create_udwf
/// [`simple_udwf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udwf.rs
/// [`advanced_udwf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
#[derive(Debug, Clone)]
pub struct WindowUDF {
    inner: Arc<dyn WindowUDFImpl>,
}

/// Defines how the WindowUDF is shown to users
impl Display for WindowUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl PartialEq for WindowUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }
}

impl Eq for WindowUDF {}

impl std::hash::Hash for WindowUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.signature().hash(state);
    }
}

impl WindowUDF {
    /// Create a new WindowUDF from low level details.
    ///
    /// See [`WindowUDFImpl`] for a more convenient way to create a
    /// `WindowUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement ScalarUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        partition_evaluator_factory: &PartitionEvaluatorFactory,
    ) -> Self {
        Self::new_from_impl(WindowUDFLegacyWrapper {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            partition_evaluator_factory: partition_evaluator_factory.clone(),
        })
    }

    /// Create a new `WindowUDF` from a `[WindowUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`WindowUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> WindowUDF
    where
        F: WindowUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`WindowUDFImpl`] trait object for this function
    pub fn inner(&self) -> Arc<dyn WindowUDFImpl> {
        self.inner.clone()
    }

    /// creates a [`Expr`] that calls the window function given
    /// the `partition_by`, `order_by`, and `window_frame` definition
    ///
    /// This utility allows using the UDWF without requiring access to
    /// the registry, such as with the DataFrame API.
    pub fn call(
        &self,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<Expr>,
        window_frame: WindowFrame,
    ) -> Expr {
        let fun = crate::WindowFunctionDefinition::WindowUDF(Arc::new(self.clone()));

        Expr::WindowFunction(crate::expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        })
    }

    /// Returns this function's name
    ///
    /// See [`WindowUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns this function's signature (what input types are accepted)
    ///
    /// See [`WindowUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Return the type of the function given its input types
    ///
    /// See [`WindowUDFImpl::return_type`] for more details.
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }

    /// Return a `PartitionEvaluator` for evaluating this window function
    pub fn partition_evaluator_factory(&self) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator()
    }
}

impl<F> From<F> for WindowUDF
where
    F: WindowUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`WindowUDF`].
///
/// This trait exposes the full API for implementing user defined window functions and
/// can be used to implement any function.
///
/// See [`advanced_udwf.rs`] for a full example with complete implementation and
/// [`WindowUDF`] for other available options.
///
///
/// [`advanced_udwf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, Signature, Volatility, PartitionEvaluator, WindowFrame};
/// # use datafusion_expr::{WindowUDFImpl, WindowUDF};
/// #[derive(Debug, Clone)]
/// struct SmoothIt {
///   signature: Signature
/// };
///
/// impl SmoothIt {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the WindowUDFImpl trait for AddOne
/// impl WindowUDFImpl for SmoothIt {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "smooth_it" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Int32)) {
///        return plan_err!("smooth_it only accepts Int32 arguments");
///      }
///      Ok(DataType::Int32)
///    }
///    // The actual implementation would add one to the argument
///    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> { unimplemented!() }
/// }
///
/// // Create a new ScalarUDF from the implementation
/// let smooth_it = WindowUDF::from(SmoothIt::new());
///
/// // Call the function `add_one(col)`
/// let expr = smooth_it.call(
///     vec![col("speed")],                 // smooth_it(speed)
///     vec![col("car")],                   // PARTITION BY car
///     vec![col("time").sort(true, true)], // ORDER BY time ASC
///     WindowFrame::new(None),
/// );
/// ```
pub trait WindowUDFImpl: Debug + Send + Sync {
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

    /// Invoke the function, returning the [`PartitionEvaluator`] instance
    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>>;
}

/// Implementation of [`WindowUDFImpl`] that wraps the function style pointers
/// of the older API (see <https://github.com/apache/arrow-datafusion/pull/8719>
/// for more details)
pub struct WindowUDFLegacyWrapper {
    /// name
    name: String,
    /// signature
    signature: Signature,
    /// Return type
    return_type: ReturnTypeFunction,
    /// Return the partition evaluator
    partition_evaluator_factory: PartitionEvaluatorFactory,
}

impl Debug for WindowUDFLegacyWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("WindowUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("return_type", &"<func>")
            .field("partition_evaluator_factory", &"<func>")
            .finish_non_exhaustive()
    }
}

impl WindowUDFImpl for WindowUDFLegacyWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Old API returns an Arc of the datatype for some reason
        let res = (self.return_type)(arg_types)?;
        Ok(res.as_ref().clone())
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        (self.partition_evaluator_factory)()
    }
}
