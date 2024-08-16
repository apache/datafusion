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

use arrow::compute::SortOptions;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{
    any::Any,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use arrow::datatypes::DataType;

use datafusion_common::Result;

use crate::expr::WindowFunction;
use crate::{
    function::WindowFunctionSimplification, Expr, PartitionEvaluator,
    PartitionEvaluatorFactory, ReturnTypeFunction, Signature,
};

/// Logical representation of a user-defined window function (UDWF)
/// A UDWF is different from a UDF in that it is stateful across batches.
///
/// See the documentation on [`PartitionEvaluator`] for more details
///
/// 1. For simple use cases, use [`create_udwf`] (examples in
///    [`simple_udwf.rs`]).
///
/// 2. For advanced use cases, use [`WindowUDFImpl`] which provides full API
///    access (examples in [`advanced_udwf.rs`]).
///
/// # API Note
/// This is a separate struct from `WindowUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [`PartitionEvaluator`]: crate::PartitionEvaluator
/// [`create_udwf`]: crate::expr_fn::create_udwf
/// [`simple_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udwf.rs
/// [`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
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
        self.inner.equals(other.inner.as_ref())
    }
}

impl Eq for WindowUDF {}

impl Hash for WindowUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash_value().hash(state)
    }
}

impl WindowUDF {
    /// Create a new WindowUDF from low level details.
    ///
    /// See [`WindowUDFImpl`] for a more convenient way to create a
    /// `WindowUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement WindowUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        partition_evaluator_factory: &PartitionEvaluatorFactory,
    ) -> Self {
        Self::new_from_impl(WindowUDFLegacyWrapper {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: Arc::clone(return_type),
            partition_evaluator_factory: Arc::clone(partition_evaluator_factory),
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
    pub fn inner(&self) -> &Arc<dyn WindowUDFImpl> {
        &self.inner
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`WindowUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedWindowUDFImpl::new(Arc::clone(&self.inner), aliases))
    }

    /// creates a [`Expr`] that calls the window function with default
    /// values for `order_by`, `partition_by`, `window_frame`.
    ///
    /// See [`ExprFunctionExt`] for details on setting these values.
    ///
    /// This utility allows using a user defined window function without
    /// requiring access to the registry, such as with the DataFrame API.
    ///
    /// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        let fun = crate::WindowFunctionDefinition::WindowUDF(Arc::new(self.clone()));

        Expr::WindowFunction(WindowFunction::new(fun, args))
    }

    /// Returns this function's name
    ///
    /// See [`WindowUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the aliases for this function.
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
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

    /// Do the function rewrite
    ///
    /// See [`WindowUDFImpl::simplify`] for more details.
    pub fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.inner.simplify()
    }

    /// Return a `PartitionEvaluator` for evaluating this window function
    pub fn partition_evaluator_factory(&self) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator()
    }

    /// Returns if column values are nullable for this window function.
    ///
    /// See [`WindowUDFImpl::nullable`] for more details.
    pub fn nullable(&self) -> bool {
        self.inner.nullable()
    }

    /// Returns custom result ordering introduced by this window function
    /// which is used to update ordering equivalences.
    ///
    /// See [`WindowUDFImpl::sort_options`] for more details.
    pub fn sort_options(&self) -> Option<SortOptions> {
        self.inner.sort_options()
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
/// [`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, Signature, Volatility, PartitionEvaluator, WindowFrame, ExprFunctionExt};
/// # use datafusion_expr::{WindowUDFImpl, WindowUDF};
/// #[derive(Debug, Clone)]
/// struct SmoothIt {
///   signature: Signature
/// }
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
/// // Create a new WindowUDF from the implementation
/// let smooth_it = WindowUDF::from(SmoothIt::new());
///
/// // Call the function `add_one(col)`
/// // smooth_it(speed) OVER (PARTITION BY car ORDER BY time ASC)
/// let expr = smooth_it.call(vec![col("speed")])
///     .partition_by(vec![col("car")])
///     .order_by(vec![col("time").sort(true, true)])
///     .window_frame(WindowFrame::new(None))
///     .build()
///     .unwrap();
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

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }

    /// Optionally apply per-UDWF simplification / rewrite rules.
    ///
    /// This can be used to apply function specific simplification rules during
    /// optimization. The default implementation does nothing.
    ///
    /// Note that DataFusion handles simplifying arguments and  "constant
    /// folding" (replacing a function call with constant arguments such as
    /// `my_add(1,2) --> 3` ). Thus, there is no need to implement such
    /// optimizations manually for specific UDFs.
    ///
    /// Example:
    /// [`simplify_udwf_expression.rs`]: <https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simplify_udwf_expression.rs>
    ///
    /// # Returns
    /// [None] if simplify is not defined or,
    ///
    /// Or, a closure with two arguments:
    /// * 'window_function': [crate::expr::WindowFunction] for which simplified has been invoked
    /// * 'info': [crate::simplify::SimplifyInfo]
    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        None
    }

    /// Return true if this window UDF is equal to the other.
    ///
    /// Allows customizing the equality of window UDFs.
    /// Must be consistent with [`Self::hash_value`] and follow the same rules as [`Eq`]:
    ///
    /// - reflexive: `a.equals(a)`;
    /// - symmetric: `a.equals(b)` implies `b.equals(a)`;
    /// - transitive: `a.equals(b)` and `b.equals(c)` implies `a.equals(c)`.
    ///
    /// By default, compares [`Self::name`] and [`Self::signature`].
    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }

    /// Returns a hash value for this window UDF.
    ///
    /// Allows customizing the hash code of window UDFs. Similarly to [`Hash`] and [`Eq`],
    /// if [`Self::equals`] returns true for two UDFs, their `hash_value`s must be the same.
    ///
    /// By default, hashes [`Self::name`] and [`Self::signature`].
    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.name().hash(hasher);
        self.signature().hash(hasher);
        hasher.finish()
    }

    /// Allows customizing nullable of column for this window UDF.
    ///
    /// By default, the final result of evaluating the window UDF is
    /// allowed to have null values. But if that is not the case then
    /// it can be customized in the window UDF implementation.
    fn nullable(&self) -> bool {
        true
    }

    /// Allows the window UDF to define a custom result ordering.
    ///
    /// By default, a window UDF doesn't introduce an ordering.
    /// But when specified by a window UDF this is used to update
    /// ordering equivalences.
    fn sort_options(&self) -> Option<SortOptions> {
        None
    }
}

/// WindowUDF that adds an alias to the underlying function. It is better to
/// implement [`WindowUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedWindowUDFImpl {
    inner: Arc<dyn WindowUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedWindowUDFImpl {
    pub fn new(
        inner: Arc<dyn WindowUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));

        Self { inner, aliases }
    }
}

impl WindowUDFImpl for AliasedWindowUDFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<AliasedWindowUDFImpl>() {
            self.inner.equals(other.inner.as_ref()) && self.aliases == other.aliases
        } else {
            false
        }
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.inner.hash_value().hash(hasher);
        self.aliases.hash(hasher);
        hasher.finish()
    }
}

/// Implementation of [`WindowUDFImpl`] that wraps the function style pointers
/// of the older API (see <https://github.com/apache/datafusion/pull/8719>
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
