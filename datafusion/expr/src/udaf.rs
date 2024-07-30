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

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::vec;

use arrow::datatypes::{DataType, Field};

use datafusion_common::{exec_err, not_impl_err, Result};

use crate::expr::AggregateFunction;
use crate::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use crate::groups_accumulator::GroupsAccumulator;
use crate::utils::format_state_name;
use crate::utils::AggregateOrderSensitivity;
use crate::{Accumulator, Expr};
use crate::{AccumulatorFactoryFunction, ReturnTypeFunction, Signature};

/// Logical representation of a user-defined [aggregate function] (UDAF).
///
/// An aggregate function combines the values from multiple input rows
/// into a single output "aggregate" (summary) row. It is different
/// from a scalar function because it is stateful across batches. User
/// defined aggregate functions can be used as normal SQL aggregate
/// functions (`GROUP BY` clause) as well as window functions (`OVER`
/// clause).
///
/// `AggregateUDF` provides DataFusion the information needed to plan and call
/// aggregate functions, including name, type information, and a factory
/// function to create an [`Accumulator`] instance, to perform the actual
/// aggregation.
///
/// For more information, please see [the examples]:
///
/// 1. For simple use cases, use [`create_udaf`] (examples in [`simple_udaf.rs`]).
///
/// 2. For advanced use cases, use [`AggregateUDFImpl`] which provides full API
///    access (examples in [`advanced_udaf.rs`]).
///
/// # API Note
/// This is a separate struct from `AggregateUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [the examples]: https://github.com/apache/datafusion/tree/main/datafusion-examples#single-process
/// [aggregate function]: https://en.wikipedia.org/wiki/Aggregate_function
/// [`Accumulator`]: crate::Accumulator
/// [`create_udaf`]: crate::expr_fn::create_udaf
/// [`simple_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udaf.rs
/// [`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
#[derive(Debug, Clone)]
pub struct AggregateUDF {
    inner: Arc<dyn AggregateUDFImpl>,
}

impl PartialEq for AggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.inner.equals(other.inner.as_ref())
    }
}

impl Eq for AggregateUDF {}

impl Hash for AggregateUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash_value().hash(state)
    }
}

impl fmt::Display for AggregateUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
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
    ) -> Self {
        Self::new_from_impl(AggregateUDFLegacyWrapper {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: Arc::clone(return_type),
            accumulator: Arc::clone(accumulator),
        })
    }

    /// Create a new `AggregateUDF` from a `[AggregateUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`AggregateUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> AggregateUDF
    where
        F: AggregateUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`AggregateUDFImpl`] trait object for this function
    pub fn inner(&self) -> &Arc<dyn AggregateUDFImpl> {
        &self.inner
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`AggregateUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedAggregateUDFImpl::new(
            Arc::clone(&self.inner),
            aliases,
        ))
    }

    /// creates an [`Expr`] that calls the aggregate function.
    ///
    /// This utility allows using the UDAF without requiring access to
    /// the registry, such as with the DataFrame API.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::new(self.clone()),
            args,
            false,
            None,
            None,
            None,
        ))
    }

    /// Returns this function's name
    ///
    /// See [`AggregateUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the aliases for this function.
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    /// Returns this function's signature (what input types are accepted)
    ///
    /// See [`AggregateUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Return the type of the function given its input types
    ///
    /// See [`AggregateUDFImpl::return_type`] for more details.
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }

    /// Return an accumulator the given aggregate, given its return datatype
    pub fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    /// Return the fields used to store the intermediate state for this aggregator, given
    /// the name of the aggregate, value type and ordering fields. See [`AggregateUDFImpl::state_fields`]
    /// for more details.
    ///
    /// This is used to support multi-phase aggregations
    pub fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        self.inner.state_fields(args)
    }

    /// See [`AggregateUDFImpl::groups_accumulator_supported`] for more details.
    pub fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    /// See [`AggregateUDFImpl::create_groups_accumulator`] for more details.
    pub fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    pub fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    pub fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    /// See [`AggregateUDFImpl::with_beneficial_ordering`] for more details.
    pub fn with_beneficial_ordering(
        self,
        beneficial_ordering: bool,
    ) -> Result<Option<AggregateUDF>> {
        self.inner
            .with_beneficial_ordering(beneficial_ordering)
            .map(|updated_udf| updated_udf.map(|udf| Self { inner: udf }))
    }

    /// Gets the order sensitivity of the UDF. See [`AggregateOrderSensitivity`]
    /// for possible options.
    pub fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    /// Reserves the `AggregateUDF` (e.g. returns the `AggregateUDF` that will
    /// generate same result with this `AggregateUDF` when iterated in reverse
    /// order, and `None` if there is no such `AggregateUDF`).
    pub fn reverse_udf(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    /// Do the function rewrite
    ///
    /// See [`AggregateUDFImpl::simplify`] for more details.
    pub fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    /// Returns true if the function is max, false if the function is min
    /// None in all other cases, used in certain optimizations or
    /// or aggregate
    ///
    pub fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
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
/// [`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
///
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility, Expr};
/// # use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator, function::{AccumulatorArgs, StateFieldsArgs}};
/// # use arrow::datatypes::Schema;
/// # use arrow::datatypes::Field;
/// #[derive(Debug, Clone)]
/// struct GeoMeanUdf {
///   signature: Signature
/// }
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
///    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> { unimplemented!() }
///    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
///        Ok(vec![
///             Field::new("value", args.return_type.clone(), true),
///             Field::new("ordering", DataType::UInt32, true)
///        ])
///    }
/// }
///
/// // Create a new AggregateUDF from the implementation
/// let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());
///
/// // Call the function `geo_mean(col)`
/// let expr = geometric_mean.call(vec![col("a")]);
/// ```
pub trait AggregateUDFImpl: Debug + Send + Sync {
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

    /// Return a new [`Accumulator`] that aggregates values for a specific
    /// group during query execution.
    ///
    /// acc_args: [`AccumulatorArgs`] contains information about how the
    /// aggregate function was called.
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>;

    /// Return the fields used to store the intermediate state of this accumulator.
    ///
    /// # Arguments:
    /// 1. `name`: the name of the expression (e.g. AVG, SUM, etc)
    /// 2. `value_type`: Aggregate function output returned by [`Self::return_type`] if defined, otherwise
    ///    it is equivalent to the data type of the first arguments
    /// 3. `ordering_fields`: the fields used to order the input arguments, if any.
    ///    Empty if no ordering expression is provided.
    ///
    /// # Notes:
    ///
    /// The default implementation returns a single state field named `name`
    /// with the same type as `value_type`. This is suitable for aggregates such
    /// as `SUM` or `MIN` where partial state can be combined by applying the
    /// same aggregate.
    ///
    /// For aggregates such as `AVG` where the partial state is more complex
    /// (e.g. a COUNT and a SUM), this method is used to define the additional
    /// fields.
    ///
    /// The name of the fields must be unique within the query and thus should
    /// be derived from `name`. See [`format_state_name`] for a utility function
    /// to generate a unique name.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let fields = vec![Field::new(
            format_state_name(args.name, "value"),
            args.return_type.clone(),
            true,
        )];

        Ok(fields
            .into_iter()
            .chain(args.ordering_fields.to_vec())
            .collect())
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator]` will be called.
    ///
    /// # Notes
    ///
    /// Even if this function returns true, DataFusion will still use
    /// `Self::accumulator` for certain queries, such as when this aggregate is
    /// used as a window function or when there no GROUP BY columns in the
    /// query.
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("GroupsAccumulator hasn't been implemented for {self:?} yet")
    }

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }

    /// Sliding accumulator is an alternative accumulator that can be used for
    /// window functions. It has retract method to revert the previous update.
    ///
    /// See [retract_batch] for more details.
    ///
    /// [retract_batch]: crate::accumulator::Accumulator::retract_batch
    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }

    /// Sets the indicator whether ordering requirements of the AggregateUDFImpl is
    /// satisfied by its input. If this is not the case, UDFs with order
    /// sensitivity `AggregateOrderSensitivity::Beneficial` can still produce
    /// the correct result with possibly more work internally.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(updated_udf))` if the process completes successfully.
    /// If the expression can benefit from existing input ordering, but does
    /// not implement the method, returns an error. Order insensitive and hard
    /// requirement aggregators return `Ok(None)`.
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        if self.order_sensitivity().is_beneficial() {
            return exec_err!(
                "Should implement with satisfied for aggregator :{:?}",
                self.name()
            );
        }
        Ok(None)
    }

    /// Gets the order sensitivity of the UDF. See [`AggregateOrderSensitivity`]
    /// for possible options.
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        // We have hard ordering requirements by default, meaning that order
        // sensitive UDFs need their input orderings to satisfy their ordering
        // requirements to generate correct results.
        AggregateOrderSensitivity::HardRequirement
    }

    /// Optionally apply per-UDaF simplification / rewrite rules.
    ///
    /// This can be used to apply function specific simplification rules during
    /// optimization (e.g. `arrow_cast` --> `Expr::Cast`). The default
    /// implementation does nothing.
    ///
    /// Note that DataFusion handles simplifying arguments and  "constant
    /// folding" (replacing a function call with constant arguments such as
    /// `my_add(1,2) --> 3` ). Thus, there is no need to implement such
    /// optimizations manually for specific UDFs.
    ///
    /// # Returns
    ///
    /// [None] if simplify is not defined or,
    ///
    /// Or, a closure with two arguments:
    /// * 'aggregate_function': [crate::expr::AggregateFunction] for which simplified has been invoked
    /// * 'info': [crate::simplify::SimplifyInfo]
    ///
    /// closure returns simplified [Expr] or an error.
    ///
    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        None
    }

    /// Returns the reverse expression of the aggregate function.
    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::NotSupported
    }

    /// Coerce arguments of a function call to types that the function can evaluate.
    ///
    /// This function is only called if [`AggregateUDFImpl::signature`] returns [`crate::TypeSignature::UserDefined`]. Most
    /// UDAFs should return one of the other variants of `TypeSignature` which handle common
    /// cases
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// For example, if your function requires a floating point arguments, but the user calls
    /// it like `my_func(1::int)` (aka with `1` as an integer), coerce_types could return `[DataType::Float64]`
    /// to ensure the argument was cast to `1::double`
    ///
    /// # Parameters
    /// * `arg_types`: The argument types of the arguments  this function with
    ///
    /// # Return value
    /// A Vec the same length as `arg_types`. DataFusion will `CAST` the function call
    /// arguments to these specific types.
    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        not_impl_err!("Function {} does not implement coerce_types", self.name())
    }

    /// Return true if this aggregate UDF is equal to the other.
    ///
    /// Allows customizing the equality of aggregate UDFs.
    /// Must be consistent with [`Self::hash_value`] and follow the same rules as [`Eq`]:
    ///
    /// - reflexive: `a.equals(a)`;
    /// - symmetric: `a.equals(b)` implies `b.equals(a)`;
    /// - transitive: `a.equals(b)` and `b.equals(c)` implies `a.equals(c)`.
    ///
    /// By default, compares [`Self::name`] and [`Self::signature`].
    fn equals(&self, other: &dyn AggregateUDFImpl) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }

    /// Returns a hash value for this aggregate UDF.
    ///
    /// Allows customizing the hash code of aggregate UDFs. Similarly to [`Hash`] and [`Eq`],
    /// if [`Self::equals`] returns true for two UDFs, their `hash_value`s must be the same.
    ///
    /// By default, hashes [`Self::name`] and [`Self::signature`].
    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.name().hash(hasher);
        self.signature().hash(hasher);
        hasher.finish()
    }

    /// If this function is max, return true
    /// if the function is min, return false
    /// otherwise return None (the default)
    ///
    ///
    /// Note: this is used to use special aggregate implementations in certain conditions
    fn is_descending(&self) -> Option<bool> {
        None
    }
}

pub enum ReversedUDAF {
    /// The expression is the same as the original expression, like SUM, COUNT
    Identical,
    /// The expression does not support reverse calculation
    NotSupported,
    /// The expression is different from the original expression
    Reversed(Arc<AggregateUDF>),
}

/// AggregateUDF that adds an alias to the underlying function. It is better to
/// implement [`AggregateUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedAggregateUDFImpl {
    inner: Arc<dyn AggregateUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedAggregateUDFImpl {
    pub fn new(
        inner: Arc<dyn AggregateUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));

        Self { inner, aliases }
    }
}

impl AggregateUDFImpl for AliasedAggregateUDFImpl {
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

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn equals(&self, other: &dyn AggregateUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<AliasedAggregateUDFImpl>() {
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

/// Implementation of [`AggregateUDFImpl`] that wraps the function style pointers
/// of the older API
pub struct AggregateUDFLegacyWrapper {
    /// name
    name: String,
    /// Signature (input arguments)
    signature: Signature,
    /// Return type
    return_type: ReturnTypeFunction,
    /// actual implementation
    accumulator: AccumulatorFactoryFunction,
}

impl Debug for AggregateUDFLegacyWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl AggregateUDFImpl for AggregateUDFLegacyWrapper {
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

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(acc_args)
    }
}
