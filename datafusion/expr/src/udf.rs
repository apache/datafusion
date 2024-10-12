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

use crate::expr::schema_name_from_exprs_comma_seperated_without_space;
use crate::simplify::{ExprSimplifyResult, SimplifyInfo};
use crate::sort_properties::{ExprProperties, SortProperties};
use crate::{
    ColumnarValue, Documentation, Expr, ScalarFunctionImplementation, Signature,
};
use arrow::datatypes::DataType;
use datafusion_common::{not_impl_err, ExprSchema, Result};
use datafusion_expr_common::interval_arithmetic::Interval;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

/// Logical representation of a Scalar User Defined Function.
///
/// A scalar function produces a single row output for each row of input. This
/// struct contains the information DataFusion needs to plan and invoke
/// functions you supply such name, type signature, return type, and actual
/// implementation.
///
/// 1. For simple use cases, use [`create_udf`] (examples in [`simple_udf.rs`]).
///
/// 2. For advanced use cases, use [`ScalarUDFImpl`] which provides full API
///    access (examples in  [`advanced_udf.rs`]).
///
/// See [`Self::call`] to invoke a `ScalarUDF` with arguments.
///
/// # API Note
///
/// This is a separate struct from `ScalarUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [`create_udf`]: crate::expr_fn::create_udf
/// [`simple_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udf.rs
/// [`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
#[derive(Debug, Clone)]
pub struct ScalarUDF {
    inner: Arc<dyn ScalarUDFImpl>,
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.inner.equals(other.inner.as_ref())
    }
}

// Manual implementation based on `ScalarUDFImpl::equals`
impl PartialOrd for ScalarUDF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name().partial_cmp(other.name()) {
            Some(Ordering::Equal) => self.signature().partial_cmp(other.signature()),
            cmp => cmp,
        }
    }
}

impl Eq for ScalarUDF {}

impl Hash for ScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash_value().hash(state)
    }
}

impl ScalarUDF {
    /// Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`ScalarUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> ScalarUDF
    where
        F: ScalarUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`ScalarUDFImpl`] trait object for this function
    pub fn inner(&self) -> &Arc<dyn ScalarUDFImpl> {
        &self.inner
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`ScalarUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedScalarUDFImpl::new(Arc::clone(&self.inner), aliases))
    }

    /// Returns a [`Expr`] logical expression to call this UDF with specified
    /// arguments.
    ///
    /// This utility allows easily calling UDFs
    ///
    /// # Example
    /// ```no_run
    /// use datafusion_expr::{col, lit, ScalarUDF};
    /// # fn my_udf() -> ScalarUDF { unimplemented!() }
    /// let my_func: ScalarUDF = my_udf();
    /// // Create an expr for `my_func(a, 12.3)`
    /// let expr = my_func.call(vec![col("a"), lit(12.3)]);
    /// ```
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(crate::expr::ScalarFunction::new_udf(
            Arc::new(self.clone()),
            args,
        ))
    }

    /// Returns this function's name.
    ///
    /// See [`ScalarUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns this function's display_name.
    ///
    /// See [`ScalarUDFImpl::display_name`] for more details
    pub fn display_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.display_name(args)
    }

    /// Returns this function's schema_name.
    ///
    /// See [`ScalarUDFImpl::schema_name`] for more details
    pub fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.schema_name(args)
    }

    /// Returns the aliases for this function.
    ///
    /// See [`ScalarUDF::with_aliases`] for more details
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    /// Returns this function's [`Signature`] (what input types are accepted).
    ///
    /// See [`ScalarUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// The datatype this function returns given the input argument input types.
    /// This function is used when the input arguments are [`Expr`]s.
    ///
    ///
    /// See [`ScalarUDFImpl::return_type_from_exprs`] for more details.
    pub fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        // If the implementation provides a return_type_from_exprs, use it
        self.inner.return_type_from_exprs(args, schema, arg_types)
    }

    /// Do the function rewrite
    ///
    /// See [`ScalarUDFImpl::simplify`] for more details.
    pub fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    /// Invoke the function on `args`, returning the appropriate result.
    ///
    /// See [`ScalarUDFImpl::invoke`] for more details.
    pub fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        self.inner.invoke(args)
    }

    pub fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.inner.is_nullable(args, schema)
    }

    /// Invoke the function without `args` but number of rows, returning the appropriate result.
    ///
    /// See [`ScalarUDFImpl::invoke_no_args`] for more details.
    pub fn invoke_no_args(&self, number_rows: usize) -> Result<ColumnarValue> {
        self.inner.invoke_no_args(number_rows)
    }

    /// Returns a `ScalarFunctionImplementation` that can invoke the function
    /// during execution
    #[deprecated(since = "42.0.0", note = "Use `invoke` or `invoke_no_args` instead")]
    pub fn fun(&self) -> ScalarFunctionImplementation {
        let captured = Arc::clone(&self.inner);
        Arc::new(move |args| captured.invoke(args))
    }

    /// Get the circuits of inner implementation
    pub fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    /// Computes the output interval for a [`ScalarUDF`], given the input
    /// intervals.
    ///
    /// # Parameters
    ///
    /// * `inputs` are the intervals for the inputs (children) of this function.
    ///
    /// # Example
    ///
    /// If the function is `ABS(a)`, and the input interval is `a: [-3, 2]`,
    /// then the output interval would be `[0, 3]`.
    pub fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(inputs)
    }

    /// Updates bounds for child expressions, given a known interval for this
    /// function. This is used to propagate constraints down through an expression
    /// tree.
    ///
    /// # Parameters
    ///
    /// * `interval` is the currently known interval for this function.
    /// * `inputs` are the current intervals for the inputs (children) of this function.
    ///
    /// # Returns
    ///
    /// A `Vec` of new intervals for the children, in order.
    ///
    /// If constraint propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children intervals change as a result of
    /// propagation, may return an empty vector instead of cloning `children`.
    /// This is the default (and conservative) return value.
    ///
    /// # Example
    ///
    /// If the function is `ABS(a)`, the current `interval` is `[4, 5]` and the
    /// input `a` is given as `[-7, 3]`, then propagation would return `[-5, 3]`.
    pub fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    /// Calculates the [`SortProperties`] of this function based on its
    /// children's properties.
    pub fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    /// See [`ScalarUDFImpl::coerce_types`] for more details.
    pub fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    /// Returns the documentation for this Scalar UDF.
    ///
    /// Documentation can be accessed programmatically as well as
    /// generating publicly facing documentation.
    pub fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
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
/// [`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use std::sync::OnceLock;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Documentation, Signature, Volatility};
/// # use datafusion_expr::{ScalarUDFImpl, ScalarUDF};
/// # use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
///
/// #[derive(Debug)]
/// struct AddOne {
///   signature: Signature,
/// }
///
/// impl AddOne {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
///      }
///   }
/// }
///  
/// static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
///
/// fn get_doc() -> &'static Documentation {
///     DOCUMENTATION.get_or_init(|| {
///         Documentation::builder()
///             .with_doc_section(DOC_SECTION_MATH)
///             .with_description("Add one to an int32")
///             .with_syntax_example("add_one(2)")
///             .with_argument("arg1", "The int32 number to add one to")
///             .build()
///             .unwrap()
///     })
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
///    fn documentation(&self) -> Option<&Documentation> {
///         Some(get_doc())
///     }
/// }
///
/// // Create a new ScalarUDF from the implementation
/// let add_one = ScalarUDF::from(AddOne::new());
///
/// // Call the function `add_one(col)`
/// let expr = add_one.call(vec![col("a")]);
/// ```
pub trait ScalarUDFImpl: Debug + Send + Sync {
    // Note: When adding any methods (with default implementations), remember to add them also
    // into the AliasedScalarUDFImpl below!

    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the user-defined display name of the UDF given the arguments
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        let names: Vec<String> = args.iter().map(ToString::to_string).collect();
        // TODO: join with ", " to standardize the formatting of Vec<Expr>, <https://github.com/apache/datafusion/issues/10364>
        Ok(format!("{}({})", self.name(), names.join(",")))
    }

    /// Returns the name of the column this expression would create
    ///
    /// See [`Expr::schema_name`] for details
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!(
            "{}({})",
            self.name(),
            schema_name_from_exprs_comma_seperated_without_space(args)?
        ))
    }

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments.
    ///
    /// # Notes
    ///
    /// If you provide an implementation for [`Self::return_type_from_exprs`],
    /// DataFusion will not call `return_type` (this function). In this case it
    /// is recommended to return [`DataFusionError::Internal`].
    ///
    /// [`DataFusionError::Internal`]: datafusion_common::DataFusionError::Internal
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// What [`DataType`] will be returned by this function, given the
    /// arguments?
    ///
    /// Note most UDFs should implement [`Self::return_type`] and not this
    /// function. The output type for most functions only depends on the types
    /// of their inputs (e.g. `sqrt(f32)` is always `f32`).
    ///
    /// By default, this function calls [`Self::return_type`] with the
    /// types of each argument.
    ///
    /// This method can be overridden for functions that return different
    /// *types* based on the *values* of their arguments.
    ///
    /// For example, the following two function calls get the same argument
    /// types (something and a `Utf8` string) but return different types based
    /// on the value of the second argument:
    ///
    /// * `arrow_cast(x, 'Int16')` --> `Int16`
    /// * `arrow_cast(x, 'Float32')` --> `Float32`
    ///
    /// # Notes:
    ///
    /// This function must consistently return the same type for the same
    /// logical input even if the input is simplified (e.g. it must return the same
    /// value for `('foo' | 'bar')` as it does for ('foobar').
    fn return_type_from_exprs(
        &self,
        _args: &[Expr],
        _schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        self.return_type(arg_types)
    }

    fn is_nullable(&self, _args: &[Expr], _schema: &dyn ExprSchema) -> bool {
        true
    }

    /// Invoke the function on `args`, returning the appropriate result
    ///
    /// The function will be invoked passed with the slice of [`ColumnarValue`]
    /// (either scalar or array).
    ///
    /// If the function does not take any arguments, please use [invoke_no_args]
    /// instead and return [not_impl_err] for this function.
    ///
    ///
    /// # Performance
    ///
    /// For the best performance, the implementations of `invoke` should handle
    /// the common case when one or more of their arguments are constant values
    /// (aka  [`ColumnarValue::Scalar`]).
    ///
    /// [`ColumnarValue::values_to_arrays`] can be used to convert the arguments
    /// to arrays, which will likely be simpler code, but be slower.
    ///
    /// [invoke_no_args]: ScalarUDFImpl::invoke_no_args
    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue>;

    /// Invoke the function without `args`, instead the number of rows are provided,
    /// returning the appropriate result.
    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        not_impl_err!(
            "Function {} does not implement invoke_no_args but called",
            self.name()
        )
    }

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Aliases can be used to invoke the same function using different names.
    /// For example in some databases `now()` and `current_timestamp()` are
    /// aliases for the same function. This behavior can be obtained by
    /// returning `current_timestamp` as an alias for the `now` function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }

    /// Optionally apply per-UDF simplification / rewrite rules.
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
    /// # Arguments
    /// * `args`: The arguments of the function
    /// * `info`: The necessary information for simplification
    ///
    /// # Returns
    /// [`ExprSimplifyResult`] indicating the result of the simplification NOTE
    /// if the function cannot be simplified, the arguments *MUST* be returned
    /// unmodified
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered
    /// Setting this to true prevents certain optimizations such as common subexpression elimination
    fn short_circuits(&self) -> bool {
        false
    }

    /// Computes the output interval for a [`ScalarUDFImpl`], given the input
    /// intervals.
    ///
    /// # Parameters
    ///
    /// * `children` are the intervals for the children (inputs) of this function.
    ///
    /// # Example
    ///
    /// If the function is `ABS(a)`, and the input interval is `a: [-3, 2]`,
    /// then the output interval would be `[0, 3]`.
    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        // We cannot assume the input datatype is the same of output type.
        Interval::make_unbounded(&DataType::Null)
    }

    /// Updates bounds for child expressions, given a known interval for this
    /// function. This is used to propagate constraints down through an expression
    /// tree.
    ///
    /// # Parameters
    ///
    /// * `interval` is the currently known interval for this function.
    /// * `inputs` are the current intervals for the inputs (children) of this function.
    ///
    /// # Returns
    ///
    /// A `Vec` of new intervals for the children, in order.
    ///
    /// If constraint propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children intervals change as a result of
    /// propagation, may return an empty vector instead of cloning `children`.
    /// This is the default (and conservative) return value.
    ///
    /// # Example
    ///
    /// If the function is `ABS(a)`, the current `interval` is `[4, 5]` and the
    /// input `a` is given as `[-7, 3]`, then propagation would return `[-5, 3]`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(Some(vec![]))
    }

    /// Calculates the [`SortProperties`] of this function based on its
    /// children's properties.
    fn output_ordering(&self, _inputs: &[ExprProperties]) -> Result<SortProperties> {
        Ok(SortProperties::Unordered)
    }

    /// Coerce arguments of a function call to types that the function can evaluate.
    ///
    /// This function is only called if [`ScalarUDFImpl::signature`] returns [`crate::TypeSignature::UserDefined`]. Most
    /// UDFs should return one of the other variants of `TypeSignature` which handle common
    /// cases
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// For example, if your function requires a floating point arguments, but the user calls
    /// it like `my_func(1::int)` (i.e. with `1` as an integer), coerce_types can return `[DataType::Float64]`
    /// to ensure the argument is converted to `1::double`
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

    /// Return true if this scalar UDF is equal to the other.
    ///
    /// Allows customizing the equality of scalar UDFs.
    /// Must be consistent with [`Self::hash_value`] and follow the same rules as [`Eq`]:
    ///
    /// - reflexive: `a.equals(a)`;
    /// - symmetric: `a.equals(b)` implies `b.equals(a)`;
    /// - transitive: `a.equals(b)` and `b.equals(c)` implies `a.equals(c)`.
    ///
    /// By default, compares [`Self::name`] and [`Self::signature`].
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }

    /// Returns a hash value for this scalar UDF.
    ///
    /// Allows customizing the hash code of scalar UDFs. Similarly to [`Hash`] and [`Eq`],
    /// if [`Self::equals`] returns true for two UDFs, their `hash_value`s must be the same.
    ///
    /// By default, hashes [`Self::name`] and [`Self::signature`].
    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.name().hash(hasher);
        self.signature().hash(hasher);
        hasher.finish()
    }

    /// Returns the documentation for this Scalar UDF.
    ///
    /// Documentation can be accessed programmatically as well as
    /// generating publicly facing documentation.
    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

/// ScalarUDF that adds an alias to the underlying function. It is better to
/// implement [`ScalarUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedScalarUDFImpl {
    inner: Arc<dyn ScalarUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedScalarUDFImpl {
    pub fn new(
        inner: Arc<dyn ScalarUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));
        Self { inner, aliases }
    }
}

impl ScalarUDFImpl for AliasedScalarUDFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        self.inner.return_type_from_exprs(args, schema, arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        self.inner.invoke(args)
    }

    fn invoke_no_args(&self, number_rows: usize) -> Result<ColumnarValue> {
        self.inner.invoke_no_args(number_rows)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<AliasedScalarUDFImpl>() {
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

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

// Scalar UDF doc sections for use in public documentation
pub mod scalar_doc_sections {
    use crate::DocSection;

    pub fn doc_sections() -> Vec<DocSection> {
        vec![
            DOC_SECTION_MATH,
            DOC_SECTION_CONDITIONAL,
            DOC_SECTION_STRING,
            DOC_SECTION_BINARY_STRING,
            DOC_SECTION_REGEX,
            DOC_SECTION_DATETIME,
            DOC_SECTION_ARRAY,
            DOC_SECTION_STRUCT,
            DOC_SECTION_MAP,
            DOC_SECTION_HASHING,
            DOC_SECTION_OTHER,
        ]
    }

    pub const DOC_SECTION_MATH: DocSection = DocSection {
        include: true,
        label: "Math Functions",
        description: None,
    };

    pub const DOC_SECTION_CONDITIONAL: DocSection = DocSection {
        include: true,
        label: "Conditional Functions",
        description: None,
    };

    pub const DOC_SECTION_STRING: DocSection = DocSection {
        include: true,
        label: "String Functions",
        description: None,
    };

    pub const DOC_SECTION_BINARY_STRING: DocSection = DocSection {
        include: true,
        label: "Binary String Functions",
        description: None,
    };

    pub const DOC_SECTION_REGEX: DocSection = DocSection {
        include: true,
        label: "Regular Expression Functions",
        description: Some(
            r#"Apache DataFusion uses a [PCRE-like](https://en.wikibooks.org/wiki/Regular_Expressions/Perl-Compatible_Regular_Expressions)
regular expression [syntax](https://docs.rs/regex/latest/regex/#syntax)
(minus support for several features including look-around and backreferences).
The following regular expression functions are supported:"#,
        ),
    };

    pub const DOC_SECTION_DATETIME: DocSection = DocSection {
        include: true,
        label: "Time and Date Functions",
        description: None,
    };

    pub const DOC_SECTION_ARRAY: DocSection = DocSection {
        include: true,
        label: "Array Functions",
        description: None,
    };

    pub const DOC_SECTION_STRUCT: DocSection = DocSection {
        include: true,
        label: "Struct Functions",
        description: None,
    };

    pub const DOC_SECTION_MAP: DocSection = DocSection {
        include: true,
        label: "Map Functions",
        description: None,
    };

    pub const DOC_SECTION_HASHING: DocSection = DocSection {
        include: true,
        label: "Hashing Functions",
        description: None,
    };

    pub const DOC_SECTION_OTHER: DocSection = DocSection {
        include: true,
        label: "Other Functions",
        description: None,
    };
}
