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

//! [`HigherOrderUDF`]: User Defined Higher Order Functions

use crate::expr::schema_name_from_exprs_comma_separated_without_space;
use crate::{ColumnarValue, Documentation, Expr};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr_common::dyn_eq::{DynEq, DynHash};
use datafusion_expr_common::signature::Volatility;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// The types of arguments for which a function has implementations.
///
/// [`HigherOrderTypeSignature`] **DOES NOT** define the types that a user query could call the
/// function with. DataFusion will automatically coerce (cast) argument types to
/// one of the supported function signatures, if possible.
///
/// # Overview
/// Functions typically provide implementations for a small number of different
/// argument [`DataType`]s, rather than all possible combinations. If a user
/// calls a function with arguments that do not match any of the declared types,
/// DataFusion will attempt to automatically coerce (add casts to) function
/// arguments so they match the [`HigherOrderTypeSignature`]. See the [`type_coercion`] module
/// for more details
///
/// [`type_coercion`]: crate::type_coercion
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum HigherOrderTypeSignature {
    /// The acceptable signature and coercions rules are special for this
    /// function.
    ///
    /// If this signature is specified,
    /// DataFusion will call [`HigherOrderUDF::coerce_value_types`] to prepare argument types.
    UserDefined,
    /// One or more lambdas or arguments with arbitrary types
    VariadicAny,
    /// The specified number of lambdas or arguments with arbitrary types.
    Any(usize),
}

/// Provides information necessary for calling a higher order function.
///
/// - [`HigherOrderTypeSignature`] defines the argument types that a function has implementations
///   for.
///
/// - [`Volatility`] defines how the output of the function changes with the input.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct HigherOrderSignature {
    /// The data types that the function accepts. See [HigherOrderTypeSignature] for more information.
    pub type_signature: HigherOrderTypeSignature,
    /// The volatility of the function. See [Volatility] for more information.
    pub volatility: Volatility,
    /// Whether [HigherOrderUDF::coerce_values_for_lambdas] should be called
    pub coerce_values_for_lambdas: bool,
    /// The max number of times to call [HigherOrderUDF::lambda_parameters] before raising an error.
    /// Used to guard against implementations that causes an infinite loop by endlessly returning
    /// [LambdaParametersProgress::Partial]. Defaults to 256
    pub lambda_parameters_max_iterations: usize,
}

const LAMBDA_PARAMETERS_MAX_ITERATIONS: usize = 256;

impl HigherOrderSignature {
    /// Creates a new `HigherOrderSignature` from a given type signature and volatility.
    pub fn new(type_signature: HigherOrderTypeSignature, volatility: Volatility) -> Self {
        HigherOrderSignature {
            type_signature,
            volatility,
            coerce_values_for_lambdas: false,
            lambda_parameters_max_iterations: LAMBDA_PARAMETERS_MAX_ITERATIONS,
        }
    }

    /// User-defined coercion rules for the function.
    pub fn user_defined(volatility: Volatility) -> Self {
        Self {
            type_signature: HigherOrderTypeSignature::UserDefined,
            volatility,
            coerce_values_for_lambdas: false,
            lambda_parameters_max_iterations: LAMBDA_PARAMETERS_MAX_ITERATIONS,
        }
    }

    /// An arbitrary number of lambdas or arguments of any type.
    pub fn variadic_any(volatility: Volatility) -> Self {
        Self {
            type_signature: HigherOrderTypeSignature::VariadicAny,
            volatility,
            coerce_values_for_lambdas: false,
            lambda_parameters_max_iterations: LAMBDA_PARAMETERS_MAX_ITERATIONS,
        }
    }

    /// A specified number of arguments of any type
    pub fn any(arg_count: usize, volatility: Volatility) -> Self {
        Self {
            type_signature: HigherOrderTypeSignature::Any(arg_count),
            volatility,
            coerce_values_for_lambdas: false,
            lambda_parameters_max_iterations: LAMBDA_PARAMETERS_MAX_ITERATIONS,
        }
    }

    /// Set [Self::coerce_values_for_lambdas] to true to indicate that [HigherOrderUDF::coerce_values_for_lambdas]
    /// should be called
    pub fn with_coerce_values_for_lambdas(mut self) -> Self {
        self.coerce_values_for_lambdas = true;

        self
    }
}

impl PartialEq for dyn HigherOrderUDF {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other as _)
    }
}

impl PartialOrd for dyn HigherOrderUDF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut cmp = self.name().cmp(other.name());
        if cmp == Ordering::Equal {
            cmp = self.signature().partial_cmp(other.signature())?;
        }
        if cmp == Ordering::Equal {
            cmp = self.aliases().partial_cmp(other.aliases())?;
        }
        // Contract for PartialOrd and PartialEq consistency requires that
        // a == b if and only if partial_cmp(a, b) == Some(Equal).
        if cmp == Ordering::Equal && self != other {
            // Functions may have other properties besides name and signature
            // that differentiate two instances (e.g. type, or arbitrary parameters).
            // We cannot return Some(Equal) in such case.
            return None;
        }
        debug_assert!(
            cmp == Ordering::Equal || self != other,
            "Detected incorrect implementation of PartialEq when comparing functions: '{}' and '{}'. \
            The functions compare as equal, but they are not equal based on general properties that \
            the PartialOrd implementation observes,",
            self.name(),
            other.name()
        );
        Some(cmp)
    }
}

impl Eq for dyn HigherOrderUDF {}

impl Hash for dyn HigherOrderUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state)
    }
}

/// Arguments passed to [`HigherOrderUDF::invoke_with_args`] when invoking a
/// higher order function.
#[derive(Debug, Clone)]
pub struct HigherOrderFunctionArgs {
    /// The evaluated arguments and lambdas to the function
    pub args: Vec<ValueOrLambda<ColumnarValue, LambdaArgument>>,
    /// Field associated with each arg, if it exists
    /// For lambdas, it will be the field of the result of
    /// the lambda if evaluated with the parameters
    /// returned from [`HigherOrderUDF::lambda_parameters`]
    pub arg_fields: Vec<ValueOrLambda<FieldRef, FieldRef>>,
    /// The number of rows in record batch being evaluated
    pub number_rows: usize,
    /// The return field of the higher order function returned
    /// (from `return_field_from_args`) when creating the
    /// physical expression from the logical expression
    pub return_field: FieldRef,
    /// The config options at execution time
    pub config_options: Arc<ConfigOptions>,
}

impl HigherOrderFunctionArgs {
    /// The return type of the function. See [`Self::return_field`] for more
    /// details.
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }
}

/// A lambda argument to a HigherOrderFunction
#[derive(Clone, Debug)]
pub struct LambdaArgument {
    /// The parameters defined in this lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be `vec![Field::new("v", DataType::Int32, true)]`
    params: Vec<FieldRef>,
    /// The body of the lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be the physical expression of `-v`
    body: Arc<dyn PhysicalExpr>,
}

impl LambdaArgument {
    pub fn new(params: Vec<FieldRef>, body: Arc<dyn PhysicalExpr>) -> Self {
        Self { params, body }
    }

    /// Evaluate this lambda
    /// `args` should evaluate to the value of each parameter
    /// of the correspondent lambda returned in [HigherOrderUDF::lambda_parameters].
    pub fn evaluate(
        &self,
        args: &[&dyn Fn() -> Result<ArrayRef>],
    ) -> Result<ColumnarValue> {
        let columns = args
            .iter()
            .take(self.params.len())
            .map(|arg| arg())
            .collect::<Result<_>>()?;

        let schema = Arc::new(Schema::new(self.params.clone()));

        let batch = RecordBatch::try_new(schema, columns)?;

        self.body.evaluate(&batch)
    }
}

/// Information about arguments passed to the function
///
/// This structure contains metadata about how the function was called
/// such as the type of the arguments, any scalar arguments and if the
/// arguments can (ever) be null
///
/// See [`HigherOrderUDF::return_field_from_args`] for more information
#[derive(Clone, Debug)]
pub struct HigherOrderReturnFieldArgs<'a> {
    /// The data types of the arguments to the function
    ///
    /// If argument `i` to the function is a lambda, it will be the field of the result of the
    /// lambda if evaluated with the parameters returned from [`HigherOrderUDF::lambda_parameters`]
    ///
    /// For example, with `array_transform([1], v -> v == 5)`
    /// this field will be
    /// ```ignore
    /// [
    ///     ValueOrLambda::Value(Field::new("", DataType::new_list(DataType::Int32, true), true)),
    ///     ValueOrLambda::Lambda(Field::new("", DataType::Boolean, true))
    /// ]
    /// ```
    pub arg_fields: &'a [ValueOrLambda<FieldRef, FieldRef>],
    /// Is argument `i` to the function a scalar (constant)?
    ///
    /// If the argument `i` is not a scalar, it will be None
    ///
    /// For example, if a function is called like `array_transform([1], v -> v == 5)`
    /// this field will be `[Some(ScalarValue::List(...), None]`
    pub scalar_arguments: &'a [Option<&'a ScalarValue>],
}

/// An argument to a higher order function
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueOrLambda<V, L> {
    /// A value with associated data
    Value(V),
    /// A lambda with associated data
    Lambda(L),
}

/// Represents a step during the resolution of the parameters of all lambdas of a given
/// higher-order function via [HigherOrderUDF::lambda_parameters]. It's valid that the
/// fields of a given lambda changes between steps, and is up to the implementation to
/// provide during the function evaluation the parameters that matches the fields returned
/// at the [LambdaParametersProgress::Complete] step. See [HigherOrderUDF::lambda_parameters]
/// docs for more details
pub enum LambdaParametersProgress {
    /// The parameters of some lambdas are unknown due to a dependency on another lambda output field
    /// or are placeholders due to a dependency on it's own output field. It's perfectly valid to
    /// contain only `Some`'s and not a single `None`, representing lambdas that depends only on itself
    /// and not on others. [HigherOrderUDF::lambda_parameters] will be called again with the output
    /// field of all lambdas with known parameters.
    Partial(Vec<Option<Vec<FieldRef>>>),
    /// There are no unmet dependencies and all parameters are known, [HigherOrderUDF::lambda_parameters]
    /// will not be called again
    Complete(Vec<Vec<FieldRef>>),
}

/// Trait for implementing user defined higher order functions.
///
/// This trait exposes the full API for implementing user defined functions and
/// can be used to implement any function.
///
/// See [`array_transform.rs`] for a commented complete implementation
///
/// [`array_transform.rs`]: https://github.com/apache/datafusion/blob/main/datafusion/functions-nested/src/array_transform.rs
pub trait HigherOrderUDF: Debug + DynEq + DynHash + Send + Sync + Any {
    /// Returns this function's name
    fn name(&self) -> &str;

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

    /// Returns the name of the column this expression would create
    ///
    /// See [`Expr::schema_name`] for details
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!(
            "{}({})",
            self.name(),
            schema_name_from_exprs_comma_separated_without_space(args)?
        ))
    }

    /// Returns a [`HigherOrderSignature`] describing the argument types for which this
    /// function has an implementation, and the function's [`Volatility`].
    ///
    /// See [`HigherOrderSignature`] for more details on argument type handling
    /// and [`Self::return_field_from_args`] for computing the return type.
    ///
    /// [`Volatility`]: datafusion_expr_common::signature::Volatility
    fn signature(&self) -> &HigherOrderSignature;

    /// Return the field of all the parameters supported by the lambdas in `fields`.
    /// If a lambda support multiple parameters, all should be returned, regardless of
    /// whether they are used or not on a particular invocation
    ///
    /// Tip: If you have a [`HigherOrderFunction`] invocation, you can call the helper
    /// [`HigherOrderFunction::lambda_parameters`] instead of this method directly
    ///
    /// The name of the returned fields are ignored.
    ///
    /// This function is repeatedelly called until [LambdaParametersProgress::Complete] is returned, with
    /// `step` increased by one at each invocation, starting at 0.
    ///
    /// For functions which all lambda parameters depend only on the field of it's value arguments,
    /// this can return [LambdaParametersProgress::Complete] at step 0. Taking as an example a strict
    /// array_reduce with the signature `(arr: [V], initial_value: I, (I, V) -> I, (I) -> O) -> O`, which
    /// requires it's initial value to be the exact same type of it's merge output, which is also the
    /// parameter of it's finish lambda, the expression
    ///
    /// `array_reduce([1.2, 2.1], 0.0, (acc, v) -> acc + v + 1.5, v -> v > 5.1)`
    ///
    ///  would result in this function being called as the following:
    ///
    /// ```ignore
    /// let lambda_parameters = array_reduce.lambda_parameters(
    ///     0,
    ///     &[
    ///         // the Field of the literal `[1.2, 2.1]`, the array being reduced
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::new_list(DataType::Float32, true), true))),
    ///         // the Field of the literal `0.0`, the initial value
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::Float32, true))),
    ///         // the Field of the output of the merge lambda, which is unknown at this point because it depends
    ///         // on the return of this call
    ///         ValueOrLambda::Lambda(None),
    ///         // the Field of the output of the finish lambda, unknown for the same reason as above
    ///         ValueOrLambda::Lambda(None),
    /// ])?;
    ///
    /// assert_eq!(
    ///      lambda_parameters,
    ///      LambdaParametersProgress::Complete(vec![
    ///         // the finish lambda supported parameters, regardless of how many are actually used
    ///         vec![
    ///             // the accumulator which is the field of the initial value
    ///             Arc::new(Field::new("ignored_name", DataType::Float32, true)),
    ///             // the array values being reduced
    ///             Arc::new(Field::new("", DataType::Float32, true)),
    ///         ],
    ///         // the merge lambda supported parameters
    ///         vec![
    ///             // the reduced value which is the field of the initial value
    ///             Arc::new(Field::new("ignored_name", DataType::Float32, true)),
    ///         ],
    ///      ])
    /// );
    /// ```
    ///
    /// For functions which lambda parameters depends on the output of other lambdas, or on their own lambda,
    /// this can return [LambdaParametersProgress::Partial] until all dependencies are met. Note that for
    /// lambda with cyclic dependencies, you likely want to use [HigherOrderUDF::coerce_values_for_lambdas] too.
    /// Take as an example a flexible array_reduce with the signature `(arr: [V], initial_value: I, (ACC, V) -> ACC, (ACC) -> O) -> O`.
    /// It has a cyclic dependency in the merge lambda, and a dependency of the finish lambda in the merge lambda,
    /// and only requires the initial value to be *coercible* to the output of the merge lambda, which is defined by
    /// it's [HigherOrderUDF::coerce_values_for_lambdas] implementation. The expression
    ///
    /// `array_reduce([1.2, 2.1], 0, (acc, v) -> acc + v + 1.5, v -> v > 5.1)`
    ///
    /// would result in this function being called as the following:
    ///
    /// ```ignore
    /// let lambda_parameters = array_reduce.lambda_parameters(
    ///     0,
    ///     &[
    ///         // the Field of the literal `[1.2, 2.1]`, the array being reduced
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::new_list(DataType::Float32, true), true))),
    ///         // the Field of the literal `0`, the initial value
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::Int32, true))),
    ///         // the Field of the output of the merge lambda, which is unknown at this point because it depends on
    ///         // the return this call
    ///         ValueOrLambda::Lambda(None),
    ///         // the Field of the output of the finish lambda, unknown for the same reason as above
    ///         ValueOrLambda::Lambda(None),
    /// ])?;
    ///
    /// assert_eq!(
    ///      lambda_parameters,
    ///      LambdaParametersProgress::Partial(vec![
    ///         // the finish lambda supported parameters, regardless of how many are actually used
    ///         Some(vec![
    ///             // at step 0, use the field of the initial value
    ///             Arc::new(Field::new("ignored_name", DataType::Int32, true)),
    ///             // the array values being reduced
    ///             Arc::new(Field::new("", DataType::Float32, true)),
    ///         ]),
    ///         // the merge lambda supported parameters, unknown at this point due to dependency on the merge output
    ///         None,
    ///      ])
    /// );
    ///
    /// let lambda_parameters = array_reduce.lambda_parameters(
    ///     1,
    ///     &[
    ///         // the Field of the literal `[1.2, 2.1]`, the array being reduced
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::new_list(DataType::Float32, true), true))),
    ///         // the Field of the literal `0`, the initial value
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::Int32, true))),
    ///         // the Field of the output of the merge lambda, which could be inferred to be a Float32 based on the
    ///         // returned values of the previous step
    ///         ValueOrLambda::Value(Arc::new(Field::new("", DataType::Float32, true))),
    ///         // the Field of the output of the finish lambda, which is unknown at this point because it depends
    ///         // on the return of this call
    ///         ValueOrLambda::Lambda(None),
    /// ])?;
    ///
    /// assert_eq!(
    ///      lambda_parameters,
    ///      LambdaParametersProgress::Complete(vec![
    ///         // the finish lambda supported parameters, regardless of how many are actually used
    ///         vec![
    ///             // the finish lambda own output now used as it's accumulator
    ///             Arc::new(Field::new("ignored_name", DataType::Float32, true)),
    ///             // the array values being reduced
    ///             Arc::new(Field::new("", DataType::Float32, true)),
    ///         ],
    ///         // the merge lambda supported parameters, which is the output of the merge lambda,
    ///         vec![
    ///             // the output of the merge lambda
    ///             Arc::new(Field::new("", DataType::Float32, true)),
    ///         ],
    ///      ])
    /// );
    ///
    /// let coerce_to = array_reduce.coerce_values_for_lambdas(&[
    ///     // the literal `[1.2, 2.1]` data type, the array being reduced
    ///     ValueOrLambda::Value(DataType::new_list(DataType::Float32, true)),
    ///     // the literal `0` data type, the initial value
    ///     ValueOrLambda::Value(DataType::Int32),
    ///     // the output data type of the merge lambda
    ///     ValueOrLambda::Lambda(DataType::Float32),
    ///     // the output data type of the finish lambda
    ///     ValueOrLambda::Lambda(DataType::Boolean),
    /// ])?;
    ///
    /// assert_eq!(
    ///     coerce_to,
    ///     vec![
    ///         // return the same type for the array being reduced
    ///         DataType::new_list(DataType::Float32, true),
    ///         // coerce the initial value to the output of the merge lambda
    ///         DataType::Float32,
    ///     ]
    /// );
    ///
    /// ```
    ///
    /// Note this may also be called at step 0 with all lambda outputs already set, and in that case,
    /// [LambdaParametersProgress::Complete] must be returned
    ///
    /// The implementation can assume that some other part of the code has coerced
    /// the actual argument types to match [`Self::signature`], except the coercion defined by
    /// [Self::coerce_values_for_lambdas], if applicable.
    ///
    /// [`HigherOrderFunction`]: crate::expr::HigherOrderFunction
    /// [`HigherOrderFunction::lambda_parameters`]: crate::expr::HigherOrderFunction::lambda_parameters
    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress>;

    /// Coerce value arguments of a function call to types that the function can evaluate also taking into
    /// account the *output type of it's lambdas*. This differs from [HigherOrderUDF::coerce_value_types]
    /// that only has access to the type of it's value arguments because it's called before the output type
    /// of lambdas are known. So that this method is called, the function must have it's
    /// [HigherOrderSignature::coerce_values_for_lambdas] set to true
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// # Parameters
    /// * `fields`: The argument types of the value arguments of this function, or the output type of lambdas
    ///
    /// # Return value
    /// A Vec with the same number of [ValueOrLambda::Value] in `fields`. DataFusion will `CAST` the
    /// function call arguments to these specific types.
    ///
    /// For example, a flexible array_reduce implementation (see [Self::lambda_parameters] docs), when working
    /// with the expression below, may want to coerce it's initial value argument, the *integer* `0`,
    /// to match the output it's merge function, which is a *float*:
    ///
    /// `array_reduce([1.2, 2.1], 0, (acc, v) -> acc + v + 1.5, v -> v > 2.0)`
    fn coerce_values_for_lambdas(
        &self,
        _fields: &[ValueOrLambda<DataType, DataType>],
    ) -> Result<Vec<DataType>> {
        not_impl_err!(
            "{} coerce_values_for_lambdas is not implemented",
            self.name()
        )
    }

    /// What type will be returned by this function, given the arguments?
    ///
    /// The implementation can assume that some other part of the code has coerced
    /// the actual argument types to match [`Self::signature`], including the coercion
    /// defined by [Self::coerce_values_for_lambdas], if applicable.
    ///
    /// # Example creating `Field`
    ///
    /// Note the name of the `Field` is ignored, except for structured types such as
    /// `DataType::Struct`.
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field, FieldRef};
    /// # use datafusion_common::Result;
    /// # use datafusion_expr::HigherOrderReturnFieldArgs;
    /// # struct Example{}
    /// # impl Example {
    /// fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
    ///     let field = Arc::new(Field::new("ignored_name", DataType::Int32, true));
    ///     Ok(field)
    /// }
    /// # }
    /// ```
    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<FieldRef>;

    /// Whether List or LargeList arguments should have it's non-empty null
    /// sublists cleaned with [remove_list_null_values] before invoking this function
    ///
    /// The default implementation always returns true and should only be implemented
    /// if you want to handle non-empty null sublists yourself
    ///
    /// [remove_list_null_values]: datafusion_common::utils::remove_list_null_values
    // todo: extend this to listview and maps when remove_list_null_values supports it
    fn clear_null_values(&self) -> bool {
        true
    }

    /// Invoke the function returning the appropriate result.
    ///
    /// # Performance
    ///
    /// For the best performance, the implementations should handle the common case
    /// when one or more of their arguments are constant values (aka
    /// [`ColumnarValue::Scalar`]).
    ///
    /// [`ColumnarValue::values_to_arrays`] can be used to convert the arguments
    /// to arrays, which will likely be simpler code, but be slower.
    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue>;

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered.
    ///
    /// Setting this to true prevents certain optimizations such as common
    /// subexpression elimination
    ///
    /// When overriding this function to return `true`, [HigherOrderUDF::conditional_arguments] can also be
    /// overridden to report more accurately which arguments are eagerly evaluated and which ones
    /// lazily.
    fn short_circuits(&self) -> bool {
        false
    }

    /// Determines which of the arguments passed to *this higher-order function*
    /// are evaluated eagerly and which may be evaluated lazily. Note that this
    /// does *not* applies to the arguments that *lambda functions* pass to it's
    /// body expression
    ///
    /// If this function returns `None`, all arguments are eagerly evaluated.
    /// Returning `None` is a micro optimization that saves a needless `Vec`
    /// allocation.
    ///
    /// If the function returns `Some`, returns (`eager`, `lazy`) where `eager`
    /// are the arguments that are always evaluated, and `lazy` are the
    /// arguments that may be evaluated lazily (i.e. may not be evaluated at all
    /// in some cases).
    ///
    /// Implementations must ensure that the two returned `Vec`s are disjunct,
    /// and that each argument from `args` is present in one the two `Vec`s.
    ///
    /// When overriding this function, [HigherOrderUDF::short_circuits] must
    /// be overridden to return `true`.
    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        if self.short_circuits() {
            Some((vec![], args.iter().collect()))
        } else {
            None
        }
    }

    /// Coerce value arguments of a function call to types that the function can evaluate.
    /// Note that if you need to coerce values based on the output type of lambdas, you
    /// must use [HigherOrderUDF::coerce_values_for_lambdas], as this function is used before
    /// the output type of lambdas are known
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// For example, if your function requires a contiguous list argument, but the user calls
    /// it like `my_func(c, v -> v+2)` (i.e. with `c` as a ListView), coerce_types can return `[DataType::List(..)]`
    /// to ensure the argument is converted to a List
    ///
    /// # Parameters
    /// * `arg_types`: The argument types of the value arguments of this function, excluding lambdas
    ///
    /// # Return value
    /// A Vec the same length as `arg_types`. DataFusion will `CAST` the function call
    /// arguments to these specific types.
    fn coerce_value_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        not_impl_err!(
            "Function {} does not implement coerce_value_types",
            self.name()
        )
    }

    /// Returns the documentation for this HigherOrderUDF.
    ///
    /// Documentation can be accessed programmatically as well as generating
    /// publicly facing documentation.
    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr_common::signature::Volatility;

    use super::*;
    use std::hash::DefaultHasher;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestHigherOrderUDF {
        name: &'static str,
        field: &'static str,
        signature: HigherOrderSignature,
    }
    impl HigherOrderUDF for TestHigherOrderUDF {
        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &HigherOrderSignature {
            &self.signature
        }

        fn lambda_parameters(
            &self,
            _step: usize,
            _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
        ) -> Result<LambdaParametersProgress> {
            unimplemented!()
        }

        fn return_field_from_args(
            &self,
            _args: HigherOrderReturnFieldArgs,
        ) -> Result<FieldRef> {
            unimplemented!()
        }

        fn invoke_with_args(
            &self,
            _args: HigherOrderFunctionArgs,
        ) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    // PartialEq and Hash must be consistent, and also PartialEq and PartialOrd
    // must be consistent, so they are tested together.
    #[test]
    fn test_partial_eq_hash_and_partial_ord() {
        // A parameterized function
        let f = test_func("foo", "a");

        // Same like `f`, different instance
        let f2 = test_func("foo", "a");
        assert_eq!(&f, &f2);
        assert_eq!(hash(&f), hash(&f2));
        assert_eq!(f.partial_cmp(&f2), Some(Ordering::Equal));

        // Different parameter
        let b = test_func("foo", "b");
        assert_ne!(&f, &b);
        assert_ne!(hash(&f), hash(&b)); // hash can collide for different values but does not collide in this test
        assert_eq!(f.partial_cmp(&b), None);

        // Different name
        let o = test_func("other", "a");
        assert_ne!(&f, &o);
        assert_ne!(hash(&f), hash(&o)); // hash can collide for different values but does not collide in this test
        assert_eq!(f.partial_cmp(&o), Some(Ordering::Less));

        // Different name and parameter
        assert_ne!(&b, &o);
        assert_ne!(hash(&b), hash(&o)); // hash can collide for different values but does not collide in this test
        assert_eq!(b.partial_cmp(&o), Some(Ordering::Less));
    }

    fn test_func(name: &'static str, parameter: &'static str) -> Arc<dyn HigherOrderUDF> {
        Arc::new(TestHigherOrderUDF {
            name,
            field: parameter,
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
        })
    }

    fn hash<T: Hash>(value: &T) -> u64 {
        let hasher = &mut DefaultHasher::new();
        value.hash(hasher);
        hasher.finish()
    }
}
