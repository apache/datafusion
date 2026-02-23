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

//! [`LambdaUDF`]: Lambda User Defined Functions

use crate::expr::schema_name_from_exprs_comma_separated_without_space;
use crate::simplify::{ExprSimplifyResult, SimplifyInfo};
use crate::sort_properties::{ExprProperties, SortProperties};
use crate::{ColumnarValue, Documentation, Expr};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr_common::dyn_eq::{DynEq, DynHash};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::signature::Signature;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

impl PartialEq for dyn LambdaUDF {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other.as_any())
    }
}

impl PartialOrd for dyn LambdaUDF {
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
            self.name(), other.name()
        );
        Some(cmp)
    }
}

impl Eq for dyn LambdaUDF {}

impl Hash for dyn LambdaUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state)
    }
}

#[derive(Clone, Debug)]
pub enum ValueOrLambdaParameter {
    /// A columnar value with the given field
    Value(FieldRef),
    /// A lambda
    Lambda,
}

/// Arguments passed to [`LambdaUDF::invoke_with_args`] when invoking a
/// lambda function.
#[derive(Debug, Clone)]
pub struct LambdaFunctionArgs {
    /// The evaluated arguments to the function
    pub args: Vec<ValueOrLambda>,
    /// Field associated with each arg, if it exists
    pub arg_fields: Vec<ValueOrLambdaField>,
    /// The number of rows in record batch being evaluated
    pub number_rows: usize,
    /// The return field of the lambda function returned (from `return_type`
    /// or `return_field_from_args`) when creating the physical expression
    /// from the logical expression
    pub return_field: FieldRef,
    /// The config options at execution time
    pub config_options: Arc<ConfigOptions>,
}

/// A lambda argument to a LambdaFunction
#[derive(Clone, Debug)]
pub struct LambdaFunctionLambdaArg {
    /// The parameters defined in this lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be `vec![Field::new("v", DataType::Int32, true)]`
    pub params: Vec<FieldRef>,
    /// The body of the lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be the physical expression of `-v`
    pub body: Arc<dyn PhysicalExpr>,
    /// A RecordBatch containing at least the captured columns inside this lambda body, if any
    /// Note that it may contain additional, non-specified columns, but that's implementation detail
    ///
    /// For example, for `array_transform([2], v -> v + a + b)`,
    /// this will be a `RecordBatch` with two columns, `a` and `b`
    pub captures: Option<RecordBatch>,
}

impl LambdaFunctionArgs {
    /// The return type of the function. See [`Self::return_field`] for more
    /// details.
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }
}

// An argument to a LambdaUDF that supports lambdas
#[derive(Clone, Debug)]
pub enum ValueOrLambda {
    Value(ColumnarValue),
    Lambda(LambdaFunctionLambdaArg),
}

/// Information about arguments passed to the function
///
/// This structure contains metadata about how the function was called
/// such as the type of the arguments, any scalar arguments and if the
/// arguments can (ever) be null
///
/// See [`LambdaUDF::return_field_from_args`] for more information
#[derive(Clone, Debug)]
pub struct LambdaReturnFieldArgs<'a> {
    /// The data types of the arguments to the function
    ///
    /// If argument `i` to the function is a lambda, it will be the field returned by the
    /// lambda when executed with the arguments returned from `LambdaUDF::lambdas_parameters`
    ///
    /// For example, with `array_transform([1], v -> v == 5)`
    /// this field will be `[Field::new("", DataType::List(DataType::Int32), false), Field::new("", DataType::Boolean, false)]`
    pub arg_fields: &'a [ValueOrLambdaField],
    /// Is argument `i` to the function a scalar (constant)?
    ///
    /// If the argument `i` is not a scalar, it will be None
    ///
    /// For example, if a function is called like `my_function(column_a, 5)`
    /// this field will be `[None, Some(ScalarValue::Int32(Some(5)))]`
    pub scalar_arguments: &'a [Option<&'a ScalarValue>],
}

/// A tagged Field indicating whether it correspond to a value or a lambda argument
#[derive(Clone, Debug)]
pub enum ValueOrLambdaField {
    /// The Field of a ColumnarValue argument
    Value(FieldRef),
    /// The Field of the return of the lambda body when evaluated with the parameters from LambdaUDF::lambda_parameters
    Lambda(FieldRef),
}

/// Trait for implementing user defined lambda functions.
///
/// This trait exposes the full API for implementing user defined functions and
/// can be used to implement any function.
///
/// See [`advanced_udf.rs`] for a full example with complete implementation and
/// [`LambdaUDF`] for other available options.
///
/// [`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
///
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use std::sync::LazyLock;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Documentation, LambdaFunctionArgs, Signature, Volatility};
/// # use datafusion_expr::LambdaUDF;
/// # use datafusion_expr::lambda_doc_sections::DOC_SECTION_MATH;
/// /// This struct for a simple UDF that adds one to an int32
/// #[derive(Debug, PartialEq, Eq, Hash)]
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
/// static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
///         Documentation::builder(DOC_SECTION_MATH, "Add one to an int32", "add_one(2)")
///             .with_argument("arg1", "The int32 number to add one to")
///             .build()
///     });
///
/// fn get_doc() -> &'static Documentation {
///     &DOCUMENTATION
/// }
///
/// /// Implement the LambdaUDF trait for AddOne
/// impl LambdaUDF for AddOne {
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
///    fn invoke_with_args(&self, args: LambdaFunctionArgs) -> Result<ColumnarValue> {
///         unimplemented!()
///    }
///    fn documentation(&self) -> Option<&Documentation> {
///         Some(get_doc())
///     }
/// }
///
/// // Create a new LambdaUDF from the implementation
/// let add_one = LambdaUDF::from(AddOne::new());
///
/// // Call the function `add_one(col)`
/// let expr = add_one.call(vec![col("a")]);
/// ```
pub trait LambdaUDF: Debug + DynEq + DynHash + Send + Sync {
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

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

    /// Returns a [`Signature`] describing the argument types for which this
    /// function has an implementation, and the function's [`Volatility`].
    ///
    /// See [`Signature`] for more details on argument type handling
    /// and [`Self::return_type`] for computing the return type.
    ///
    /// [`Volatility`]: datafusion_expr_common::signature::Volatility
    fn signature(&self) -> &Signature;

    /// Create a new instance of this function with updated configuration.
    ///
    /// This method is called when configuration options change at runtime
    /// (e.g., via `SET` statements) to allow functions that depend on
    /// configuration to update themselves accordingly.
    ///
    /// Note the current [`ConfigOptions`] are also passed to [`Self::invoke_with_args`] so
    /// this API is not needed for functions where the values may
    /// depend on the current options.
    ///
    /// This API is useful for functions where the return
    /// **type** depends on the configuration options, such as the `now()` function
    /// which depends on the current timezone.
    ///
    /// # Arguments
    ///
    /// * `config` - The updated configuration options
    ///
    /// # Returns
    ///
    /// * `Some(LambdaUDF)` - A new instance of this function configured with the new settings
    /// * `None` - If this function does not change with new configuration settings (the default)
    fn with_updated_config(&self, _config: &ConfigOptions) -> Option<Arc< dyn LambdaUDF>> {
        None
    }

    /// What type will be returned by this function, given the arguments?
    ///
    /// By default, this function calls [`Self::return_type`] with the
    /// types of each argument.
    ///
    /// # Notes
    ///
    /// For the majority of UDFs, implementing [`Self::return_type`] is sufficient,
    /// as the result type is typically a deterministic function of the input types
    /// (e.g., `sqrt(f32)` consistently yields `f32`). Implementing this method directly
    /// is generally unnecessary unless the return type depends on runtime values.
    ///
    /// This function can be used for more advanced cases such as:
    ///
    /// 1. specifying nullability
    /// 2. return types based on the **values** of the arguments (rather than
    ///    their **types**.
    ///
    /// # Example creating `Field`
    ///
    /// Note the name of the [`Field`] is ignored, except for structured types such as
    /// `DataType::Struct`.
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field, FieldRef};
    /// # use datafusion_common::Result;
    /// # use datafusion_expr::LambdaReturnFieldArgs;
    /// # struct Example{}
    /// # impl Example {
    /// fn return_field_from_args(&self, args: LambdaReturnFieldArgs) -> Result<FieldRef> {
    ///     // report output is only nullable if any one of the arguments are nullable
    ///     let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
    ///     let field = Arc::new(Field::new("ignored_name", DataType::Int32, true));
    ///     Ok(field)
    /// }
    /// # }
    /// ```
    ///
    /// # Output Type based on Values
    ///
    /// For example, the following two function calls get the same argument
    /// types (something and a `Utf8` string) but return different types based
    /// on the value of the second argument:
    ///
    /// * `arrow_cast(x, 'Int16')` --> `Int16`
    /// * `arrow_cast(x, 'Float32')` --> `Float32`
    ///
    /// # Requirements
    ///
    /// This function **must** consistently return the same type for the same
    /// logical input even if the input is simplified (e.g. it must return the same
    /// value for `('foo' | 'bar')` as it does for ('foobar').
    fn return_field_from_args(&self, args: LambdaReturnFieldArgs) -> Result<FieldRef>;

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
    fn invoke_with_args(&self, args: LambdaFunctionArgs) -> Result<ColumnarValue>;

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
    ///
    /// # Notes
    ///
    /// The returned expression must have the same schema as the original
    /// expression, including both the data type and nullability. For example,
    /// if the original expression is nullable, the returned expression must
    /// also be nullable, otherwise it may lead to schema verification errors
    /// later in query planning.
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered.
    ///
    /// Setting this to true prevents certain optimizations such as common
    /// subexpression elimination
    ///
    /// When overriding this function to return `true`, [LambdaUDF::conditional_arguments] can also be
    /// overridden to report more accurately which arguments are eagerly evaluated and which ones
    /// lazily.
    fn short_circuits(&self) -> bool {
        false
    }

    /// Determines which of the arguments passed to this function are evaluated eagerly
    /// and which may be evaluated lazily.
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
    /// When overriding this function, [LambdaUDF::short_circuits] must
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

    /// Computes the output [`Interval`] for a [`LambdaUDF`], given the input
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

    /// Updates bounds for child expressions, given a known [`Interval`]s for this
    /// function.
    ///
    /// This function is used to propagate constraints down through an
    /// expression tree.
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

    /// Calculates the [`SortProperties`] of this function based on its children's properties.
    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        if !self.preserves_lex_ordering(inputs)? {
            return Ok(SortProperties::Unordered);
        }

        let Some(first_order) = inputs.first().map(|p| &p.sort_properties) else {
            return Ok(SortProperties::Singleton);
        };

        if inputs
            .iter()
            .skip(1)
            .all(|input| &input.sort_properties == first_order)
        {
            Ok(*first_order)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    /// Returns true if the function preserves lexicographical ordering based on
    /// the input ordering.
    ///
    /// For example, `concat(a || b)` preserves lexicographical ordering, but `abs(a)` does not.
    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(false)
    }

    /// Coerce arguments of a function call to types that the function can evaluate.
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// [`TypeSignature`]: crate::TypeSignature
    ///
    /// For example, if your function requires a floating point arguments, but the user calls
    /// it like `my_func(1::int)` (i.e. with `1` as an integer), coerce_types can return `[DataType::Float64]`
    /// to ensure the argument is converted to `1::double`
    ///
    /// # Parameters
    /// * `arg_types`: The argument types of the arguments this function with
    ///
    /// # Return value
    /// A Vec the same length as `arg_types`. DataFusion will `CAST` the function call
    /// arguments to these specific types.
    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        not_impl_err!("Function {} does not implement coerce_types", self.name())
    }

    /// Returns the documentation for this Lambda UDF.
    ///
    /// Documentation can be accessed programmatically as well as generating
    /// publicly facing documentation.
    fn documentation(&self) -> Option<&Documentation> {
        None
    }

    /// Returns the parameters that any lambda supports
    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambdaParameter],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        Ok(vec![None; args.len()])
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr_common::signature::Volatility;

    use super::*;
    use std::hash::DefaultHasher;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestLambdaUDF {
        name: &'static str,
        field: &'static str,
        signature: Signature,
    }
    impl LambdaUDF for TestLambdaUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_field_from_args(&self, _args: LambdaReturnFieldArgs) -> Result<FieldRef> {
            unimplemented!()
        }

        fn invoke_with_args(&self, _args: LambdaFunctionArgs) -> Result<ColumnarValue> {
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

    fn test_func(name: &'static str, parameter: &'static str) -> Arc<dyn LambdaUDF> {
        Arc::new(TestLambdaUDF {
            name,
            field: parameter,
            signature: Signature::any(1, Volatility::Immutable),
        })
    }

    fn hash<T: Hash>(value: &T) -> u64 {
        let hasher = &mut DefaultHasher::new();
        value.hash(hasher);
        hasher.finish()
    }
}
