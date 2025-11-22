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

use crate::async_udf::AsyncScalarUDF;
use crate::expr::{schema_name_from_exprs_comma_separated_without_space, Lambda};
use crate::simplify::{ExprSimplifyResult, SimplifyInfo};
use crate::sort_properties::{ExprProperties, SortProperties};
use crate::udf_eq::UdfEq;
use crate::{ColumnarValue, Documentation, Expr, ExprSchemable, Signature};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    exec_err, not_impl_err, DFSchema, ExprSchema, Result, ScalarValue,
};
use datafusion_expr_common::dyn_eq::{DynEq, DynHash};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use indexmap::IndexMap;
use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

/// Logical representation of a Scalar User Defined Function.
///
/// A scalar function produces a single row output for each row of input. This
/// struct contains the information DataFusion needs to plan and invoke
/// functions you supply such as name, type signature, return type, and actual
/// implementation.
///
/// 1. For simple use cases, use [`create_udf`] (examples in [`simple_udf.rs`]).
///
/// 2. For advanced use cases, use [`ScalarUDFImpl`] which provides full API
///    access (examples in  [`advanced_udf.rs`]).
///
/// See [`Self::call`] to create an `Expr` which invokes a `ScalarUDF` with arguments.
///
/// # API Note
///
/// This is a separate struct from [`ScalarUDFImpl`] to maintain backwards
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
        self.inner.dyn_eq(other.inner.as_any())
    }
}

impl PartialOrd for ScalarUDF {
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

impl Eq for ScalarUDF {}

impl Hash for ScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state)
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
        Self::new_from_shared_impl(Arc::new(fun))
    }

    /// Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
    pub fn new_from_shared_impl(fun: Arc<dyn ScalarUDFImpl>) -> ScalarUDF {
        Self { inner: fun }
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
    #[deprecated(
        since = "50.0.0",
        note = "This method is unused and will be removed in a future release"
    )]
    pub fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[expect(deprecated)]
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

    /// The datatype this function returns given the input argument types.
    /// This function is used when the input arguments are [`DataType`]s.
    ///
    ///  # Notes
    ///
    /// If a function implement [`ScalarUDFImpl::return_field_from_args`],
    /// its [`ScalarUDFImpl::return_type`] should raise an error.
    ///
    /// See [`ScalarUDFImpl::return_type`] for more details.
    pub fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    /// Return the datatype this function returns given the input argument types.
    ///
    /// See [`ScalarUDFImpl::return_field_from_args`] for more details.
    pub fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
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

    #[deprecated(since = "50.0.0", note = "Use `return_field_from_args` instead.")]
    pub fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[allow(deprecated)]
        self.inner.is_nullable(args, schema)
    }

    /// Invoke the function on `args`, returning the appropriate result.
    ///
    /// See [`ScalarUDFImpl::invoke_with_args`] for details.
    pub fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        #[cfg(debug_assertions)]
        let return_field = Arc::clone(&args.return_field);
        let result = self.inner.invoke_with_args(args)?;
        // Maybe this could be enabled always?
        // This doesn't use debug_assert!, but it's meant to run anywhere except on production. It's same in spirit, thus conditioning on debug_assertions.
        #[cfg(debug_assertions)]
        {
            if &result.data_type() != return_field.data_type() {
                return datafusion_common::internal_err!("Function '{}' returned value of type '{:?}' while the following type was promised at planning time and expected: '{:?}'",
                        self.name(),
                        result.data_type(),
                        return_field.data_type()
                    );
            }
            // TODO verify return data is non-null when it was promised to be?
        }
        Ok(result)
    }

    /// Determines which of the arguments passed to this function are evaluated eagerly
    /// and which may be evaluated lazily.
    ///
    /// See [ScalarUDFImpl::conditional_arguments] for more information.
    pub fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.conditional_arguments(args)
    }

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered.
    ///
    /// See [ScalarUDFImpl::short_circuits] for more information.
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

    pub fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.preserves_lex_ordering(inputs)
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

    /// Return true if this function is an async function
    pub fn as_async(&self) -> Option<&AsyncScalarUDF> {
        self.inner().as_any().downcast_ref::<AsyncScalarUDF>()
    }

    /// Variation of arguments_from_logical_args that works with arrow Schema's and ScalarFunctionArgMetadata instead
    pub(crate) fn arguments_expr_schema<'a>(
        &self,
        args: &[Expr],
        schema: &'a dyn ExprSchema,
    ) -> Result<Vec<impl ExprSchema + 'a>> {
        self.arguments_scope_with(
            &lambda_parameters(args, schema)?,
            ExtendableExprSchema::new(schema),
        )
    }

    /// Variation of arguments_from_logical_args that works with arrow Schema's and ScalarFunctionArgMetadata instead,
    pub fn arguments_arrow_schema<'a>(
        &self,
        args: &[ValueOrLambdaParameter],
        schema: &'a Schema,
    ) -> Result<Vec<Cow<'a, Schema>>> {
        self.arguments_scope_with(args, Cow::Borrowed(schema))
    }

    pub fn arguments_schema_from_logical_args<'a>(
        &self,
        args: &[Expr],
        schema: &'a DFSchema,
    ) -> Result<Vec<Cow<'a, DFSchema>>> {
        self.arguments_scope_with(
            &lambda_parameters(args, schema)?,
            Cow::Borrowed(schema),
        )
    }

    /// Scalar function supports lambdas as arguments, which will be evaluated with
    /// a different schema that of the function itself. This functions returns a vec
    /// with the correspoding schema that each argument will run
    ///
    /// Return a vec with a value for each argument in args that, if it's a value, it's a clone of base_scope,
    /// if it's a lambda, it's the return of merge called with the index and the fields from lambdas_parameters
    /// updated with names from metadata
    fn arguments_scope_with<T: ExtendSchema + Clone>(
        &self,
        args: &[ValueOrLambdaParameter],
        schema: T,
    ) -> Result<Vec<T>> {
        let parameters = self.inner().lambdas_parameters(args)?;

        if parameters.len() != args.len() {
            return exec_err!(
                "lambdas_schemas: {} lambdas_parameters returned {} values instead of {}",
                self.name(),
                args.len(),
                parameters.len()
            );
        }

        std::iter::zip(args, parameters)
            .enumerate()
            .map(|(i, (arg, parameters))| match (arg, parameters) {
                (ValueOrLambdaParameter::Value(_), None) => Ok(schema.clone()),
                (ValueOrLambdaParameter::Value(_), Some(_)) => exec_err!("lambdas_schemas: {} argument {} (0-indexed) is a value but lambdas_parameters result treat it as a lambda", self.name(), i),
                (ValueOrLambdaParameter::Lambda(_, _), None) => exec_err!("lambdas_schemas: {} argument {} (0-indexed) is a lambda but lambdas_parameters result treat it as a value", self.name(), i),
                (ValueOrLambdaParameter::Lambda(names, captures), Some(args)) => {
                    if names.len() > args.len() {
                        return exec_err!("lambdas_schemas: {} argument {} (0-indexed), a lambda, supports up to {} arguments, but got {}", self.name(), i, args.len(), names.len())
                    }

                    let fields = std::iter::zip(*names, args)
                        .map(|(name, arg)| arg.with_name(name))
                        .collect::<Fields>();

                    if *captures {
                        schema.extend(fields)
                    } else {
                        T::from_fields(fields)
                    }
                }
            })
            .collect()
    }
}

pub trait ExtendSchema: Sized {
    fn from_fields(params: Fields) -> Result<Self>;
    fn extend(&self, params: Fields) -> Result<Self>;
}

impl ExtendSchema for DFSchema {
    fn from_fields(params: Fields) -> Result<Self> {
        DFSchema::from_unqualified_fields(params, Default::default())
    }

    fn extend(&self, params: Fields) -> Result<Self> {
        let qualified_fields = self
            .iter()
            .map(|(qualifier, field)| {
                if params.find(field.name().as_str()).is_none() {
                    return (qualifier.cloned(), Arc::clone(field));
                }

                let alias_gen = AliasGenerator::new();

                loop {
                    let alias = alias_gen.next(field.name().as_str());

                    if params.find(&alias).is_none()
                        && !self.has_column_with_unqualified_name(&alias)
                    {
                        return (
                            qualifier.cloned(),
                            Arc::new(Field::new(
                                alias,
                                field.data_type().clone(),
                                field.is_nullable(),
                            )),
                        );
                    }
                }
            })
            .collect();

        let mut schema = DFSchema::new_with_metadata(qualified_fields, HashMap::new())?;
        let fields_schema = DFSchema::from_unqualified_fields(params, HashMap::new())?;

        schema.merge(&fields_schema);

        assert_eq!(
            schema.fields().len(),
            self.fields().len() + fields_schema.fields().len()
        );

        Ok(schema)
    }
}

impl ExtendSchema for Schema {
    fn from_fields(params: Fields) -> Result<Self> {
        Ok(Schema::new(params))
    }

    fn extend(&self, params: Fields) -> Result<Self> {
        let mut params2 = params.iter()
            .map(|f| (f.name().as_str(), Some(Arc::clone(f))))
            .collect::<IndexMap<_, _>>();

        let mut fields = self.fields()
            .iter()
            .map(|field| {
                match params2.get_mut(field.name().as_str()).and_then(|p| p.take()) {
                    Some(param) => param,
                    None => Arc::clone(field),
                }
            })
            .collect::<Vec<_>>();

        fields.extend(params2.into_values().flatten());

        let fields = self
            .fields()
            .iter()
            .map(|field| {
                if params.find(field.name().as_str()).is_none() {
                    return Arc::clone(field);
                }

                let alias_gen = AliasGenerator::new();

                loop {
                    let alias = alias_gen.next(field.name().as_str());

                    if params.find(&alias).is_none()
                        && self.column_with_name(&alias).is_none()
                    {
                        return Arc::new(Field::new(
                            alias,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ));
                    }
                }
            })
            .chain(params.iter().cloned())
            .collect::<Fields>();

        assert_eq!(fields.len(), self.fields().len() + params.len());

        Ok(Schema::new_with_metadata(fields, self.metadata.clone()))
    }
}

impl<T: ExtendSchema + Clone> ExtendSchema for Cow<'_, T> {
    fn from_fields(params: Fields) -> Result<Self> {
        Ok(Cow::Owned(T::from_fields(params)?))
    }

    fn extend(&self, params: Fields) -> Result<Self> {
        Ok(Cow::Owned(self.as_ref().extend(params)?))
    }
}

impl<T: ExtendSchema> ExtendSchema for Arc<T> {
    fn from_fields(params: Fields) -> Result<Self> {
        Ok(Arc::new(T::from_fields(params)?))
    }

    fn extend(&self, params: Fields) -> Result<Self> {
        Ok(Arc::new(self.as_ref().extend(params)?))
    }
}

impl ExtendSchema for ExtendableExprSchema<'_> {
    fn from_fields(params: Fields) -> Result<Self> {
        static EMPTY_DFSCHEMA: LazyLock<DFSchema> = LazyLock::new(DFSchema::empty);

        Ok(ExtendableExprSchema {
            fields_chain: vec![params],
            outer_schema: &*EMPTY_DFSCHEMA,
        })
    }

    fn extend(&self, params: Fields) -> Result<Self> {
        Ok(ExtendableExprSchema {
            fields_chain: std::iter::once(params)
                .chain(self.fields_chain.iter().cloned())
                .collect(),
            outer_schema: self.outer_schema,
        })
    }
}

/// A `&dyn ExprSchema` wrapper that supports adding the parameters of a lambda
#[derive(Clone, Debug)]
struct ExtendableExprSchema<'a> {
    fields_chain: Vec<Fields>,
    outer_schema: &'a dyn ExprSchema,
}

impl<'a> ExtendableExprSchema<'a> {
    fn new(schema: &'a dyn ExprSchema) -> Self {
        Self {
            fields_chain: vec![],
            outer_schema: schema,
        }
    }
}

impl ExprSchema for ExtendableExprSchema<'_> {
    fn field_from_column(&self, col: &datafusion_common::Column) -> Result<&Field> {
        if col.relation.is_none() {
            for fields in &self.fields_chain {
                if let Some((_index, lambda_param)) = fields.find(&col.name) {
                    return Ok(lambda_param);
                }
            }
        }

        self.outer_schema.field_from_column(col)
    }
}

#[derive(Clone, Debug)]
pub enum ValueOrLambdaParameter<'a> {
    /// A columnar value with the given field
    Value(FieldRef),
    /// A lambda with the given parameters names and a flag indicating wheter it captures any columns
    Lambda(&'a [String], bool),
}

impl<F> From<F> for ScalarUDF
where
    F: ScalarUDFImpl + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Arguments passed to [`ScalarUDFImpl::invoke_with_args`] when invoking a
/// scalar function.
#[derive(Debug, Clone)]
pub struct ScalarFunctionArgs {
    /// The evaluated arguments to the function
    /// If it's a lambda, will be `ColumnarValue::Scalar(ScalarValue::Null)`
    pub args: Vec<ColumnarValue>,
    /// Field associated with each arg, if it exists
    pub arg_fields: Vec<FieldRef>,
    /// The number of rows in record batch being evaluated
    pub number_rows: usize,
    /// The return field of the scalar function returned (from `return_type`
    /// or `return_field_from_args`) when creating the physical expression
    /// from the logical expression
    pub return_field: FieldRef,
    /// The config options at execution time
    pub config_options: Arc<ConfigOptions>,
    /// The lambdas passed to the function
    /// If it's not a lambda it will be `None`
    pub lambdas: Option<Vec<Option<ScalarFunctionLambdaArg>>>,
}

/// A lambda argument to a ScalarFunction
#[derive(Clone, Debug)]
pub struct ScalarFunctionLambdaArg {
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

impl ScalarFunctionArgs {
    /// The return type of the function. See [`Self::return_field`] for more
    /// details.
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    pub fn to_lambda_args(&self) -> Vec<ValueOrLambda<'_>> {
        match &self.lambdas {
            Some(lambdas) => std::iter::zip(&self.args, lambdas)
                .map(|(arg, lambda)| match lambda {
                    Some(lambda) => ValueOrLambda::Lambda(lambda),
                    None => ValueOrLambda::Value(arg),
                })
                .collect(),
            None => self.args.iter().map(ValueOrLambda::Value).collect(),
        }
    }
}

// An argument to a ScalarUDF that supports lambdas
#[derive(Debug)]
pub enum ValueOrLambda<'a> {
    Value(&'a ColumnarValue),
    Lambda(&'a ScalarFunctionLambdaArg),
}

/// Information about arguments passed to the function
///
/// This structure contains metadata about how the function was called
/// such as the type of the arguments, any scalar arguments and if the
/// arguments can (ever) be null
///
/// See [`ScalarUDFImpl::return_field_from_args`] for more information
#[derive(Debug)]
pub struct ReturnFieldArgs<'a> {
    /// The data types of the arguments to the function
    ///
    /// If argument `i` to the function is a lambda, it will be the field returned by the
    /// lambda when executed with the arguments returned from `ScalarUDFImpl::lambdas_parameters`
    ///
    /// For example, with `array_transform([1], v -> v == 5)`
    /// this field will be `[Field::new("", DataType::List(DataType::Int32), false), Field::new("", DataType::Boolean, false)]`
    pub arg_fields: &'a [FieldRef],
    /// Is argument `i` to the function a scalar (constant)?
    ///
    /// If the argument `i` is not a scalar, it will be None
    ///
    /// For example, if a function is called like `my_function(column_a, 5)`
    /// this field will be `[None, Some(ScalarValue::Int32(Some(5)))]`
    pub scalar_arguments: &'a [Option<&'a ScalarValue>],
    /// Is argument `i` to the function a lambda?
    ///
    /// For example, with `array_transform([1], v -> v == 5)`
    /// this field will be `[false, true]`
    pub lambdas: &'a [bool],
}

/// A tagged Field indicating whether it correspond to a value or a lambda argument
#[derive(Debug)]
pub enum ValueOrLambdaField<'a> {
    /// The Field of a ColumnarValue argument
    Value(&'a FieldRef),
    /// The Field of the return of the lambda body when evaluated with the parameters from ScalarUDF::lambda_parameters
    Lambda(&'a FieldRef),
}

impl<'a> ReturnFieldArgs<'a> {
    /// Based on self.lambdas, encodes self.arg_fields to tagged enums
    /// indicating whether it correspond to a value or a lambda argument
    pub fn to_lambda_args(&self) -> Vec<ValueOrLambdaField<'a>> {
        std::iter::zip(self.arg_fields, self.lambdas)
            .map(|(field, is_lambda)| {
                if *is_lambda {
                    ValueOrLambdaField::Lambda(field)
                } else {
                    ValueOrLambdaField::Value(field)
                }
            })
            .collect()
    }
}

/// Trait for implementing user defined scalar functions.
///
/// This trait exposes the full API for implementing user defined functions and
/// can be used to implement any function.
///
/// See [`advanced_udf.rs`] for a full example with complete implementation and
/// [`ScalarUDF`] for other available options.
///
/// [`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
///
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use std::sync::LazyLock;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Documentation, ScalarFunctionArgs, Signature, Volatility};
/// # use datafusion_expr::{ScalarUDFImpl, ScalarUDF};
/// # use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
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
///    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
///         unimplemented!()
///    }
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
pub trait ScalarUDFImpl: Debug + DynEq + DynHash + Send + Sync {
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

    /// Returns the user-defined display name of function, given the arguments
    ///
    /// This can be used to customize the output column name generated by this
    /// function.
    ///
    /// Defaults to `name(args[0], args[1], ...)`
    #[deprecated(
        since = "50.0.0",
        note = "This method is unused and will be removed in a future release"
    )]
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

    /// [`DataType`] returned by this function, given the types of the
    /// arguments.
    ///
    /// # Arguments
    ///
    /// `arg_types` Data types of the arguments. The implementation of
    /// `return_type` can assume that some other part of the code has coerced
    /// the actual argument types to match [`Self::signature`].
    ///
    /// # Notes
    ///
    /// If you provide an implementation for [`Self::return_field_from_args`],
    /// DataFusion will not call `return_type` (this function). While it is
    /// valid to to put [`unimplemented!()`] or [`unreachable!()`], it is
    /// recommended to return [`DataFusionError::Internal`] instead, which
    /// reduces the severity of symptoms if bugs occur (an error rather than a
    /// panic).
    ///
    /// [`DataFusionError::Internal`]: datafusion_common::DataFusionError::Internal
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

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
    /// * `Some(ScalarUDF)` - A new instance of this function configured with the new settings
    /// * `None` - If this function does not change with new configuration settings (the default)
    fn with_updated_config(&self, _config: &ConfigOptions) -> Option<ScalarUDF> {
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
    /// # use datafusion_expr::ReturnFieldArgs;
    /// # struct Example{}
    /// # impl Example {
    /// fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
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
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>();
        let return_type = self.return_type(&data_types)?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    #[deprecated(
        since = "45.0.0",
        note = "Use `return_field_from_args` instead. if you use `is_nullable` that returns non-nullable with `return_type`, you would need to switch to `return_field_from_args`, you might have error"
    )]
    fn is_nullable(&self, _args: &[Expr], _schema: &dyn ExprSchema) -> bool {
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue>;

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
    /// When overriding this function to return `true`, [ScalarUDFImpl::conditional_arguments] can also be
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
    /// When overriding this function, [ScalarUDFImpl::short_circuits] must
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

    /// Computes the output [`Interval`] for a [`ScalarUDFImpl`], given the input
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
    /// This function is only called if [`ScalarUDFImpl::signature`] returns
    /// [`crate::TypeSignature::UserDefined`]. Most UDFs should return one of
    /// the other variants of [`TypeSignature`] which handle common cases.
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
    /// * `arg_types`: The argument types of the arguments  this function with
    ///
    /// # Return value
    /// A Vec the same length as `arg_types`. DataFusion will `CAST` the function call
    /// arguments to these specific types.
    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        not_impl_err!("Function {} does not implement coerce_types", self.name())
    }

    /// Returns the documentation for this Scalar UDF.
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

/// ScalarUDF that adds an alias to the underlying function. It is better to
/// implement [`ScalarUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug, PartialEq, Eq, Hash)]
struct AliasedScalarUDFImpl {
    inner: UdfEq<Arc<dyn ScalarUDFImpl>>,
    aliases: Vec<String>,
}

impl AliasedScalarUDFImpl {
    pub fn new(
        inner: Arc<dyn ScalarUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));
        Self {
            inner: inner.into(),
            aliases,
        }
    }
}

#[warn(clippy::missing_trait_methods)] // Delegates, so it should implement every single trait method
impl ScalarUDFImpl for AliasedScalarUDFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[expect(deprecated)]
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[allow(deprecated)]
        self.inner.is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }

    fn with_updated_config(&self, _config: &ConfigOptions) -> Option<ScalarUDF> {
        None
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.conditional_arguments(args)
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

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambdaParameter],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        self.inner.lambdas_parameters(args)
    }
}

fn lambda_parameters<'a>(
    args: &'a [Expr],
    schema: &dyn ExprSchema,
) -> Result<Vec<ValueOrLambdaParameter<'a>>> {
    args.iter()
        .map(|e| match e {
            Expr::Lambda(Lambda { params, body: _ }) => {
                let mut captures = false;

                e.apply_with_lambdas_params(|expr, lambdas_params| match expr {
                    Expr::Column(c) if !c.is_lambda_parameter(lambdas_params) => {
                        captures = true;

                        Ok(TreeNodeRecursion::Stop)
                    }
                    _ => Ok(TreeNodeRecursion::Continue),
                })
                .unwrap();

                Ok(ValueOrLambdaParameter::Lambda(params.as_slice(), captures))
            }
            _ => Ok(ValueOrLambdaParameter::Value(e.to_field(schema)?.1)),
        })
        .collect()
}

/// Merge the lambda body captured columns with it's arguments
/// Datafusion relies on an unspecified field ordering implemented in this function
/// As such, this is the only correct way to merge the captured values with the arguments
/// The number of args should not be lower than the number of params
///
/// See also merge_captures_with_lazy_args and merge_captures_with_boxed_lazy_args that lazily
/// computes only the necessary arguments to match the number of params
pub fn merge_captures_with_args(
    captures: Option<&RecordBatch>,
    params: &[FieldRef],
    args: &[ArrayRef],
) -> Result<RecordBatch> {
    if args.len() < params.len() {
        return exec_err!(
            "merge_captures_with_args called with {} params but with {} args",
            params.len(),
            args.len()
        );
    }

    // the order of the merged batch must be kept in sync with ScalarFunction::lambdas_schemas variants
    let (fields, columns) = match captures {
        Some(captures) => {
            let fields = captures
                .schema()
                .fields()
                .iter()
                .chain(params)
                .cloned()
                .collect::<Vec<_>>();

            let columns = [captures.columns(), args].concat();

            (fields, columns)
        }
        None => (params.to_vec(), args.to_vec()),
    };

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Lazy version of merge_captures_with_args that receives closures to compute the arguments,
/// and calls only the necessary to match the number of params
pub fn merge_captures_with_lazy_args(
    captures: Option<&RecordBatch>,
    params: &[FieldRef],
    args: &[&dyn Fn() -> Result<ArrayRef>],
) -> Result<RecordBatch> {
    merge_captures_with_args(
        captures,
        params,
        &args
            .iter()
            .take(params.len())
            .map(|arg| arg())
            .collect::<Result<Vec<_>>>()?,
    )
}

/// Variation of merge_captures_with_lazy_args that take boxed closures
pub fn merge_captures_with_boxed_lazy_args(
    captures: Option<&RecordBatch>,
    params: &[FieldRef],
    args: &[Box<dyn Fn() -> Result<ArrayRef>>],
) -> Result<RecordBatch> {
    merge_captures_with_args(
        captures,
        params,
        &args
            .iter()
            .take(params.len())
            .map(|arg| arg())
            .collect::<Result<Vec<_>>>()?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr_common::signature::Volatility;
    use std::hash::DefaultHasher;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestScalarUDFImpl {
        name: &'static str,
        field: &'static str,
        signature: Signature,
    }
    impl ScalarUDFImpl for TestScalarUDFImpl {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            unimplemented!()
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
        assert_eq!(f, f2);
        assert_eq!(hash(&f), hash(&f2));
        assert_eq!(f.partial_cmp(&f2), Some(Ordering::Equal));

        // Different parameter
        let b = test_func("foo", "b");
        assert_ne!(f, b);
        assert_ne!(hash(&f), hash(&b)); // hash can collide for different values but does not collide in this test
        assert_eq!(f.partial_cmp(&b), None);

        // Different name
        let o = test_func("other", "a");
        assert_ne!(f, o);
        assert_ne!(hash(&f), hash(&o)); // hash can collide for different values but does not collide in this test
        assert_eq!(f.partial_cmp(&o), Some(Ordering::Less));

        // Different name and parameter
        assert_ne!(b, o);
        assert_ne!(hash(&b), hash(&o)); // hash can collide for different values but does not collide in this test
        assert_eq!(b.partial_cmp(&o), Some(Ordering::Less));
    }

    fn test_func(name: &'static str, parameter: &'static str) -> ScalarUDF {
        ScalarUDF::from(TestScalarUDFImpl {
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

    use std::borrow::Cow;

    use arrow::datatypes::Fields;

    use crate::{
        tree_node::tests::{args, list_int, list_list_int, array_transform_udf},
        udf::{lambda_parameters, ExtendableExprSchema},
    };

    #[test]
    fn test_arguments_expr_schema() {
        let args = args();
        let schema = list_list_int();

        let schemas = array_transform_udf()
            .arguments_expr_schema(&args, &schema)
            .unwrap()
            .into_iter()
            .map(|s| format!("{s:?}"))
            .collect::<Vec<_>>();

        let mut lambdas_parameters = array_transform_udf()
            .inner()
            .lambdas_parameters(&lambda_parameters(&args, &schema).unwrap())
            .unwrap();

        assert_eq!(
            schemas,
            &[
                format!("{}", &list_list_int()),
                format!(
                    "{:?}",
                    ExtendableExprSchema {
                        fields_chain: vec![Fields::from(
                            lambdas_parameters[0].take().unwrap()
                        )],
                        outer_schema: &list_list_int()
                    }
                ),
            ]
        )
    }

    #[test]
    fn test_arguments_arrow_schema() {
        let list_int = list_int();
        let list_list_int = list_list_int();

        let schemas = array_transform_udf()
            .arguments_arrow_schema(
                &lambda_parameters(&args(), &list_list_int).unwrap(),
                //&[HashSet::new(), HashSet::from([0])],
                list_list_int.as_arrow(),
            )
            .unwrap();

        assert_eq!(
            schemas,
            &[
                Cow::Borrowed(list_list_int.as_arrow()),
                Cow::Owned(list_int.as_arrow().clone())
            ]
        )
    }

    #[test]
    fn test_arguments_schema_from_logical_args() {
        let list_list_int = list_list_int();

        let schemas = array_transform_udf()
            .arguments_schema_from_logical_args(&args(), &list_list_int)
            .unwrap();

        assert_eq!(
            schemas,
            &[Cow::Borrowed(&list_list_int), Cow::Owned(list_int())]
        )
    }
}
