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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::expressions::{Column, LambdaExpr, Literal};
use crate::PhysicalExpr;

use arrow::array::{Array, NullArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{internal_err, HashSet, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::data_types_with_scalar_udf;
use datafusion_expr::{
    expr_vec_fmt, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarFunctionLambdaArg, ScalarUDF, ValueOrLambdaParameter, Volatility,
};

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: Arc<ScalarUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_field: FieldRef,
        config_options: Arc<ConfigOptions>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_field,
            config_options,
        }
    }

    /// Create a new Scalar function
    pub fn try_new(
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let lambdas_schemas = lambdas_schemas_from_args(&fun, &args, schema)?;

        let arg_fields = std::iter::zip(&args, lambdas_schemas)
            .map(|(e, schema)| {
                if let Some(lambda) = e.as_any().downcast_ref::<LambdaExpr>() {
                    lambda.body().return_field(&schema)
                } else {
                    e.return_field(&schema)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // verify that input data types is consistent with function's `TypeSignature`
        let arg_types = arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();

        data_types_with_scalar_udf(&arg_types, &fun)?;

        let arguments = args
            .iter()
            .map(|e| {
                e.as_any()
                    .downcast_ref::<Literal>()
                    .map(|literal| literal.value())
            })
            .collect::<Vec<_>>();

        let lambdas = args
            .iter()
            .map(|e| e.as_any().is::<LambdaExpr>())
            .collect::<Vec<_>>();

        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &arguments,
            lambdas: &lambdas,
        };

        let return_field = fun.return_field_from_args(ret_args)?;
        let name = fun.name().to_string();

        Ok(Self {
            fun,
            name,
            args,
            return_field,
            config_options,
        })
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.return_field = self
            .return_field
            .as_ref()
            .clone()
            .with_nullable(nullable)
            .into();
        self
    }

    pub fn nullable(&self) -> bool {
        self.return_field.is_nullable()
    }

    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Given an arbitrary PhysicalExpr attempt to downcast it to a ScalarFunctionExpr
    /// and verify that its inner function is of type T.
    /// If the downcast fails, or the function is not of type T, returns `None`.
    /// Otherwise returns `Some(ScalarFunctionExpr)`.
    pub fn try_downcast_func<T>(expr: &dyn PhysicalExpr) -> Option<&ScalarFunctionExpr>
    where
        T: 'static,
    {
        match expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            Some(scalar_expr)
                if scalar_expr
                    .fun()
                    .inner()
                    .as_any()
                    .downcast_ref::<T>()
                    .is_some() =>
            {
                Some(scalar_expr)
            }
            _ => None,
        }
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PartialEq for ScalarFunctionExpr {
    fn eq(&self, o: &Self) -> bool {
        if std::ptr::eq(self, o) {
            // The equality implementation is somewhat expensive, so let's short-circuit when possible.
            return true;
        }
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options,
        } = self;
        fun.eq(&o.fun)
            && name.eq(&o.name)
            && args.eq(&o.args)
            && return_field.eq(&o.return_field)
            && (Arc::ptr_eq(config_options, &o.config_options)
                || sorted_config_entries(config_options)
                    == sorted_config_entries(&o.config_options))
    }
}
impl Eq for ScalarFunctionExpr {}
impl Hash for ScalarFunctionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options: _, // expensive to hash, and often equal
        } = self;
        fun.hash(state);
        name.hash(state);
        args.hash(state);
        return_field.hash(state);
    }
}

fn sorted_config_entries(config_options: &ConfigOptions) -> Vec<ConfigEntry> {
    let mut entries = config_options.entries();
    entries.sort_by(|l, r| l.key.cmp(&r.key));
    entries
}

impl PhysicalExpr for ScalarFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let args = self
            .args
            .iter()
            .map(|e| match e.as_any().downcast_ref::<LambdaExpr>() {
                Some(_) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
                None => Ok(e.evaluate(batch)?),
            })
            .collect::<Result<Vec<_>>>()?;

        let arg_fields = self
            .args
            .iter()
            .map(|e| e.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

        let lambdas = if self.args.iter().any(|arg| arg.as_any().is::<LambdaExpr>()) {
            let args_metadata = std::iter::zip(&self.args, &arg_fields)
                .map(
                    |(expr, field)| match expr.as_any().downcast_ref::<LambdaExpr>() {
                        Some(lambda) => {
                            let mut captures = false;

                            expr.apply_with_lambdas_params(|expr, lambdas_params| {
                                match expr.as_any().downcast_ref::<Column>() {
                                    Some(col) if !lambdas_params.contains(col.name()) => {
                                        captures = true;

                                        Ok(TreeNodeRecursion::Stop)
                                    }
                                    _ => Ok(TreeNodeRecursion::Continue),
                                }
                            })
                            .unwrap();

                            ValueOrLambdaParameter::Lambda(lambda.params(), captures)
                        }
                        None => ValueOrLambdaParameter::Value(Arc::clone(field)),
                    },
                )
                .collect::<Vec<_>>();

            let params = self.fun().inner().lambdas_parameters(&args_metadata)?;

            let lambdas = std::iter::zip(&self.args, params)
                .map(|(arg, lambda_params)| {
                    arg.as_any()
                        .downcast_ref::<LambdaExpr>()
                        .map(|lambda| {
                            let mut indices = HashSet::new();

                            arg.apply_with_lambdas_params(|expr, lambdas_params| {
                                if let Some(column) =
                                    expr.as_any().downcast_ref::<Column>()
                                {
                                    if !lambdas_params.contains(column.name()) {
                                        indices.insert(
                                            column.index(), //batch
                                                            //    .schema_ref()
                                                            //    .index_of(column.name())?,
                                        );
                                    }
                                }

                                Ok(TreeNodeRecursion::Continue)
                            })?;

                            //let mut indices = indices.into_iter().collect::<Vec<_>>();

                            //indices.sort_unstable();

                            let params =
                                std::iter::zip(lambda.params(), lambda_params.unwrap())
                                    .map(|(name, param)| Arc::new(param.with_name(name)))
                                    .collect();

                            let captures = if !indices.is_empty() {
                                let (fields, columns): (Vec<_>, _) = std::iter::zip(
                                    batch.schema_ref().fields(),
                                    batch.columns(),
                                )
                                .enumerate()
                                .map(|(column_index, (field, column))| {
                                    if indices.contains(&column_index) {
                                        (Arc::clone(field), Arc::clone(column))
                                    } else {
                                        (
                                            Arc::new(Field::new(
                                                field.name(),
                                                DataType::Null,
                                                false,
                                            )),
                                            Arc::new(NullArray::new(column.len())) as _,
                                        )
                                    }
                                })
                                .unzip();

                                let schema = Arc::new(Schema::new(fields));

                                Some(RecordBatch::try_new(schema, columns)?)
                                //Some(batch.project(&indices)?)
                            } else {
                                None
                            };

                            Ok(ScalarFunctionLambdaArg {
                                params,
                                body: Arc::clone(lambda.body()),
                                captures,
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;

            Some(lambdas)
        } else {
            None
        };

        // evaluate the function
        let output = self.fun.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
            lambdas,
        })?;

        if let ColumnarValue::Array(array) = &output {
            if array.len() != batch.num_rows() {
                // If the arguments are a non-empty slice of scalar values, we can assume that
                // returning a one-element array is equivalent to returning a scalar.
                let preserve_scalar =
                    array.len() == 1 && !input_empty && input_all_scalar;
                return if preserve_scalar {
                    ScalarValue::try_from_array(array, 0).map(ColumnarValue::Scalar)
                } else {
                    internal_err!("UDF {} returned a different number of rows than expected. Expected: {}, Got: {}",
                            self.name, batch.num_rows(), array.len())
                };
            }
        }
        Ok(output)
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::new(
            &self.name,
            Arc::clone(&self.fun),
            children,
            Arc::clone(&self.return_field),
            Arc::clone(&self.config_options),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.fun.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.fun.propagate_constraints(interval, children)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let sort_properties = self.fun.output_ordering(children)?;
        let preserves_lex_ordering = self.fun.preserves_lex_ordering(children)?;
        let children_range = children
            .iter()
            .map(|props| &props.range)
            .collect::<Vec<_>>();
        let range = self.fun().evaluate_bounds(&children_range)?;

        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering,
        })
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, expr) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, ")")
    }

    fn is_volatile_node(&self) -> bool {
        self.fun.signature().volatility == Volatility::Volatile
    }
}

pub fn lambdas_schemas_from_args<'a>(
    fun: &ScalarUDF,
    args: &[Arc<dyn PhysicalExpr>],
    schema: &'a Schema,
) -> Result<Vec<Cow<'a, Schema>>> {
    let args_metadata = args
        .iter()
        .map(|e| match e.as_any().downcast_ref::<LambdaExpr>() {
            Some(lambda) => {
                let mut captures = false;

                e.apply_with_lambdas_params(|expr, lambdas_params| {
                    match expr.as_any().downcast_ref::<Column>() {
                        Some(col) if !lambdas_params.contains(col.name()) => {
                            captures = true;

                            Ok(TreeNodeRecursion::Stop)
                        }
                        _ => Ok(TreeNodeRecursion::Continue),
                    }
                })
                .unwrap();

                Ok(ValueOrLambdaParameter::Lambda(lambda.params(), captures))
            }
            None => Ok(ValueOrLambdaParameter::Value(e.return_field(schema)?)),
        })
        .collect::<Result<Vec<_>>>()?;

    /*let captures = args
    .iter()
    .map(|arg| {
        if arg.as_any().is::<LambdaExpr>() {
            let mut columns = HashSet::new();

            arg.apply_with_lambdas_params(|n, lambdas_params| {
                if let Some(column) = n.as_any().downcast_ref::<Column>() {
                    if !lambdas_params.contains(column.name()) {
                        columns.insert(schema.index_of(column.name())?);
                    }
                    // columns.insert(column.index());
                }

                Ok(TreeNodeRecursion::Continue)
            })?;

            Ok(columns)
        } else {
            Ok(HashSet::new())
        }
    })
    .collect::<Result<Vec<_>>>()?; */

    fun.arguments_arrow_schema(&args_metadata, schema)
}

pub trait PhysicalExprExt: Sized {
    fn apply_with_lambdas_params<
        'n,
        F: FnMut(&'n Self, &HashSet<&'n str>) -> Result<TreeNodeRecursion>,
    >(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    fn apply_with_schema<'n, F: FnMut(&'n Self, &Schema) -> Result<TreeNodeRecursion>>(
        &'n self,
        schema: &Schema,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    fn apply_children_with_schema<
        'n,
        F: FnMut(&'n Self, &Schema) -> Result<TreeNodeRecursion>,
    >(
        &'n self,
        schema: &Schema,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    fn transform_down_with_schema<F: FnMut(Self, &Schema) -> Result<Transformed<Self>>>(
        self,
        schema: &Schema,
        f: F,
    ) -> Result<Transformed<Self>>;

    fn transform_up_with_schema<F: FnMut(Self, &Schema) -> Result<Transformed<Self>>>(
        self,
        schema: &Schema,
        f: F,
    ) -> Result<Transformed<Self>>;

    fn transform_with_schema<F: FnMut(Self, &Schema) -> Result<Transformed<Self>>>(
        self,
        schema: &Schema,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up_with_schema(schema, f)
    }

    fn transform_down_with_lambdas_params(
        self,
        f: impl FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    ) -> Result<Transformed<Self>>;

    fn transform_up_with_lambdas_params(
        self,
        f: impl FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    ) -> Result<Transformed<Self>>;

    fn transform_with_lambdas_params(
        self,
        f: impl FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    ) -> Result<Transformed<Self>> {
        self.transform_up_with_lambdas_params(f)
    }
}

impl PhysicalExprExt for Arc<dyn PhysicalExpr> {
    fn apply_with_lambdas_params<
        'n,
        F: FnMut(&'n Self, &HashSet<&'n str>) -> Result<TreeNodeRecursion>,
    >(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn apply_with_lambdas_params_impl<
            'n,
            F: FnMut(
                &'n Arc<dyn PhysicalExpr>,
                &HashSet<&'n str>,
            ) -> Result<TreeNodeRecursion>,
        >(
            node: &'n Arc<dyn PhysicalExpr>,
            args: &HashSet<&'n str>,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            match node.as_any().downcast_ref::<LambdaExpr>() {
                Some(lambda) => {
                    let mut args = args.clone();

                    args.extend(lambda.params().iter().map(|v| v.as_str()));

                    f(node, &args)?.visit_children(|| {
                        node.apply_children(|c| {
                            apply_with_lambdas_params_impl(c, &args, f)
                        })
                    })
                }
                _ => f(node, args)?.visit_children(|| {
                    node.apply_children(|c| apply_with_lambdas_params_impl(c, args, f))
                }),
            }
        }

        apply_with_lambdas_params_impl(self, &HashSet::new(), &mut f)
    }

    fn apply_with_schema<'n, F: FnMut(&'n Self, &Schema) -> Result<TreeNodeRecursion>>(
        &'n self,
        schema: &Schema,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn apply_with_lambdas_impl<
            'n,
            F: FnMut(&'n Arc<dyn PhysicalExpr>, &Schema) -> Result<TreeNodeRecursion>,
        >(
            node: &'n Arc<dyn PhysicalExpr>,
            schema: &Schema,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node, schema)?.visit_children(|| {
                node.apply_children_with_schema(schema, |c, schema| {
                    apply_with_lambdas_impl(c, schema, f)
                })
            })
        }

        apply_with_lambdas_impl(self, schema, &mut f)
    }

    fn apply_children_with_schema<
        'n,
        F: FnMut(&'n Self, &Schema) -> Result<TreeNodeRecursion>,
    >(
        &'n self,
        schema: &Schema,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        match self.as_any().downcast_ref::<ScalarFunctionExpr>() {
            Some(scalar_function)
                if scalar_function
                    .args()
                    .iter()
                    .any(|arg| arg.as_any().is::<LambdaExpr>()) =>
            {
                let mut lambdas_schemas = lambdas_schemas_from_args(
                    scalar_function.fun(),
                    scalar_function.args(),
                    schema,
                )?
                .into_iter();

                self.apply_children(|expr| f(expr, &lambdas_schemas.next().unwrap()))
            }
            _ => self.apply_children(|e| f(e, schema)),
        }
    }

    fn transform_down_with_schema<
        F: FnMut(Self, &Schema) -> Result<Transformed<Self>>,
    >(
        self,
        schema: &Schema,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_with_schema_impl<
            F: FnMut(
                Arc<dyn PhysicalExpr>,
                &Schema,
            ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
        >(
            node: Arc<dyn PhysicalExpr>,
            schema: &Schema,
            f: &mut F,
        ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
            f(node, schema)?.transform_children(|node| {
                map_children_with_schema(node, schema, |n, schema| {
                    transform_down_with_schema_impl(n, schema, f)
                })
            })
        }

        transform_down_with_schema_impl(self, schema, &mut f)
    }

    fn transform_up_with_schema<F: FnMut(Self, &Schema) -> Result<Transformed<Self>>>(
        self,
        schema: &Schema,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_up_with_schema_impl<
            F: FnMut(
                Arc<dyn PhysicalExpr>,
                &Schema,
            ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
        >(
            node: Arc<dyn PhysicalExpr>,
            schema: &Schema,
            f: &mut F,
        ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
            map_children_with_schema(node, schema, |n, schema| {
                transform_up_with_schema_impl(n, schema, f)
            })?
            .transform_parent(|n| f(n, schema))
        }

        transform_up_with_schema_impl(self, schema, &mut f)
    }

    fn transform_up_with_lambdas_params(
        self,
        mut f: impl FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_up_with_lambdas_params_impl<
            F: FnMut(
                Arc<dyn PhysicalExpr>,
                &HashSet<String>,
            ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
        >(
            node: Arc<dyn PhysicalExpr>,
            params: &HashSet<String>,
            f: &mut F,
        ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
            map_children_with_lambdas_params(node, params, |n, params| {
                transform_up_with_lambdas_params_impl(n, params, f)
            })?
            .transform_parent(|n| f(n, params))
        }

        transform_up_with_lambdas_params_impl(self, &HashSet::new(), &mut f)
    }

    fn transform_down_with_lambdas_params(
        self,
        mut f: impl FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_with_lambdas_params_impl<
            F: FnMut(
                Arc<dyn PhysicalExpr>,
                &HashSet<String>,
            ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
        >(
            node: Arc<dyn PhysicalExpr>,
            params: &HashSet<String>,
            f: &mut F,
        ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
            f(node, params)?.transform_children(|node| {
                map_children_with_lambdas_params(node, params, |node, args| {
                    transform_down_with_lambdas_params_impl(node, args, f)
                })
            })
        }

        transform_down_with_lambdas_params_impl(self, &HashSet::new(), &mut f)
    }
}

fn map_children_with_schema(
    node: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    mut f: impl FnMut(
        Arc<dyn PhysicalExpr>,
        &Schema,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    match node.as_any().downcast_ref::<ScalarFunctionExpr>() {
        Some(fun) if fun.args().iter().any(|arg| arg.as_any().is::<LambdaExpr>()) => {
            let mut args_schemas =
                lambdas_schemas_from_args(fun.fun(), fun.args(), schema)?.into_iter();

            node.map_children(|node| f(node, &args_schemas.next().unwrap()))
        }
        _ => node.map_children(|node| f(node, schema)),
    }
}

fn map_children_with_lambdas_params(
    node: Arc<dyn PhysicalExpr>,
    params: &HashSet<String>,
    mut f: impl FnMut(
        Arc<dyn PhysicalExpr>,
        &HashSet<String>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    match node.as_any().downcast_ref::<LambdaExpr>() {
        Some(lambda) => {
            let mut params = params.clone();

            params.extend(lambda.params().iter().cloned());

            node.map_children(|node| f(node, &params))
        }
        None => node.map_children(|node| f(node, params)),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::{borrow::Cow, sync::Arc};

    use super::*;
    use super::{lambdas_schemas_from_args, PhysicalExprExt};
    use crate::expressions::Column;
    use crate::{create_physical_expr, ScalarFunctionExpr};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{tree_node::TreeNodeRecursion, DFSchema, HashSet, Result};
    use datafusion_expr::{
        col, expr::Lambda, Expr, ScalarFunctionArgs, ValueOrLambdaParameter, Volatility,
    };
    use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_physical_expr_common::physical_expr::is_volatile;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    /// Test helper to create a mock UDF with a specific volatility
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockScalarUDF {
        signature: Signature,
    }

    impl ScalarUDFImpl for MockScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))))
        }
    }

    #[test]
    fn test_scalar_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));

        // Create a non-volatile UDF
        let stable_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        }));

        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let config_options = Arc::new(ConfigOptions::new());

        // Test volatile function
        let volatile_expr = ScalarFunctionExpr::try_new(
            volatile_udf,
            args.clone(),
            &schema,
            Arc::clone(&config_options),
        )
        .unwrap();

        assert!(volatile_expr.is_volatile_node());
        let volatile_arc: Arc<dyn PhysicalExpr> = Arc::new(volatile_expr);
        assert!(is_volatile(&volatile_arc));

        // Test non-volatile function
        let stable_expr =
            ScalarFunctionExpr::try_new(stable_udf, args, &schema, config_options)
                .unwrap();

        assert!(!stable_expr.is_volatile_node());
        let stable_arc: Arc<dyn PhysicalExpr> = Arc::new(stable_expr);
        assert!(!is_volatile(&stable_arc));
    }

    fn list_list_int() -> Schema {
        Schema::new(vec![Field::new(
            "v",
            DataType::new_list(DataType::new_list(DataType::Int32, false), false),
            false,
        )])
    }

    fn list_int() -> Schema {
        Schema::new(vec![Field::new(
            "v",
            DataType::new_list(DataType::Int32, false),
            false,
        )])
    }

    fn int() -> Schema {
        Schema::new(vec![Field::new("v", DataType::Int32, false)])
    }

    fn array_transform_udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(ArrayTransformFunc::new())
    }

    fn args() -> Vec<Expr> {
        vec![
            col("v"),
            Expr::Lambda(Lambda::new(
                vec!["v".into()],
                array_transform_udf().call(vec![
                    col("v"),
                    Expr::Lambda(Lambda::new(vec!["v".into()], -col("v"))),
                ]),
            )),
        ]
    }

    // array_transform(v, |v| -> array_transform(v, |v| -> -v))
    fn array_transform() -> Arc<dyn PhysicalExpr> {
        let e = array_transform_udf().call(args());

        create_physical_expr(
            &e,
            &DFSchema::try_from(list_list_int()).unwrap(),
            &Default::default(),
        )
        .unwrap()
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ArrayTransformFunc {
        signature: Signature,
    }

    impl ArrayTransformFunc {
        pub fn new() -> Self {
            Self {
                signature: Signature::any(2, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for ArrayTransformFunc {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "array_transform"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            Ok(arg_types[0].clone())
        }

        fn lambdas_parameters(
            &self,
            args: &[ValueOrLambdaParameter],
        ) -> Result<Vec<Option<Vec<Field>>>> {
            let ValueOrLambdaParameter::Value(value_field) = &args[0] else {
                unimplemented!()
            };
            let DataType::List(field) = value_field.data_type() else {
                unimplemented!()
            };

            Ok(vec![
                None,
                Some(vec![Field::new(
                    "",
                    field.data_type().clone(),
                    field.is_nullable(),
                )]),
            ])
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn test_lambdas_schemas_from_args() {
        let schema = list_list_int();
        let expr = array_transform();

        let args = expr
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .unwrap()
            .args();

        let schemas =
            lambdas_schemas_from_args(&array_transform_udf(), args, &schema).unwrap();

        assert_eq!(schemas, &[Cow::Borrowed(&schema), Cow::Owned(list_int())]);
    }

    #[test]
    fn test_apply_with_schema() {
        let mut steps = vec![];

        array_transform()
            .apply_with_schema(&list_list_int(), |node, schema| {
                steps.push((node.to_string(), schema.clone()));

                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();

        let expected = [
            (
                "array_transform(v@0, (v) -> array_transform(v@0, (v) -> (- v@0)))",
                list_list_int(),
            ),
            ("(v) -> array_transform(v@0, (v) -> (- v@0))", list_int()),
            ("array_transform(v@0, (v) -> (- v@0))", list_int()),
            ("(v) -> (- v@0)", int()),
            ("(- v@0)", int()),
            ("v@0", int()),
            ("v@0", int()),
            ("v@0", int()),
        ]
        .map(|(a, b)| (String::from(a), b));

        assert_eq!(steps, expected);
    }

    #[test]
    fn test_apply_with_lambdas_params() {
        let array_transform = array_transform();
        let mut steps = vec![];

        array_transform
            .apply_with_lambdas_params(|node, params| {
                steps.push((node.to_string(), params.clone()));

                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();

        let expected = [
            (
                "array_transform(v@0, (v) -> array_transform(v@0, (v) -> (- v@0)))",
                HashSet::from(["v"]),
            ),
            (
                "(v) -> array_transform(v@0, (v) -> (- v@0))",
                HashSet::from(["v"]),
            ),
            ("array_transform(v@0, (v) -> (- v@0))", HashSet::from(["v"])),
            ("(v) -> (- v@0)", HashSet::from(["v"])),
            ("(- v@0)", HashSet::from(["v"])),
            ("v@0", HashSet::from(["v"])),
            ("v@0", HashSet::from(["v"])),
            ("v@0", HashSet::from(["v"])),
        ]
        .map(|(a, b)| (String::from(a), b));

        assert_eq!(steps, expected);
    }
}
