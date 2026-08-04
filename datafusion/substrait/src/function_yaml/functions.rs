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

use crate::function_yaml::FunctionYamlOverrides;
use crate::function_yaml::signatures::{
    return_type_or_any, signature_to_impls, type_name,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{HashSet, Result, internal_err};
use datafusion::logical_expr::function::WindowUDFFieldArgs;
use datafusion::logical_expr::{
    AggregateUDF, Documentation, HigherOrderTypeSignature, HigherOrderUDF, ScalarUDF,
    Signature, TypeSignature, ValueOrLambda, WindowUDF,
};
use itertools::Itertools;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use substrait::text::simple_extensions as ext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
}

impl FunctionKind {
    fn yaml_key(self) -> &'static str {
        match self {
            Self::Scalar => "scalar_functions",
            Self::Aggregate => "aggregate_functions",
            Self::Window => "window_functions",
        }
    }
}

pub fn collect_scalar_functions(
    scalar_functions: &[Arc<ScalarUDF>],
    higher_order_functions: &[Arc<dyn HigherOrderUDF>],
    overrides: &FunctionYamlOverrides,
) -> Result<Vec<ext::ScalarFunction>> {
    let functions = scalar_functions
        .iter()
        .map(|f| scalar_function(f, overrides))
        .chain(
            higher_order_functions
                .iter()
                .map(|f| higher_order_function(f, overrides)),
        )
        .collect::<Result<Vec<_>>>()?;
    sort_and_validate_scalar_functions(functions)
}

pub fn collect_aggregate_functions(
    aggregate_functions: &[Arc<AggregateUDF>],
    overrides: &FunctionYamlOverrides,
) -> Result<Vec<ext::AggregateFunction>> {
    let functions = aggregate_functions
        .iter()
        .map(|f| aggregate_function(f, overrides))
        .collect::<Result<Vec<_>>>()?;
    sort_and_validate_aggregate_functions(functions)
}

pub fn collect_window_functions(
    window_functions: &[Arc<WindowUDF>],
    overrides: &FunctionYamlOverrides,
) -> Result<Vec<ext::WindowFunction>> {
    let functions = window_functions
        .iter()
        .map(|f| window_function(f, overrides))
        .collect::<Result<Vec<_>>>()?;
    sort_and_validate_window_functions(functions)
}

pub fn metadata_from_aliases(aliases: &[String]) -> Map<String, Value> {
    let aliases: Vec<String> = aliases.iter().sorted().cloned().collect();
    let mut metadata = Map::new();
    if !aliases.is_empty() {
        metadata.insert("datafusion".to_string(), json!({ "aliases": aliases }));
    }
    metadata
}

pub fn aggregate_impl(
    function_name: &str,
    nullable: bool,
    implementation: ext::ScalarFunctionImplsItem,
) -> ext::AggregateFunctionImplsItem {
    let return_type = type_name(&implementation.return_.0);
    ext::AggregateFunctionImplsItem {
        args: implementation.args,
        decomposable: Some(aggregate_decomposable(function_name)),
        deterministic: implementation.deterministic,
        implementation: implementation.implementation,
        intermediate: Some(ext::Intermediate(ext::Type::String(
            aggregate_intermediate_type(function_name, return_type),
        ))),
        maxset: None,
        nullability: Some(if nullable {
            ext::NullabilityHandling::Mirror
        } else {
            ext::NullabilityHandling::DeclaredOutput
        }),
        options: implementation.options,
        ordered: Some(ext::Ordered(false)),
        return_: implementation.return_,
        session_dependent: implementation.session_dependent,
        variadic: implementation.variadic,
    }
}

pub fn aggregate_decomposable(function_name: &str) -> ext::Decomposable {
    match function_name {
        "median"
        | "approx_median"
        | "percentile_cont"
        | "approx_percentile_cont"
        | "approx_percentile_cont_with_weight" => ext::Decomposable::None,
        _ => ext::Decomposable::Many,
    }
}

pub fn aggregate_intermediate_type(function_name: &str, return_type: &str) -> String {
    match function_name {
        "count" | "regr_count" | "approx_distinct" => "i64".to_string(),
        _ => return_type.to_string(),
    }
}

fn scalar_function(
    function: &Arc<ScalarUDF>,
    overrides: &FunctionYamlOverrides,
) -> Result<ext::ScalarFunction> {
    let signature = function.signature();
    let documentation = function.documentation();
    let arg_names = argument_names(signature, documentation);
    let impls = signature_to_impls(
        function.name(),
        signature,
        &arg_names,
        overrides,
        |arg_types| {
            return_type_or_any(function.name(), overrides, arg_types, || {
                function.return_type(arg_types)
            })
        },
    )?;

    Ok(ext::ScalarFunction {
        description: documentation.map(|doc| doc.description.clone()),
        impls,
        metadata: metadata_from_aliases(function.aliases()),
        name: function.name().to_string(),
    })
}

fn aggregate_function(
    function: &Arc<AggregateUDF>,
    overrides: &FunctionYamlOverrides,
) -> Result<ext::AggregateFunction> {
    let signature = function.signature();
    let documentation = function.documentation();
    let arg_names = argument_names(signature, documentation);
    let nullable = function.is_nullable();
    let impls = signature_to_impls(
        function.name(),
        signature,
        &arg_names,
        overrides,
        |arg_types| {
            return_type_or_any(function.name(), overrides, arg_types, || {
                function.return_type(arg_types)
            })
        },
    )?
    .into_iter()
    .map(|implementation| aggregate_impl(function.name(), nullable, implementation))
    .collect();

    Ok(ext::AggregateFunction {
        description: documentation.map(|doc| doc.description.clone()),
        impls,
        metadata: metadata_from_aliases(function.aliases()),
        name: function.name().to_string(),
    })
}

fn window_function(
    function: &Arc<WindowUDF>,
    overrides: &FunctionYamlOverrides,
) -> Result<ext::WindowFunction> {
    let signature = function.signature();
    let documentation = function.documentation();
    let arg_names = argument_names(signature, documentation);
    let impls = signature_to_impls(
        function.name(),
        signature,
        &arg_names,
        overrides,
        |arg_types| {
            return_type_or_any(function.name(), overrides, arg_types, || {
                window_return_type(function, arg_types)
            })
        },
    )?
    .into_iter()
    .map(window_impl)
    .collect();

    Ok(ext::WindowFunction {
        description: documentation.map(|doc| doc.description.clone()),
        impls,
        metadata: metadata_from_aliases(function.aliases()),
        name: function.name().to_string(),
    })
}

fn higher_order_function(
    function: &Arc<dyn HigherOrderUDF>,
    overrides: &FunctionYamlOverrides,
) -> Result<ext::ScalarFunction> {
    let signature = higher_order_signature(function.signature());
    let documentation = function.documentation();
    let arg_names = argument_names(&signature, documentation);
    let impls = signature_to_impls(
        function.name(),
        &signature,
        &arg_names,
        overrides,
        |_arg_types| Ok("any".to_string()),
    )?;

    Ok(ext::ScalarFunction {
        description: documentation.map(|doc| doc.description.clone()),
        impls,
        metadata: metadata_from_aliases(function.aliases()),
        name: function.name().to_string(),
    })
}

fn higher_order_signature(
    signature: &datafusion::logical_expr::HigherOrderSignature,
) -> Signature {
    let type_signature = match &signature.type_signature {
        HigherOrderTypeSignature::UserDefined => TypeSignature::UserDefined,
        HigherOrderTypeSignature::VariadicAny => TypeSignature::VariadicAny,
        HigherOrderTypeSignature::Any(count) => TypeSignature::Any(*count),
        // Both Value and Lambda are opaque at the Substrait level; represent as Null.
        HigherOrderTypeSignature::Exact(args) => TypeSignature::Exact(
            args.iter()
                .map(|arg| match arg {
                    ValueOrLambda::Value(()) | ValueOrLambda::Lambda(()) => {
                        DataType::Null
                    }
                })
                .collect(),
        ),
    };
    Signature::new(type_signature, signature.volatility)
}

fn argument_names(
    signature: &Signature,
    documentation: Option<&Documentation>,
) -> Vec<String> {
    if let Some(parameter_names) = &signature.parameter_names {
        return parameter_names.clone();
    }

    documentation
        .and_then(|doc| doc.arguments.as_ref())
        .map(|args| args.iter().map(|(name, _)| name.clone()).collect())
        .unwrap_or_default()
}

fn window_impl(
    implementation: ext::ScalarFunctionImplsItem,
) -> ext::WindowFunctionImplsItem {
    ext::WindowFunctionImplsItem {
        args: implementation.args,
        decomposable: None,
        deterministic: implementation.deterministic,
        implementation: implementation.implementation,
        intermediate: None,
        maxset: None,
        nullability: implementation.nullability,
        options: implementation.options,
        ordered: None,
        return_: implementation.return_,
        session_dependent: implementation.session_dependent,
        variadic: implementation.variadic,
        window_type: Some(ext::WindowFunctionImplsItemWindowType::Partition),
    }
}

fn window_return_type(
    function: &Arc<WindowUDF>,
    arg_types: &[DataType],
) -> Result<DataType> {
    let fields: Vec<_> = arg_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(Field::new(format!("arg_{idx}"), data_type.clone(), true))
        })
        .collect();
    let field = function.field(WindowUDFFieldArgs::new(&fields, function.name()))?;
    Ok(field.data_type().clone())
}

fn sort_and_validate_scalar_functions(
    mut functions: Vec<ext::ScalarFunction>,
) -> Result<Vec<ext::ScalarFunction>> {
    functions.sort_by(|left, right| left.name.cmp(&right.name));
    validate_unique_names(
        functions.iter().map(|function| function.name.as_str()),
        FunctionKind::Scalar,
    )?;
    Ok(functions)
}

fn sort_and_validate_aggregate_functions(
    mut functions: Vec<ext::AggregateFunction>,
) -> Result<Vec<ext::AggregateFunction>> {
    functions.sort_by(|left, right| left.name.cmp(&right.name));
    validate_unique_names(
        functions.iter().map(|function| function.name.as_str()),
        FunctionKind::Aggregate,
    )?;
    Ok(functions)
}

fn sort_and_validate_window_functions(
    mut functions: Vec<ext::WindowFunction>,
) -> Result<Vec<ext::WindowFunction>> {
    functions.sort_by(|left, right| left.name.cmp(&right.name));
    validate_unique_names(
        functions.iter().map(|function| function.name.as_str()),
        FunctionKind::Window,
    )?;
    Ok(functions)
}

fn validate_unique_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
    kind: FunctionKind,
) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return internal_err!("duplicate {} function `{}`", kind.yaml_key(), name);
        }
    }
    Ok(())
}
