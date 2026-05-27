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
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, HashSet, Result, internal_err};
use datafusion::logical_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, Coercion, Signature, TypeSignature,
};
use substrait::text::simple_extensions as ext;

use super::types::{
    arrow_type_to_substrait, type_class_to_substrait, volatility_to_substrait,
};

pub fn signature_to_impls<F>(
    function_name: &str,
    signature: &Signature,
    arg_names: &[String],
    overrides: &FunctionYamlOverrides,
    return_type: F,
) -> Result<Vec<ext::ScalarFunctionImplsItem>>
where
    F: Fn(&[DataType]) -> Result<String>,
{
    let mut impls = type_signature_to_impls(
        function_name,
        &signature.type_signature,
        arg_names,
        overrides,
        &return_type,
    )?;

    let (deterministic, session_dependent) =
        volatility_to_substrait(&signature.volatility);
    for implementation in &mut impls {
        implementation.deterministic = Some(ext::Deterministic(deterministic));
        implementation.session_dependent = Some(ext::SessionDependent(session_dependent));
    }

    // Dedup collapses impls whose Arrow types map to the same Substrait type
    // (e.g. Utf8/LargeUtf8/Utf8View all become "string").
    deduplicate_impls(&mut impls);
    Ok(impls)
}

pub fn return_type_or_any<F>(
    function_name: &str,
    overrides: &FunctionYamlOverrides,
    arg_types: &[DataType],
    f: F,
) -> Result<String>
where
    F: FnOnce() -> Result<DataType>,
{
    // When arg types are not yet resolved (Null placeholders), emit "any" rather
    // than calling the return-type closure, which would likely fail or mislead.
    if arg_types
        .iter()
        .any(|data_type| data_type == &DataType::Null)
    {
        return Ok("any".to_string());
    }

    match f().and_then(|data_type| arrow_type_to_substrait(&data_type)) {
        Ok(data_type) => Ok(data_type),
        Err(_) => overrides
            .return_types
            .get(function_name)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "cannot infer Substrait return type for `{function_name}`"
                ))
            }),
    }
}

pub fn arg_yaml(idx: usize, arg_names: &[String], value: String) -> ext::ArgumentsItem {
    // Names that are not YAML plain-safe are dropped rather than quoted,
    // because quoted scalar keys interact poorly with some Substrait parsers.
    ext::ValueArg {
        constant: None,
        description: None,
        name: arg_names
            .get(idx)
            .cloned()
            .filter(|name| is_yaml_plain_safe(name)),
        value: ext::Type::String(value),
    }
    .into()
}

pub fn function_impl(
    args: Vec<ext::ArgumentsItem>,
    variadic: Option<ext::VariadicBehavior>,
    return_type: String,
) -> ext::ScalarFunctionImplsItem {
    ext::ScalarFunctionImplsItem {
        args: (!args.is_empty()).then_some(ext::Arguments(args)),
        deterministic: None,
        implementation: None,
        nullability: Some(ext::NullabilityHandling::Mirror),
        options: None,
        return_: ext::ReturnValue(ext::Type::String(return_type)),
        session_dependent: None,
        variadic,
    }
}

#[cfg(test)]
pub fn detect_duplicate_impls(
    function_name: &str,
    impls: &[ext::ScalarFunctionImplsItem],
) -> Result<()> {
    let mut signatures = HashSet::new();
    for implementation in impls {
        let key = signature_key(implementation);
        if !signatures.insert(key) {
            return internal_err!(
                "duplicate Substrait implementation signature for `{function_name}`"
            );
        }
    }
    Ok(())
}

pub fn type_name(value: &ext::Type) -> &str {
    match value {
        ext::Type::String(value) => value,
        ext::Type::Object(_) => "object",
    }
}

fn type_signature_to_impls<F>(
    function_name: &str,
    signature: &TypeSignature,
    arg_names: &[String],
    overrides: &FunctionYamlOverrides,
    return_type: &F,
) -> Result<Vec<ext::ScalarFunctionImplsItem>>
where
    F: Fn(&[DataType]) -> Result<String>,
{
    match signature {
        TypeSignature::Nullary => {
            Ok(vec![function_impl(vec![], None, return_type(&[])?)])
        }
        TypeSignature::Exact(types) => Ok(vec![function_impl(
            typed_args(types, arg_names)?,
            None,
            return_type(types)?,
        )]),
        TypeSignature::Uniform(count, types) => types
            .iter()
            .map(|data_type| {
                let arg_types = vec![data_type.clone(); *count];
                let return_type = return_type(&arg_types)
                    .or_else(|_| arrow_type_to_substrait(data_type))?;
                Ok(function_impl(
                    typed_args(&arg_types, arg_names)?,
                    None,
                    return_type,
                ))
            })
            .collect(),
        TypeSignature::OneOf(signatures) => signatures
            .iter()
            .map(|signature| {
                type_signature_to_impls(
                    function_name,
                    signature,
                    arg_names,
                    overrides,
                    return_type,
                )
            })
            .try_flatten_vec(),
        TypeSignature::Variadic(types) => types
            .iter()
            .map(|data_type| {
                Ok(function_impl(
                    vec![arg_yaml(0, arg_names, arrow_type_to_substrait(data_type)?)],
                    Some(variadic(
                        1,
                        ext::VariadicBehaviorParameterConsistency::Consistent,
                    )),
                    return_type(std::slice::from_ref(data_type))?,
                ))
            })
            .collect(),
        TypeSignature::VariadicAny => Ok(vec![function_impl(
            vec![arg_yaml(0, arg_names, "any".to_string())],
            Some(variadic(
                1,
                ext::VariadicBehaviorParameterConsistency::Inconsistent,
            )),
            "any".to_string(),
        )]),
        TypeSignature::Any(count) => Ok(vec![function_impl(
            (0..*count)
                .map(|idx| arg_yaml(idx, arg_names, "any".to_string()))
                .collect(),
            None,
            "any".to_string(),
        )]),
        TypeSignature::Comparable(count) => Ok(vec![function_impl(
            (0..*count)
                .map(|idx| arg_yaml(idx, arg_names, "any1".to_string()))
                .collect(),
            None,
            "any1".to_string(),
        )]),
        TypeSignature::Numeric(count) => {
            let types = [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
                DataType::Float32,
                DataType::Float64,
            ];
            uniform_impls(&types, *count, arg_names, return_type)
        }
        TypeSignature::String(count) => {
            let types = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
            uniform_impls(&types, *count, arg_names, return_type)
        }
        TypeSignature::Coercible(coercions) => Ok(vec![function_impl(
            coercion_args(coercions, arg_names)?,
            None,
            "any".to_string(),
        )]),
        TypeSignature::ArraySignature(array_signature) => Ok(vec![function_impl(
            array_signature_args(array_signature, arg_names),
            None,
            array_signature_return(function_name),
        )]),
        TypeSignature::UserDefined => {
            user_defined_override(function_name, arg_names, overrides)
        }
    }
}

fn uniform_impls<F>(
    types: &[DataType],
    count: usize,
    arg_names: &[String],
    return_type: &F,
) -> Result<Vec<ext::ScalarFunctionImplsItem>>
where
    F: Fn(&[DataType]) -> Result<String>,
{
    types
        .iter()
        .map(|data_type| {
            let arg_types = vec![data_type.clone(); count];
            let return_type = return_type(&arg_types)
                .or_else(|_| arrow_type_to_substrait(data_type))?;
            Ok(function_impl(
                typed_args(&arg_types, arg_names)?,
                None,
                return_type,
            ))
        })
        .collect()
}

fn typed_args(
    types: &[DataType],
    arg_names: &[String],
) -> Result<Vec<ext::ArgumentsItem>> {
    types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            Ok(arg_yaml(
                idx,
                arg_names,
                arrow_type_to_substrait(data_type)?,
            ))
        })
        .collect()
}

fn coercion_args(
    coercions: &[Coercion],
    arg_names: &[String],
) -> Result<Vec<ext::ArgumentsItem>> {
    coercions
        .iter()
        .enumerate()
        .map(|(idx, coercion)| {
            let value = match coercion {
                Coercion::Exact { desired_type }
                | Coercion::Implicit { desired_type, .. } => {
                    type_class_to_substrait(desired_type)?
                }
            };
            Ok(arg_yaml(idx, arg_names, value))
        })
        .collect()
}

fn array_signature_args(
    signature: &ArrayFunctionSignature,
    arg_names: &[String],
) -> Vec<ext::ArgumentsItem> {
    match signature {
        ArrayFunctionSignature::Array { arguments, .. } => arguments
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                let value = match arg {
                    ArrayFunctionArgument::Element => "any1",
                    ArrayFunctionArgument::Index => "i64",
                    ArrayFunctionArgument::Array => "list<any1>",
                    ArrayFunctionArgument::String => "string",
                };
                arg_yaml(idx, arg_names, value.to_string())
            })
            .collect(),
        ArrayFunctionSignature::RecursiveArray => {
            vec![arg_yaml(0, arg_names, "list<any1>".to_string())]
        }
        ArrayFunctionSignature::MapArray => {
            vec![arg_yaml(0, arg_names, "map<any1,any2>".to_string())]
        }
    }
}

fn array_signature_return(function_name: &str) -> String {
    match function_name {
        "array_length" | "cardinality" => "i64".to_string(),
        "array_empty" | "empty" => "boolean".to_string(),
        "array_has" | "array_has_all" | "array_has_any" | "array_any_match"
        | "array_all_match" => "boolean".to_string(),
        "array_element" | "array_pop_front" | "array_pop_back" => "any1".to_string(),
        _ => "list<any1>".to_string(),
    }
}

fn user_defined_override(
    function_name: &str,
    arg_names: &[String],
    overrides: &FunctionYamlOverrides,
) -> Result<Vec<ext::ScalarFunctionImplsItem>> {
    let Some(override_signatures) = overrides.user_defined_signatures.get(function_name)
    else {
        return internal_err!(
            "no Substrait function YAML override for UserDefined signature `{function_name}`"
        );
    };

    override_signatures
        .iter()
        .map(|override_signature| {
            Ok(function_impl(
                override_signature
                    .args
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| arg_yaml(idx, arg_names, value.clone()))
                    .collect(),
                override_signature
                    .variadic
                    .as_ref()
                    .map(|variadic_override| {
                        variadic(
                            variadic_override.min,
                            ext::VariadicBehaviorParameterConsistency::from(
                                variadic_override.parameter_consistency,
                            ),
                        )
                    }),
                override_signature.return_type.clone(),
            ))
        })
        .collect()
}

fn variadic(
    min: usize,
    parameter_consistency: ext::VariadicBehaviorParameterConsistency,
) -> ext::VariadicBehavior {
    ext::VariadicBehavior {
        max: None,
        min: Some(min as f64),
        parameter_consistency: Some(parameter_consistency),
    }
}

fn deduplicate_impls(impls: &mut Vec<ext::ScalarFunctionImplsItem>) {
    let mut seen = HashSet::new();
    impls.retain(|implementation| seen.insert(signature_key(implementation)));
}

fn signature_key(
    implementation: &ext::ScalarFunctionImplsItem,
) -> (
    Vec<(Option<String>, String)>,
    Option<(Option<u64>, Option<String>)>,
    String,
) {
    (
        implementation
            .args
            .as_ref()
            .map(|args| args.iter().map(argument_key).collect())
            .unwrap_or_default(),
        implementation.variadic.as_ref().map(|variadic| {
            (
                variadic.min.map(f64::to_bits),
                variadic
                    .parameter_consistency
                    .map(|consistency| consistency.to_string()),
            )
        }),
        type_name(&implementation.return_.0).to_string(),
    )
}

fn argument_key(argument: &ext::ArgumentsItem) -> (Option<String>, String) {
    match argument {
        ext::ArgumentsItem::ValueArg(arg) => {
            (arg.name.clone(), type_name(&arg.value).to_string())
        }
        ext::ArgumentsItem::EnumerationArg(arg) => {
            (arg.name.clone(), format!("{:?}", arg.options))
        }
        ext::ArgumentsItem::TypeArg(arg) => (arg.name.clone(), arg.type_.clone()),
    }
}

fn is_yaml_plain_safe(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '<' | '>' | ',' | '?')
        })
        && !matches!(value, "true" | "false" | "null" | "TRUE" | "FALSE" | "NULL")
}

trait TryFlattenVec<T> {
    fn try_flatten_vec(self) -> Result<Vec<T>>;
}

impl<I, T> TryFlattenVec<T> for I
where
    I: Iterator<Item = Result<Vec<T>>>,
{
    fn try_flatten_vec(self) -> Result<Vec<T>> {
        let mut out = vec![];
        for item in self {
            out.extend(item?);
        }
        Ok(out)
    }
}
