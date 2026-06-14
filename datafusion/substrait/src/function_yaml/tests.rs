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

use super::functions::{
    aggregate_decomposable, aggregate_impl, aggregate_intermediate_type,
    metadata_from_aliases,
};
use super::signatures::{
    arg_yaml, detect_duplicate_impls, function_impl, return_type_or_any,
    signature_to_impls, type_name,
};
use super::types::{
    arrow_type_to_substrait, type_class_to_substrait, volatility_to_substrait,
};
use super::*;
use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion::common::types::logical_float64;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use std::sync::Arc;
use substrait::text::simple_extensions as ext;

fn scalar_impls<F>(
    signature: Signature,
    return_type: F,
) -> Vec<ext::ScalarFunctionImplsItem>
where
    F: Fn(&[DataType]) -> Result<String>,
{
    signature_to_impls(
        "test",
        &signature,
        &["x".to_string(), "y".to_string()],
        &FunctionYamlOverrides::default(),
        return_type,
    )
    .unwrap()
}

fn args(implementation: &ext::ScalarFunctionImplsItem) -> &[ext::ArgumentsItem] {
    implementation
        .args
        .as_ref()
        .map(|args| args.as_slice())
        .unwrap_or_default()
}

fn value_arg(argument: &ext::ArgumentsItem) -> (&Option<String>, &str) {
    match argument {
        ext::ArgumentsItem::ValueArg(arg) => (&arg.name, type_name(&arg.value)),
        _ => panic!("expected value arg"),
    }
}

fn return_type(implementation: &ext::ScalarFunctionImplsItem) -> &str {
    type_name(&implementation.return_.0)
}

#[test]
fn maps_arrow_types_to_substrait_types() {
    let cases: &[(&DataType, &str)] = &[
        (&DataType::Boolean, "boolean"),
        (&DataType::Int32, "i32"),
        (&DataType::UInt64, "u64"),
        (&DataType::Float64, "fp64"),
        (&DataType::Utf8View, "string"),
        (&DataType::BinaryView, "binary"),
        (
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            "timestamp_tz<ns>",
        ),
        (&DataType::Duration(TimeUnit::Millisecond), "duration<ms>"),
        (
            &DataType::Interval(IntervalUnit::MonthDayNano),
            "interval_month_day_nano",
        ),
        (&DataType::Decimal128(10, 3), "decimal<10,3>"),
        (
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            "list<i32>",
        ),
        (
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            "struct<i32,string>",
        ),
        (
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "string",
        ),
    ];

    for (input, expected) in cases {
        assert_eq!(
            arrow_type_to_substrait(input).unwrap(),
            *expected,
            "failed for {input:?}"
        );
    }
}

#[test]
fn maps_map_type_to_substrait() {
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    );
    assert_eq!(
        arrow_type_to_substrait(&map_type).unwrap(),
        "map<string,i64>"
    );
}

#[test]
fn maps_volatility_and_type_classes() {
    assert_eq!(
        volatility_to_substrait(&Volatility::Immutable),
        (true, false)
    );
    assert_eq!(volatility_to_substrait(&Volatility::Stable), (true, true));
    assert_eq!(
        volatility_to_substrait(&Volatility::Volatile),
        (false, false)
    );

    let cases: &[(&TypeSignatureClass, &str)] = &[
        (&TypeSignatureClass::Any, "any"),
        (&TypeSignatureClass::Timestamp, "timestamp"),
        (&TypeSignatureClass::Integer, "integer"),
        (&TypeSignatureClass::Numeric, "numeric"),
        (&TypeSignatureClass::Native(logical_float64()), "fp64"),
    ];
    for (input, expected) in cases {
        assert_eq!(type_class_to_substrait(input).unwrap(), *expected);
    }
}

#[test]
fn maps_signature_shapes_to_impls() {
    let impls = scalar_impls(Signature::nullary(Volatility::Immutable), |_| {
        Ok("i32".to_string())
    });
    assert!(impls[0].args.is_none());

    let impls = scalar_impls(
        Signature::exact(vec![DataType::Int32, DataType::Utf8], Volatility::Immutable),
        |_| Ok("boolean".to_string()),
    );
    assert_eq!(value_arg(&args(&impls[0])[0]).0.as_deref(), Some("x"));
    assert_eq!(value_arg(&args(&impls[0])[0]).1, "i32");
    assert_eq!(value_arg(&args(&impls[0])[1]).1, "string");

    let impls = scalar_impls(Signature::variadic_any(Volatility::Immutable), |_| {
        Ok("any".to_string())
    });
    assert_eq!(value_arg(&args(&impls[0])[0]).1, "any");
    assert_eq!(impls[0].variadic.as_ref().unwrap().min, Some(1.0));

    let impls = scalar_impls(Signature::any(2, Volatility::Immutable), |_| {
        Ok("any".to_string())
    });
    assert_eq!(args(&impls[0]).len(), 2);
    assert!(args(&impls[0]).iter().all(|a| value_arg(a).1 == "any"));

    let impls = scalar_impls(Signature::comparable(2, Volatility::Immutable), |_| {
        Ok("any1".to_string())
    });
    assert!(args(&impls[0]).iter().all(|a| value_arg(a).1 == "any1"));
    assert_eq!(return_type(&impls[0]), "any1");

    let impls = scalar_impls(Signature::numeric(1, Volatility::Immutable), |args| {
        arrow_type_to_substrait(&args[0])
    });
    assert_eq!(impls.len(), 10);
    assert!(impls.iter().any(|i| value_arg(&args(i)[0]).1 == "i32"));
    assert!(impls.iter().any(|i| value_arg(&args(i)[0]).1 == "fp64"));

    let impls = scalar_impls(Signature::string(1, Volatility::Immutable), |_| {
        Ok("string".to_string())
    });
    assert_eq!(impls.len(), 1);
    assert_eq!(value_arg(&args(&impls[0])[0]).1, "string");

    let impls = scalar_impls(
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
        |args| arrow_type_to_substrait(&args[0]),
    );
    assert_eq!(impls.len(), 2);
    assert_eq!(value_arg(&args(&impls[0])[0]).1, "i32");
    assert_eq!(value_arg(&args(&impls[1])[0]).1, "i64");
}

#[test]
fn uses_configured_signature_and_return_type_overrides() {
    let mut overrides = FunctionYamlOverrides::default();
    overrides.user_defined_signatures.insert(
        "custom_udf".to_string(),
        vec![FunctionSignatureOverride::variadic(
            vec!["any1".to_string()],
            "list<any1>",
            2,
            FunctionVariadicConsistency::Consistent,
        )],
    );

    let impls = signature_to_impls(
        "custom_udf",
        &Signature::user_defined(Volatility::Immutable),
        &["value".to_string()],
        &overrides,
        |_| Ok("unused".to_string()),
    )
    .unwrap();

    assert_eq!(value_arg(&args(&impls[0])[0]).0.as_deref(), Some("value"));
    assert_eq!(return_type(&impls[0]), "list<any1>");
    assert_eq!(impls[0].variadic.as_ref().unwrap().min, Some(2.0));
    assert_eq!(
        impls[0].variadic.as_ref().unwrap().parameter_consistency,
        Some(ext::VariadicBehaviorParameterConsistency::Consistent)
    );

    overrides
        .return_types
        .insert("custom_return".to_string(), "string".to_string());
    let return_type =
        return_type_or_any("custom_return", &overrides, &[DataType::Int32], || {
            Err(DataFusionError::Internal("dynamic return type".to_string()))
        })
        .unwrap();
    assert_eq!(return_type, "string");
}

#[test]
fn emits_alias_metadata_and_detects_duplicate_impls() {
    let metadata = metadata_from_aliases(&["bar".to_string()]);
    assert_eq!(metadata["datafusion"]["aliases"][0], "bar");

    let implementation = function_impl(
        vec![arg_yaml(0, &[], "i32".to_string())],
        None,
        "i32".to_string(),
    );
    let err =
        detect_duplicate_impls("duplicate", &[implementation.clone(), implementation])
            .unwrap_err();
    assert!(
        err.to_string()
            .contains("duplicate Substrait implementation")
    );
}

#[test]
fn maps_aggregate_metadata() {
    let implementation =
        aggregate_impl("sum", true, function_impl(vec![], None, "fp64".to_string()));
    assert!(!implementation.ordered.unwrap().0);
    assert_eq!(implementation.decomposable, Some(ext::Decomposable::Many));
    assert_eq!(type_name(&implementation.intermediate.unwrap().0), "fp64");
    assert_eq!(aggregate_decomposable("median"), ext::Decomposable::None);
    assert_eq!(aggregate_decomposable("sum"), ext::Decomposable::Many);
    assert_eq!(aggregate_intermediate_type("count", "i64"), "i64");
    assert_eq!(aggregate_intermediate_type("sum", "fp64"), "fp64");
}

#[test]
fn generated_extension_uses_configured_urn() {
    let extension = generate_function_extension_for_inventory(
        &FunctionYamlInventory {
            scalar_functions: vec![],
            higher_order_functions: vec![],
            aggregate_functions: vec![],
            window_functions: vec![],
        },
        &FunctionYamlConfig {
            urn: "extension:test".to_string(),
            overrides: FunctionYamlOverrides::default(),
        },
    )
    .unwrap();

    assert_eq!(extension.urn, "extension:test");
    assert!(extension.scalar_functions.is_empty());
    assert!(extension.aggregate_functions.is_empty());
    assert!(extension.window_functions.is_empty());
}
