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

//! Generate Substrait simple extension YAML declarations for DataFusion functions.
//!
//! The default generator uses the same runtime inventory as the function
//! documentation binary, so committed YAML tracks the functions registered in a
//! default [`SessionStateDefaults`]. Downstream crates can pass their own
//! [`FunctionYamlInventory`] and [`FunctionYamlConfig`] to build a Substrait
//! extension declaration for custom UDFs while reusing the same signature
//! mapping.
//!
//! The generator intentionally fails when it cannot infer a complete Substrait
//! declaration. Use [`FunctionYamlOverrides`] for signatures or return types
//! that cannot be derived from DataFusion's [`Signature`] metadata.

use datafusion::common::Result;
use datafusion::execution::SessionStateDefaults;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use std::collections::BTreeMap;
use std::sync::Arc;

mod defaults;
mod functions;
mod signatures;
#[cfg(test)]
mod tests;
mod types;

use defaults::datafusion_overrides;
use functions::{
    collect_aggregate_functions, collect_scalar_functions, collect_window_functions,
};
use substrait::text::simple_extensions as ext;

const DATAFUSION_FUNCTIONS_URN: &str = "extension:org.apache.datafusion:functions";

/// Functions to include in a generated Substrait extension declaration.
///
/// This type is public so consumers can generate Substrait extension YAML for
/// custom function registries, not only DataFusion's built-in defaults.
#[derive(Debug, Clone)]
pub struct FunctionYamlInventory {
    /// Scalar UDFs to emit into the extension file.
    pub scalar_functions: Vec<Arc<ScalarUDF>>,
    /// Higher-order UDFs to emit into the scalar function declarations.
    pub higher_order_functions: Vec<Arc<dyn HigherOrderUDF>>,
    /// Aggregate UDFs to emit into the extension file.
    pub aggregate_functions: Vec<Arc<AggregateUDF>>,
    /// Window UDFs to emit into the extension file.
    pub window_functions: Vec<Arc<WindowUDF>>,
}

impl FunctionYamlInventory {
    /// Return the function inventory registered by a default DataFusion session.
    pub fn datafusion_defaults() -> Self {
        Self {
            scalar_functions: SessionStateDefaults::default_scalar_functions(),
            higher_order_functions: SessionStateDefaults::default_higher_order_functions(
            ),
            aggregate_functions: SessionStateDefaults::default_aggregate_functions(),
            window_functions: SessionStateDefaults::default_window_functions(),
        }
    }
}

/// Configuration for generated Substrait extension declarations.
///
/// The default values produce DataFusion's built-in function declarations.
/// Custom callers can replace URNs and inference overrides while using the same
/// generation pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionYamlConfig {
    /// URN written to the extension declaration.
    pub urn: String,
    /// Explicit mappings for signatures and return types that are not inferable.
    pub overrides: FunctionYamlOverrides,
}

impl Default for FunctionYamlConfig {
    fn default() -> Self {
        Self {
            urn: DATAFUSION_FUNCTIONS_URN.to_string(),
            overrides: FunctionYamlOverrides::datafusion_defaults(),
        }
    }
}

/// Explicit mappings for function declarations that are not inferable.
///
/// DataFusion signatures generally describe argument shape and volatility, but
/// `UserDefined` signatures and some dynamic return types need explicit
/// Substrait type strings. Keys are canonical function names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FunctionYamlOverrides {
    /// Complete implementation declarations for `TypeSignature::UserDefined`.
    pub user_defined_signatures: BTreeMap<String, Vec<FunctionSignatureOverride>>,
    /// Return type fallback for functions whose return type cannot be inferred.
    pub return_types: BTreeMap<String, String>,
}

impl FunctionYamlOverrides {
    /// Return the overrides required by DataFusion's default function set.
    pub fn datafusion_defaults() -> Self {
        datafusion_overrides()
    }
}

/// Override for a single Substrait function implementation signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignatureOverride {
    /// Substrait argument type strings, in function argument order.
    pub args: Vec<String>,
    /// Substrait return type string.
    pub return_type: String,
    /// Variadic metadata when the implementation accepts repeated arguments.
    pub variadic: Option<FunctionVariadicOverride>,
}

impl FunctionSignatureOverride {
    /// Create a non-variadic implementation override.
    pub fn new(args: Vec<String>, return_type: impl Into<String>) -> Self {
        Self {
            args,
            return_type: return_type.into(),
            variadic: None,
        }
    }

    /// Create a variadic implementation override.
    pub fn variadic(
        args: Vec<String>,
        return_type: impl Into<String>,
        min: usize,
        parameter_consistency: FunctionVariadicConsistency,
    ) -> Self {
        Self {
            args,
            return_type: return_type.into(),
            variadic: Some(FunctionVariadicOverride {
                min,
                parameter_consistency,
            }),
        }
    }
}

/// Variadic metadata for a Substrait implementation override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionVariadicOverride {
    /// Minimum number of values accepted by the variadic argument.
    pub min: usize,
    /// Whether all repeated values must use a consistent type parameter.
    pub parameter_consistency: FunctionVariadicConsistency,
}

/// Substrait parameter consistency for variadic function arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVariadicConsistency {
    /// Repeated variadic arguments share the same type parameter.
    Consistent,
    /// Repeated variadic arguments can use independent types.
    Inconsistent,
}

impl From<FunctionVariadicConsistency> for ext::VariadicBehaviorParameterConsistency {
    fn from(c: FunctionVariadicConsistency) -> Self {
        match c {
            FunctionVariadicConsistency::Consistent => {
                ext::VariadicBehaviorParameterConsistency::Consistent
            }
            FunctionVariadicConsistency::Inconsistent => {
                ext::VariadicBehaviorParameterConsistency::Inconsistent
            }
        }
    }
}

/// Generate the DataFusion Substrait function extension declaration.
pub fn generate_function_extension() -> Result<ext::SimpleExtensions> {
    generate_function_extension_for_inventory(
        &FunctionYamlInventory::datafusion_defaults(),
        &FunctionYamlConfig::default(),
    )
}

/// Generate a Substrait function extension declaration for an explicit inventory.
///
/// This is the primary API for consumers that want to build declarations for a custom
/// function registry. [`generate_function_extension`] is a convenience wrapper
/// that calls this function with DataFusion's default inventory and config.
pub fn generate_function_extension_for_inventory(
    inventory: &FunctionYamlInventory,
    config: &FunctionYamlConfig,
) -> Result<ext::SimpleExtensions> {
    let scalar = collect_scalar_functions(
        &inventory.scalar_functions,
        &inventory.higher_order_functions,
        &config.overrides,
    )?;
    let aggregate =
        collect_aggregate_functions(&inventory.aggregate_functions, &config.overrides)?;
    let window =
        collect_window_functions(&inventory.window_functions, &config.overrides)?;

    Ok(ext::SimpleExtensions {
        aggregate_functions: aggregate,
        dependencies: Default::default(),
        metadata: Default::default(),
        scalar_functions: scalar,
        type_variations: vec![],
        types: vec![],
        urn: config.urn.clone(),
        window_functions: window,
    })
}
