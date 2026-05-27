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

//! DataFusion-specific generation fallbacks.
//!
//! Most function declarations are inferred from DataFusion's runtime
//! `Signature` metadata. This module is the small catalog of cases that need
//! extra information to emit a complete Substrait simple extension file.
//!
//! Keep this module focused on DataFusion's built-in functions. Custom
//! registries should pass their own [`FunctionYamlOverrides`] through
//! [`FunctionYamlConfig`](super::FunctionYamlConfig).

use super::{
    FunctionSignatureOverride, FunctionVariadicConsistency,
    FunctionVariadicConsistency::{Consistent, Inconsistent},
    FunctionYamlOverrides,
};

struct ReturnTypeDefault {
    function: &'static str,
    return_type: &'static str,
}

struct ImplementationDefault {
    function: &'static str,
    implementation: ImplementationShape,
}

enum ImplementationShape {
    Exact {
        args: &'static [&'static str],
        return_type: &'static str,
    },
    Variadic {
        args: &'static [&'static str],
        return_type: &'static str,
        min: usize,
        consistency: FunctionVariadicConsistency,
    },
}

// Dynamic return-type functions where DataFusion cannot infer a concrete Arrow
// type from the signature alone.
const RETURN_TYPE_DEFAULTS: &[ReturnTypeDefault] = &[
    ReturnTypeDefault {
        function: "arrow_typeof",
        return_type: "string",
    },
    ReturnTypeDefault {
        function: "arrow_typeof_legacy",
        return_type: "string",
    },
    ReturnTypeDefault {
        function: "from_unixtime",
        return_type: "timestamp<s>",
    },
    ReturnTypeDefault {
        function: "now",
        return_type: "timestamp<ns>",
    },
    ReturnTypeDefault {
        function: "random",
        return_type: "fp64",
    },
    ReturnTypeDefault {
        function: "version",
        return_type: "string",
    },
];

// Functions whose DataFusion signature is `UserDefined`. The generator cannot
// inspect those callbacks, so each entry records the Substrait implementation
// shape that should be emitted for DataFusion's default registry.
const IMPLEMENTATION_DEFAULTS: &[ImplementationDefault] = &[
    variadic_default("array", &["any1"], "list<any1>", 1, Consistent),
    exact_default("array_add", &["list<fp64>", "list<fp64>"], "list<fp64>"),
    variadic_default("array_concat", &["list<any1>"], "list<any1>", 1, Consistent),
    exact_default("array_distance", &["list<fp64>", "list<fp64>"], "fp64"),
    exact_default("array_normalize", &["list<fp64>"], "list<fp64>"),
    exact_default("array_scale", &["list<fp64>", "fp64"], "list<fp64>"),
    exact_default("arrow_cast", &["any", "any"], "any"),
    exact_default("arrow_try_cast", &["any", "any"], "any"),
    exact_default("cast_to_type", &["any", "any"], "any"),
    variadic_default("coalesce", &["any1"], "any1", 1, Consistent),
    exact_default("cosine_distance", &["list<fp64>", "list<fp64>"], "fp64"),
    variadic_default("get_field", &["any", "string"], "any", 1, Consistent),
    variadic_default("greatest", &["any1"], "any1", 1, Consistent),
    exact_default("inner_product", &["list<fp64>", "list<fp64>"], "fp64"),
    variadic_default("least", &["any1"], "any1", 1, Consistent),
    variadic_default("make_array", &["any"], "list<any>", 1, Inconsistent),
    exact_default("map_extract", &["map<any1,any2>", "any1"], "list<any2>"),
    exact_default("max", &["any1"], "any1"),
    exact_default("min", &["any1"], "any1"),
    variadic_default("named_struct", &["any"], "struct", 1, Inconsistent),
    exact_default("nullif", &["any1", "any1"], "any1"),
    exact_default("nvl", &["any1", "any1"], "any1"),
    exact_default("nvl2", &["any", "any1", "any1"], "any1"),
    variadic_default("struct", &["any"], "struct", 1, Inconsistent),
    exact_default("try_cast_to_type", &["any", "any"], "any"),
];

/// Return the overrides needed to generate YAML for DataFusion's default
/// function inventory.
pub fn datafusion_overrides() -> FunctionYamlOverrides {
    let mut overrides = FunctionYamlOverrides::default();

    for default in IMPLEMENTATION_DEFAULTS {
        overrides
            .user_defined_signatures
            .entry(default.function.to_string())
            .or_default()
            .push(default.implementation.to_override());
    }

    for default in RETURN_TYPE_DEFAULTS {
        overrides.return_types.insert(
            default.function.to_string(),
            default.return_type.to_string(),
        );
    }

    overrides
}

// Creates an override for one fixed implementation signature.
const fn exact_default(
    function: &'static str,
    args: &'static [&'static str],
    return_type: &'static str,
) -> ImplementationDefault {
    ImplementationDefault {
        function,
        implementation: ImplementationShape::Exact { args, return_type },
    }
}

// Creates an override for one variadic implementation signature. `args`
// describes the repeated argument pattern emitted into Substrait; `min` is the
// minimum number of accepted values for that repeated pattern.
const fn variadic_default(
    function: &'static str,
    args: &'static [&'static str],
    return_type: &'static str,
    min: usize,
    consistency: FunctionVariadicConsistency,
) -> ImplementationDefault {
    ImplementationDefault {
        function,
        implementation: ImplementationShape::Variadic {
            args,
            return_type,
            min,
            consistency,
        },
    }
}

impl ImplementationShape {
    fn to_override(&self) -> FunctionSignatureOverride {
        match self {
            Self::Exact { args, return_type } => {
                FunctionSignatureOverride::new(type_strings(args), *return_type)
            }
            Self::Variadic {
                args,
                return_type,
                min,
                consistency,
            } => FunctionSignatureOverride::variadic(
                type_strings(args),
                *return_type,
                *min,
                *consistency,
            ),
        }
    }
}

fn type_strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}
