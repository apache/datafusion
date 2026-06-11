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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

//! Variant type Functions for [DataFusion].
//!
//! This crate contains a collection of functions that operate on the
//! [Parquet Variant] logical type. Variant values are represented as Arrow
//! `StructArray`s carrying the `VariantType` extension type, with separate
//! `metadata` and `value` binary buffers.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//! [Parquet Variant]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//!
//! You can register the functions in this crate using the [`register_all`] function.

mod impl_variant_get;
mod shared;

pub mod cast_to_variant;
pub mod is_variant_null;
pub mod json_to_variant;
pub mod variant_contains;
pub mod variant_get;
pub mod variant_list_construct;
pub mod variant_list_delete;
pub mod variant_list_insert;
pub mod variant_normalize;
pub mod variant_object_construct;
pub mod variant_object_delete;
pub mod variant_object_insert;
pub mod variant_object_keys;
pub mod variant_pretty;
pub mod variant_to_json;

pub use cast_to_variant::CastToVariantUdf;
pub use is_variant_null::IsVariantNullUdf;
pub use json_to_variant::JsonToVariantUdf;
pub use variant_contains::VariantContainsUdf;
pub use variant_get::{
    VariantGetBoolUdf, VariantGetFieldUdf, VariantGetFloatUdf, VariantGetIntUdf,
    VariantGetJsonUdf, VariantGetStrUdf, VariantGetUdf,
};
pub use variant_list_construct::VariantListConstruct;
pub use variant_list_delete::VariantListDelete;
pub use variant_list_insert::VariantListInsert;
pub use variant_normalize::VariantNormalizeUdf;
pub use variant_object_construct::VariantObjectConstruct;
pub use variant_object_delete::VariantObjectDelete;
pub use variant_object_insert::VariantObjectInsert;
pub use variant_object_keys::VariantObjectKeys;
pub use variant_pretty::VariantPretty;
pub use variant_to_json::VariantToJsonUdf;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

/// Return all default variant functions
pub fn all_default_variant_functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::from(CastToVariantUdf::default())),
        Arc::new(ScalarUDF::from(IsVariantNullUdf::default())),
        Arc::new(ScalarUDF::from(JsonToVariantUdf::default())),
        Arc::new(ScalarUDF::from(VariantContainsUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetStrUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetIntUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetFloatUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetBoolUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetJsonUdf::default())),
        Arc::new(ScalarUDF::from(VariantGetFieldUdf::default())),
        Arc::new(ScalarUDF::from(VariantListConstruct::default())),
        Arc::new(ScalarUDF::from(VariantListDelete::default())),
        Arc::new(ScalarUDF::from(VariantListInsert::default())),
        Arc::new(ScalarUDF::from(VariantNormalizeUdf::default())),
        Arc::new(ScalarUDF::from(VariantObjectConstruct::default())),
        Arc::new(ScalarUDF::from(VariantObjectDelete::default())),
        Arc::new(ScalarUDF::from(VariantObjectInsert::default())),
        Arc::new(ScalarUDF::from(VariantObjectKeys::default())),
        Arc::new(ScalarUDF::from(VariantPretty::default())),
        Arc::new(ScalarUDF::from(VariantToJsonUdf::default())),
    ]
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions = all_default_variant_functions();
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::all_default_variant_functions;
    use std::collections::HashSet;

    #[test]
    fn test_no_duplicate_name() {
        let mut names = HashSet::new();
        for udf in all_default_variant_functions() {
            assert!(
                names.insert(udf.name().to_lowercase()),
                "duplicate function name: {}",
                udf.name()
            );
            for alias in udf.aliases() {
                assert!(
                    names.insert(alias.to_lowercase()),
                    "duplicate function name: {alias}"
                );
            }
        }
    }
}
