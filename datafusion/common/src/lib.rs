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
// https://github.com/apache/datafusion/issues/18503
#![deny(clippy::needless_pass_by_value)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

mod column;
mod dfschema;
mod functional_dependencies;
mod join_type;
mod param_value;
mod schema_reference;
mod table_reference;
mod unnest;

pub mod alias;
pub mod cast;
pub mod config;
pub mod cse;
pub mod datatype;
pub mod diagnostic;
pub mod display;
pub mod encryption;
pub mod error;
pub mod file_options;
pub mod format;
pub mod hash_utils;
pub mod instant;
pub mod metadata;
pub mod nested_struct;
mod null_equality;
pub mod parsers;
pub mod pruning;
pub mod rounding;
pub mod scalar;
pub mod spans;
pub mod stats;
pub mod test_util;
pub mod tree_node;
pub mod types;
pub mod utils;

/// Reexport arrow crate
pub use arrow;
pub use column::Column;
pub use dfschema::{
    DFSchema, DFSchemaRef, ExprSchema, SchemaExt, ToDFSchema, qualified_name,
};
pub use diagnostic::Diagnostic;
pub use error::{
    DataFusionError, Result, SchemaError, SharedResult, field_not_found,
    unqualified_field_not_found,
};
pub use file_options::file_type::{
    DEFAULT_ARROW_EXTENSION, DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION,
    DEFAULT_JSON_EXTENSION, DEFAULT_PARQUET_EXTENSION, GetExt,
};
pub use functional_dependencies::{
    Constraint, Constraints, Dependency, FunctionalDependence, FunctionalDependencies,
    aggregate_functional_dependencies, get_required_group_by_exprs_indices,
    get_target_functional_dependencies,
};
use hashbrown::hash_map::DefaultHashBuilder;
pub use join_type::{JoinConstraint, JoinSide, JoinType};
pub use nested_struct::cast_column;
pub use null_equality::NullEquality;
pub use param_value::ParamValues;
pub use scalar::{ScalarType, ScalarValue};
pub use schema_reference::SchemaReference;
pub use spans::{Location, Span, Spans};
pub use stats::{ColumnStatistics, Statistics};
pub use table_reference::{ResolvedTableReference, TableReference};
pub use unnest::{RecursionUnnestOption, UnnestOptions};
pub use utils::project_schema;

// These are hidden from docs purely to avoid polluting the public view of what this crate exports.
// These are just re-exports of macros by the same name, which gets around the 'cannot refer to
// macro-expanded macro_export macros by their full path' error.
// The design to get around this comes from this comment:
// https://github.com/rust-lang/rust/pull/52234#issuecomment-976702997
#[doc(hidden)]
pub use error::{
    _config_datafusion_err, _exec_datafusion_err, _internal_datafusion_err,
    _not_impl_datafusion_err, _plan_datafusion_err, _resources_datafusion_err,
    _substrait_datafusion_err,
};

// The HashMap and HashSet implementations that should be used as the uniform defaults
pub type HashMap<K, V, S = DefaultHashBuilder> = hashbrown::HashMap<K, V, S>;
pub type HashSet<T, S = DefaultHashBuilder> = hashbrown::HashSet<T, S>;
pub mod hash_map {
    pub use hashbrown::hash_map::Entry;
}
pub mod hash_set {
    pub use hashbrown::hash_set::Entry;
}

/// Downcast an Arrow Array to a concrete type, return an `DataFusionError::Internal` if the cast is
/// not possible. In normal usage of DataFusion the downcast should always succeed.
///
/// Example: `let array = downcast_value!(values, Int32Array)`
#[macro_export]
macro_rules! downcast_value {
    ($Value: expr, $Type: ident) => {{
        use $crate::__private::DowncastArrayHelper;
        $Value.downcast_array_helper::<$Type>()?
    }};
    ($Value: expr, $Type: ident, $T: tt) => {{
        use $crate::__private::DowncastArrayHelper;
        $Value.downcast_array_helper::<$Type<$T>>()?
    }};
}

// Not public API.
#[doc(hidden)]
pub mod __private {
    use crate::Result;
    use crate::error::_internal_datafusion_err;
    use arrow::array::Array;
    use std::any::{Any, type_name};

    #[doc(hidden)]
    pub trait DowncastArrayHelper {
        fn downcast_array_helper<U: Any>(&self) -> Result<&U>;
    }

    impl<T: Array + ?Sized> DowncastArrayHelper for T {
        fn downcast_array_helper<U: Any>(&self) -> Result<&U> {
            self.as_any().downcast_ref().ok_or_else(|| {
                let actual_type = self.data_type();
                let desired_type_name = type_name::<U>();
                _internal_datafusion_err!(
                    "could not cast array of type {} to {}",
                    actual_type,
                    desired_type_name
                )
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int32Array, UInt64Array};
    use std::any::{type_name, type_name_of_val};
    use std::sync::Arc;

    #[test]
    fn test_downcast_value() -> crate::Result<()> {
        let boxed: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array = downcast_value!(&boxed, Int32Array);
        assert_eq!(type_name_of_val(&array), type_name::<&Int32Array>());

        let expected: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();
        assert_eq!(array, &expected);
        Ok(())
    }

    #[test]
    fn test_downcast_value_err_message() {
        let boxed: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let error: crate::DataFusionError = (|| {
            downcast_value!(&boxed, UInt64Array);
            Ok(())
        })()
        .err()
        .unwrap();

        assert_starts_with(
            error.to_string(),
            "Internal error: could not cast array of type Int32 to arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::UInt64Type>",
        );
    }

    // `err.to_string()` depends on backtrace being present (may have backtrace appended)
    // `err.strip_backtrace()` also depends on backtrace being present (may have "This was likely caused by ..." stripped)
    fn assert_starts_with(actual: impl AsRef<str>, expected_prefix: impl AsRef<str>) {
        let actual = actual.as_ref();
        let expected_prefix = expected_prefix.as_ref();
        assert!(
            actual.starts_with(expected_prefix),
            "Expected '{actual}' to start with '{expected_prefix}'"
        );
    }
}
