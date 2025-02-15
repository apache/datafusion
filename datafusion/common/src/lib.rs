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

// Make cheap clones clear: https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

mod column;
mod dfschema;
mod functional_dependencies;
mod join_type;
mod param_value;
#[cfg(feature = "pyarrow")]
mod pyarrow;
mod schema_reference;
mod table_reference;
mod unnest;

pub mod alias;
pub mod cast;
pub mod config;
pub mod cse;
pub mod diagnostic;
pub mod display;
pub mod error;
pub mod file_options;
pub mod format;
pub mod hash_utils;
pub mod instant;
pub mod parsers;
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
    qualified_name, DFSchema, DFSchemaRef, ExprSchema, SchemaExt, ToDFSchema,
};
pub use diagnostic::Diagnostic;
pub use error::{
    field_not_found, unqualified_field_not_found, DataFusionError, Result, SchemaError,
    SharedResult,
};
pub use file_options::file_type::{
    GetExt, DEFAULT_ARROW_EXTENSION, DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION,
    DEFAULT_JSON_EXTENSION, DEFAULT_PARQUET_EXTENSION,
};
pub use functional_dependencies::{
    aggregate_functional_dependencies, get_required_group_by_exprs_indices,
    get_target_functional_dependencies, Constraint, Constraints, Dependency,
    FunctionalDependence, FunctionalDependencies,
};
use hashbrown::hash_map::DefaultHashBuilder;
pub use join_type::{JoinConstraint, JoinSide, JoinType};
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
    use super::DataFusionError;
    use super::Result;
    use arrow::array::Array;
    use std::any::{type_name, Any};

    #[doc(hidden)]
    pub trait DowncastArrayHelper {
        fn downcast_array_helper<U: Any>(&self) -> Result<&U>;
    }

    impl<T: Array + ?Sized> DowncastArrayHelper for T {
        fn downcast_array_helper<U: Any>(&self) -> Result<&U> {
            self.as_any().downcast_ref().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast array of type {} to {}",
                    self.data_type(),
                    type_name::<U>()
                ))
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

        assert_eq!(
            error.to_string(),
            "Internal error: could not cast array of type Int32 to arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::UInt64Type>.\n\
            This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"
        );
    }
}
