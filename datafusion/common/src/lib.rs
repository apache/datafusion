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

pub mod alias;
pub mod cast;
mod column;
pub mod config;
mod dfschema;
pub mod display;
mod error;
pub mod file_options;
pub mod format;
mod functional_dependencies;
pub mod hash_utils;
mod join_type;
pub mod parsers;
#[cfg(feature = "pyarrow")]
mod pyarrow;
pub mod scalar;
mod schema_reference;
pub mod stats;
mod table_reference;
pub mod test_util;
pub mod tree_node;
mod unnest;
pub mod utils;

pub use column::Column;
pub use dfschema::{DFField, DFSchema, DFSchemaRef, ExprSchema, SchemaExt, ToDFSchema};
pub use error::{
    field_not_found, unqualified_field_not_found, DataFusionError, Result, SchemaError,
    SharedResult,
};

pub use file_options::file_type::{
    FileType, GetExt, DEFAULT_ARROW_EXTENSION, DEFAULT_AVRO_EXTENSION,
    DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION, DEFAULT_PARQUET_EXTENSION,
};
pub use file_options::FileTypeWriterOptions;
pub use functional_dependencies::{
    aggregate_functional_dependencies, get_target_functional_dependencies, Constraints,
    Dependency, FunctionalDependence, FunctionalDependencies,
};
pub use join_type::{JoinConstraint, JoinSide, JoinType};
pub use scalar::{ScalarType, ScalarValue};
pub use schema_reference::{OwnedSchemaReference, SchemaReference};
pub use stats::{ColumnStatistics, Statistics};
pub use table_reference::{OwnedTableReference, ResolvedTableReference, TableReference};
pub use unnest::UnnestOptions;
pub use utils::project_schema;

/// Reexport arrow crate
pub use arrow;

/// Downcast an Arrow Array to a concrete type, return an `DataFusionError::Internal` if the cast is
/// not possible. In normal usage of DataFusion the downcast should always succeed.
///
/// Example: `let array = downcast_value!(values, Int32Array)`
#[macro_export]
macro_rules! downcast_value {
    ($Value: expr, $Type: ident) => {{
        use std::any::type_name;
        $Value.as_any().downcast_ref::<$Type>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast value to {}",
                type_name::<$Type>()
            ))
        })?
    }};
    ($Value: expr, $Type: ident, $T: tt) => {{
        use std::any::type_name;
        $Value.as_any().downcast_ref::<$Type<$T>>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast value to {}",
                type_name::<$Type<$T>>()
            ))
        })?
    }};
}
