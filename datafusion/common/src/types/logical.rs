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

use super::NativeType;
use crate::error::Result;
use arrow::datatypes::DataType;
use core::fmt;
use std::{cmp::Ordering, hash::Hash, sync::Arc};

/// Signature that uniquely identifies a type among other types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeSignature<'a> {
    /// Represents a built-in native type.
    Native(&'a NativeType),
    /// Represents an arrow-compatible extension type.
    /// (<https://arrow.apache.org/docs/format/Columnar.html#extension-types>)
    ///
    /// The `name` should contain the same value as 'ARROW:extension:name'.
    Extension {
        name: &'a str,
        parameters: &'a [TypeParameter<'a>],
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeParameter<'a> {
    Type(TypeSignature<'a>),
    Number(i128),
}

/// A reference counted [`LogicalType`].
pub type LogicalTypeRef = Arc<dyn LogicalType>;

/// Representation of a logical type with its signature and its native backing
/// type.
///
/// The logical type is meant to be used during the DataFusion logical planning
/// phase in order to reason about logical types without worrying about their
/// underlying physical implementation.
///
/// ### Extension types
///
/// [`LogicalType`] is a trait in order to allow the possibility of declaring
/// extension types:
///
/// ```
/// use datafusion_common::types::{LogicalType, NativeType, TypeSignature};
///
/// struct JSON {}
///
/// impl LogicalType for JSON {
///     fn native(&self) -> &NativeType {
///         &NativeType::String
///     }
///
///     fn signature(&self) -> TypeSignature<'_> {
///         TypeSignature::Extension {
///             name: "JSON",
///             parameters: &[],
///         }
///     }
/// }
/// ```
pub trait LogicalType: Sync + Send {
    /// Get the native backing type of this logical type.
    fn native(&self) -> &NativeType;
    /// Get the unique type signature for this logical type. Logical types with identical
    /// signatures are considered equal.
    fn signature(&self) -> TypeSignature<'_>;

    /// Get the default physical type to cast `origin` to in order to obtain a physical type
    /// that is logically compatible with this logical type.
    fn default_cast_for(&self, origin: &DataType) -> Result<DataType> {
        self.native().default_cast_for(origin)
    }
}

impl fmt::Debug for dyn LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LogicalType")
            .field(&self.signature())
            .field(&self.native())
            .finish()
    }
}

impl std::fmt::Display for dyn LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.signature() {
            TypeSignature::Native(_) => write!(f, "{}", self.native()),
            TypeSignature::Extension { name, .. } => write!(f, "{name}"),
        }
    }
}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
        // Logical types with identical signatures are considered equal.
        self.signature().eq(&other.signature())
    }
}

impl Eq for dyn LogicalType {}

impl PartialOrd for dyn LogicalType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn LogicalType {
    fn cmp(&self, other: &Self) -> Ordering {
        // Logical types with identical signatures are considered equal.
        self.signature().cmp(&other.signature())
    }
}

impl Hash for dyn LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Logical types with identical signatures are considered equal.
        self.signature().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        LogicalField, LogicalFields, logical_boolean, logical_date, logical_float32,
        logical_float64, logical_int32, logical_int64, logical_null, logical_string,
    };
    use arrow::datatypes::{Field, Fields};
    use insta::assert_snapshot;

    #[test]
    fn test_logical_type_display_simple() {
        assert_snapshot!(logical_null(), @"Null");
        assert_snapshot!(logical_boolean(), @"Boolean");
        assert_snapshot!(logical_int32(), @"Int32");
        assert_snapshot!(logical_int64(), @"Int64");
        assert_snapshot!(logical_float32(), @"Float32");
        assert_snapshot!(logical_float64(), @"Float64");
        assert_snapshot!(logical_string(), @"String");
        assert_snapshot!(logical_date(), @"Date");
    }

    #[test]
    fn test_logical_type_display_list() {
        let list_type: Arc<dyn LogicalType> = Arc::new(NativeType::List(Arc::new(
            LogicalField::from(&Field::new("item", DataType::Int32, true)),
        )));
        assert_snapshot!(list_type, @"List(Int32)");
    }

    #[test]
    fn test_logical_type_display_struct() {
        let struct_type: Arc<dyn LogicalType> = Arc::new(NativeType::Struct(
            LogicalFields::from(&Fields::from(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, true),
            ])),
        ));
        assert_snapshot!(struct_type, @r#"Struct("x": non-null Float64, "y": Float64)"#);
    }

    #[test]
    fn test_logical_type_display_fixed_size_list() {
        let fsl_type: Arc<dyn LogicalType> = Arc::new(NativeType::FixedSizeList(
            Arc::new(LogicalField::from(&Field::new(
                "item",
                DataType::Float32,
                false,
            ))),
            3,
        ));
        assert_snapshot!(fsl_type, @"FixedSizeList(3 x non-null Float32)");
    }

    #[test]
    fn test_logical_type_display_map() {
        let map_type: Arc<dyn LogicalType> = Arc::new(NativeType::Map(Arc::new(
            LogicalField::from(&Field::new("entries", DataType::Utf8, false)),
        )));
        assert_snapshot!(map_type, @"Map(non-null String)");
    }

    #[test]
    fn test_logical_type_display_union() {
        use arrow::datatypes::UnionFields;

        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new("str_val", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let union_type: Arc<dyn LogicalType> = Arc::new(NativeType::Union(
            crate::types::LogicalUnionFields::from(&union_fields),
        ));
        assert_snapshot!(union_type, @r#"Union(0: ("int_val": non-null Int32), 1: ("str_val": String))"#);
    }

    #[test]
    fn test_logical_type_display_nullable_vs_non_nullable() {
        let nullable_list: Arc<dyn LogicalType> = Arc::new(NativeType::List(Arc::new(
            LogicalField::from(&Field::new("item", DataType::Int32, true)),
        )));
        let non_nullable_list: Arc<dyn LogicalType> =
            Arc::new(NativeType::List(Arc::new(LogicalField::from(&Field::new(
                "item",
                DataType::Int32,
                false,
            )))));

        assert_snapshot!(nullable_list, @"List(Int32)");
        assert_snapshot!(non_nullable_list, @"List(non-null Int32)");
    }

    #[test]
    fn test_logical_type_display_extension() {
        struct JsonType;
        impl LogicalType for JsonType {
            fn native(&self) -> &NativeType {
                &NativeType::String
            }
            fn signature(&self) -> TypeSignature<'_> {
                TypeSignature::Extension {
                    name: "JSON",
                    parameters: &[],
                }
            }
        }
        let json: Arc<dyn LogicalType> = Arc::new(JsonType);
        assert_snapshot!(json, @"JSON");
    }
}
