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
///    fn signature(&self) -> TypeSignature<'_> {
///        TypeSignature::Extension {
///            name: "JSON",
///            parameters: &[],
///        }
///    }
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
        write!(f, "{self:?}")
    }
}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
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
        self.signature()
            .cmp(&other.signature())
            .then(self.native().cmp(other.native()))
    }
}

impl Hash for dyn LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature().hash(state);
        self.native().hash(state);
    }
}
