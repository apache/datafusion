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

use core::fmt;
use std::{cmp::Ordering, hash::Hash, sync::Arc};

use super::NativeType;

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

/// A reference counted [`LogicalType`]
pub type LogicalTypeRef = Arc<dyn LogicalType>;

pub trait LogicalType: Sync + Send {
    fn native(&self) -> &NativeType;
    fn signature(&self) -> TypeSignature<'_>;
}

impl fmt::Debug for dyn LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LogicalType")
            .field(&self.signature())
            .field(&self.native())
            .finish()
    }
}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.native().eq(other.native()) && self.signature().eq(&other.signature())
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

#[cfg(test)]
mod test {
    #![allow(dead_code)]

    use super::{LogicalType, TypeParameter, TypeSignature};
    use crate::types::NativeType;

    struct MagicalType {}

    impl LogicalType for MagicalType {
        fn native(&self) -> &NativeType {
            &NativeType::Utf8
        }

        fn signature(&self) -> TypeSignature<'_> {
            TypeSignature::Extension {
                name: "MagicalType",
                parameters: &[TypeParameter::Type(TypeSignature::Native(
                    &NativeType::Boolean,
                ))],
            }
        }
    }

    fn test(logical_type: &dyn LogicalType) {
        match logical_type.signature() {
            TypeSignature::Extension {
                name: "MagicalType",
                parameters:
                    [TypeParameter::Type(TypeSignature::Native(NativeType::Boolean))],
            } => {}
            TypeSignature::Native(NativeType::Binary) => todo!(),
            _ => unimplemented!(),
        };
    }
}
