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

use crate::types::{
    LogicalType, NativeType, TypeParameter, TypeSignature, ValuePrettyPrinter,
};
use crate::Result;
use crate::ScalarValue;
use std::sync::{Arc, LazyLock};

/// Represents the canonical [UUID extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid).
pub struct UuidType {}

impl UuidType {
    /// Creates a new [UuidType].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for UuidType {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalType for UuidType {
    fn native(&self) -> &NativeType {
        &NativeType::FixedSizeBinary(16)
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Extension {
            name: "arrow.uuid",
            parameters: vec![],
        }
    }

    fn pretty_printer(&self) -> &Arc<dyn ValuePrettyPrinter> {
        static PRETTY_PRINTER: LazyLock<Arc<dyn ValuePrettyPrinter>> =
            LazyLock::new(|| Arc::new(UuidValuePrettyPrinter {}));
        &PRETTY_PRINTER
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
struct UuidValuePrettyPrinter;

impl ValuePrettyPrinter for UuidValuePrettyPrinter {
    fn pretty_print_scalar(&self, value: &ScalarValue) -> Result<String> {
        Ok(format!("arrow.uuid({})", value))
    }
}

/// Represents the canonical [Opaque extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#opaque).
///
/// In the context of DataFusion, a common use case of the opaque type is when an extension type
/// is unknown to DataFusion. Contrary to [UnresolvedExtensionType], the extension type has
/// already been checked against the extension type registry and was not found.  
pub struct OpaqueType {
    /// The underlying native type.
    native_type: NativeType,
}

impl OpaqueType {
    /// Creates a new [OpaqueType].
    pub fn new(native_type: NativeType) -> Self {
        Self { native_type }
    }
}

impl LogicalType for OpaqueType {
    fn native(&self) -> &NativeType {
        &NativeType::FixedSizeBinary(16)
    }

    fn signature(&self) -> TypeSignature<'_> {
        let parameter = TypeParameter::Type(TypeSignature::Native(&self.native_type));
        TypeSignature::Extension {
            name: "arrow.opaque",
            parameters: vec![parameter],
        }
    }

    fn pretty_printer(&self) -> &Arc<dyn ValuePrettyPrinter> {
        static PRETTY_PRINTER: LazyLock<Arc<dyn ValuePrettyPrinter>> =
            LazyLock::new(|| Arc::new(OpaqueValuePrettyPrinter {}));
        &PRETTY_PRINTER
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
struct OpaqueValuePrettyPrinter;

impl ValuePrettyPrinter for OpaqueValuePrettyPrinter {
    fn pretty_print_scalar(&self, value: &ScalarValue) -> Result<String> {
        Ok(format!("arrow.opaque({})", value))
    }
}

/// Represents an unresolved extension type with a given native type and name.
///
/// This does not necessarily indicate that DataFusion does not understand the extension type. For
/// this purpose, see [OpaqueType]. However, it does indicate that the extension type was not yet
/// checked against the extension type registry.
///
/// This extension type exists because it is often challenging to gain access to an extension type
/// registry. Especially because extension type support is relatively new, and therefore this
/// consideration was not taken into account by users. This provides a workaround such that
/// unresolved extension types can be resolved at a later point in time where access to the registry
/// is available.
pub struct UnresolvedExtensionType {
    /// The name of the underlying extension type.
    name: String,
    /// The metadata of the underlying extension type.
    metadata: Option<String>,
    /// The underlying native type.
    native_type: NativeType,
}

impl UnresolvedExtensionType {
    /// Creates a new [UnresolvedExtensionType].
    pub fn new(name: String, metadata: Option<String>, native_type: NativeType) -> Self {
        Self {
            name,
            metadata,
            native_type,
        }
    }

    /// The name of the unresolved extension type.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The metadata of the unresolved extension type.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }
}

impl LogicalType for UnresolvedExtensionType {
    fn native(&self) -> &NativeType {
        &self.native_type
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Extension {
            name: &"datafusion.unresolved",
            parameters: vec![],
        }
    }

    fn pretty_printer(&self) -> &Arc<dyn ValuePrettyPrinter> {
        static PRETTY_PRINTER: LazyLock<Arc<dyn ValuePrettyPrinter>> =
            LazyLock::new(|| Arc::new(UnresolvedValuePrettyPrinter {}));
        &PRETTY_PRINTER
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
struct UnresolvedValuePrettyPrinter {}

impl ValuePrettyPrinter for UnresolvedValuePrettyPrinter {
    fn pretty_print_scalar(&self, value: &ScalarValue) -> Result<String> {
        Ok(format!("datafusion.unresolved({})", value))
    }
}
