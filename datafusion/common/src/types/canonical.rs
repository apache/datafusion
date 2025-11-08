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

use crate::error::_internal_err;
use crate::types::{
    LogicalType, NativeType, TypeParameter, TypeSignature, ValuePrettyPrinter,
};
use crate::ScalarValue;
use crate::{Result, _internal_datafusion_err};
use arrow_schema::extension::{ExtensionType, Opaque, Uuid};
use std::sync::{Arc, LazyLock};
use uuid::Bytes;

impl LogicalType for Uuid {
    fn native(&self) -> &NativeType {
        &NativeType::FixedSizeBinary(16)
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Extension {
            name: Uuid::NAME,
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
        match value {
            ScalarValue::FixedSizeBinary(16, value) => match value {
                Some(value) => {
                    let bytes = Bytes::try_from(value.as_slice()).map_err(|_| {
                        _internal_datafusion_err!(
                            "Invalid UUID bytes even though type is correct."
                        )
                    })?;
                    let uuid = uuid::Uuid::from_bytes(bytes);
                    Ok(format!("arrow.uuid({})", uuid))
                }
                None => Ok("arrow.uuid(NULL)".to_owned()),
            },
            _ => _internal_err!("Wrong scalar given to "),
        }
    }
}

/// Represents the canonical [Opaque extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#opaque).
///
/// In the context of DataFusion, a common use case of the opaque type is when an extension type
/// is unknown to DataFusion. Contrary to [UnresolvedExtensionType], the extension type has
/// already been checked against the extension type registry and was not found.
impl LogicalType for Opaque {
    fn native(&self) -> &NativeType {
        &NativeType::FixedSizeBinary(16)
    }

    fn signature(&self) -> TypeSignature<'_> {
        let parameter = TypeParameter::Type(TypeSignature::Extension {
            name: self.metadata().type_name(),
            parameters: vec![],
        });
        TypeSignature::Extension {
            name: Opaque::NAME,
            parameters: vec![parameter],
        }
    }

    fn pretty_printer(&self) -> &Arc<dyn ValuePrettyPrinter> {
        static PRETTY_PRINTER: LazyLock<Arc<dyn ValuePrettyPrinter>> =
            LazyLock::new(|| Arc::new(OpaqueValuePrettyPrinter {}));
        &PRETTY_PRINTER
    }
}

// TODO Other canonical extension types.

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
        let inner_type = TypeParameter::Type(TypeSignature::Extension {
            name: &self.name,
            parameters: vec![],
        });
        TypeSignature::Extension {
            name: &"datafusion.unresolved",
            parameters: vec![inner_type],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_pretty_print_uuid() {
        let my_uuid = uuid::Uuid::nil();
        let uuid = ScalarValue::FixedSizeBinary(16, Some(my_uuid.as_bytes().to_vec()));

        let printer = UuidValuePrettyPrinter::default();
        let pretty_printed = printer.pretty_print_scalar(&uuid).unwrap();
        assert_eq!(
            pretty_printed,
            "arrow.uuid(00000000-0000-0000-0000-000000000000)"
        );
    }
}
