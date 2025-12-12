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
use crate::types::{LogicalType, NativeType, TypeParameter, TypeSignature};
use crate::Result;
use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use arrow_schema::extension::{ExtensionType, Opaque, Uuid};
use arrow_schema::DataType;
use std::fmt::Write;
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

    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        if array.data_type() != &DataType::FixedSizeBinary(16) {
            return _internal_err!("Wrong array type for Uuid");
        }

        let display_index = UuidValueDisplayIndex {
            array: array.as_any().downcast_ref().unwrap(),
        };
        Ok(Some(ArrayFormatter::new(
            Box::new(display_index),
            options.safe(),
        )))
    }
}

/// Pretty printer for binary UUID values.
#[derive(Debug, Clone, Copy)]
struct UuidValueDisplayIndex<'arr> {
    array: &'arr FixedSizeBinaryArray,
}

impl DisplayIndex for UuidValueDisplayIndex<'_> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            write!(f, "arrow.uuid(NULL)")?;
            return Ok(());
        }

        let bytes = Bytes::try_from(self.array.value(idx))
            .expect("FixedSizeBinaryArray length checked in create_array_formatter");
        let uuid = uuid::Uuid::from_bytes(bytes);
        write!(f, "arrow.uuid({uuid})")?;
        Ok(())
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
}

// TODO Other canonical extension types.

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
            name: "datafusion.unresolved",
            parameters: vec![inner_type],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScalarValue;

    #[test]
    pub fn test_pretty_print_uuid() {
        let my_uuid = uuid::Uuid::nil();
        let uuid = ScalarValue::FixedSizeBinary(16, Some(my_uuid.as_bytes().to_vec()))
            .to_array_of_size(1)
            .unwrap();

        let type_instance = Uuid::try_new(uuid.data_type(), ()).unwrap();
        let formatter = type_instance
            .create_array_formatter(uuid.as_ref(), &FormatOptions::default())
            .unwrap()
            .unwrap();

        assert_eq!(
            formatter.value(0).to_string(),
            "arrow.uuid(00000000-0000-0000-0000-000000000000)"
        );
    }
}
