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

use crate::Result;
use crate::cast::{as_fixed_size_binary_array, as_string_array};
use crate::error::_internal_err;
use crate::types::CastExtension;
use crate::types::extension::DFExtensionType;
use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, StringBuilder, builder::FixedSizeBinaryBuilder,
};
use arrow::compute::{CastOptions, cast};
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use arrow_schema::Field;
use arrow_schema::extension::{ExtensionType, Uuid};
use std::fmt::Write;
use std::sync::Arc;
use uuid::Bytes;

/// Defines the extension type logic for the canonical `arrow.uuid` extension type. This extension
/// type defines that a field should be interpreted as a
/// [UUID](https://de.wikipedia.org/wiki/Universally_Unique_Identifier).
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism. See also
/// [`Uuid`] for the implementation of arrow-rs, which this type uses internally.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid>
#[derive(Debug, Clone)]
pub struct DFUuid(Uuid);

impl DFUuid {
    /// Creates a new [`DFUuid`], validating that the storage type is compatible with the
    /// extension type.
    pub fn try_new(
        data_type: &DataType,
        metadata: <Uuid as ExtensionType>::Metadata,
    ) -> Result<Self> {
        Ok(Self(<Uuid as ExtensionType>::try_new(data_type, metadata)?))
    }
}

impl DFExtensionType for DFUuid {
    fn storage_type(&self) -> DataType {
        DataType::FixedSizeBinary(16)
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.0.serialize_metadata()
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
            null_str: options.null(),
        };
        Ok(Some(ArrayFormatter::new(
            Box::new(display_index),
            options.safe(),
        )))
    }

    fn create_cast_extension(
        &self,
        other: &Field,
    ) -> Result<Option<Arc<dyn CastExtension>>> {
        if other.extension_type_name().is_some() {
            return Ok(None);
        }

        match other.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                Ok(Some(Arc::new(UuidCastExtension {})))
            }
            _ => Ok(None),
        }
    }
}

/// Pretty printer for binary UUID values.
#[derive(Debug, Clone, Copy)]
struct UuidValueDisplayIndex<'a> {
    array: &'a FixedSizeBinaryArray,
    null_str: &'a str,
}

impl DisplayIndex for UuidValueDisplayIndex<'_> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            write!(f, "{}", self.null_str)?;
            return Ok(());
        }

        let bytes = Bytes::try_from(self.array.value(idx))
            .expect("FixedSizeBinaryArray length checked in create_array_formatter");
        let uuid = uuid::Uuid::from_bytes(bytes);
        write!(f, "{uuid}")?;
        Ok(())
    }
}

#[derive(Debug)]
struct UuidCastExtension {}

impl CastExtension for UuidCastExtension {
    fn can_cast(&self, to: &Field, options: CastOptions<'static>) -> Result<bool> {
        if to.extension_type_name().is_some() {
            return Ok(false);
        }

        match to.data_type() {
            // Only explicit casts to string
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                if options.safe {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            // Can implicitly cast to storage
            DataType::FixedSizeBinary(16) => Ok(true),
            _ => Ok(false),
        }
    }

    fn can_cast_from(&self, from: &Field, options: CastOptions<'static>) -> Result<bool> {
        // Symmetric behaviour between cast from and cast to
        self.can_cast(from, options)
    }

    fn cast(
        &self,
        value: ArrayRef,
        to: &Field,
        options: CastOptions<'static>,
    ) -> Result<ArrayRef> {
        if !self.can_cast(to, options)? {
            return _internal_err!("Unhandled cast");
        }

        let storage = as_fixed_size_binary_array(&value)?;
        match to.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let mut builder =
                    StringBuilder::with_capacity(storage.len(), storage.len() * 36);
                for bytes_opt in storage {
                    match bytes_opt {
                        Some(bytes) => {
                            let bytes16 = Bytes::try_from(bytes).map_err(|e| {
                                crate::DataFusionError::Execution(e.to_string())
                            })?;
                            let uuid = uuid::Uuid::from_bytes(bytes16);
                            write!(builder, "{uuid}")?;
                            builder.append_value("");
                        }
                        None => builder.append_null(),
                    }
                }

                let string_array = Arc::new(builder.finish()) as ArrayRef;
                return Ok(cast(&string_array, to.data_type())?);
            }
            DataType::FixedSizeBinary(16) => return Ok(value),
            _ => {}
        }

        _internal_err!("Unexpected difference between can_cast()")
    }

    fn cast_from(
        &self,
        value: ArrayRef,
        from: &Field,
        options: CastOptions<'static>,
    ) -> Result<ArrayRef> {
        if !self.can_cast_from(from, options)? {
            return _internal_err!("Unhandled cast");
        }

        match from.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let string_array_ref = cast(&value, &DataType::Utf8)?;
                let string_array = as_string_array(&string_array_ref)?;
                let mut builder = FixedSizeBinaryBuilder::new(16);
                for string_opt in string_array {
                    match string_opt {
                        Some(string) => {
                            let uuid = uuid::Uuid::try_parse(string).map_err(|_| {
                                crate::DataFusionError::Execution(format!(
                                    "Failed to parsed string '{string}' as UUID"
                                ))
                            })?;
                            builder.append_value(uuid.as_bytes())?;
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            // Can implicitly cast from storage
            DataType::FixedSizeBinary(16) => return Ok(value),
            _ => {}
        }

        _internal_err!("Unexpected difference between can_cast_from()")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScalarValue;
    use arrow_schema::ArrowError;

    #[test]
    pub fn test_pretty_print_uuid() -> Result<(), ArrowError> {
        let my_uuid = uuid::Uuid::nil();
        let uuid = ScalarValue::FixedSizeBinary(16, Some(my_uuid.as_bytes().to_vec()))
            .to_array_of_size(1)?;

        let formatter = DFUuid::try_new(uuid.data_type(), ())?
            .create_array_formatter(uuid.as_ref(), &FormatOptions::default())?
            .unwrap();

        assert_eq!(
            formatter.value(0).to_string(),
            "00000000-0000-0000-0000-000000000000"
        );

        Ok(())
    }
}
