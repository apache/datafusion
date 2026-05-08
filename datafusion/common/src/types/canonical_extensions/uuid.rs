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
use crate::cast::as_string_array;
use crate::error::{_exec_err, _internal_err};
use crate::nested_struct::CastExtension;
use crate::types::DefaultExtensionCast;
use crate::types::extension::DFExtensionType;
use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, builder::FixedSizeBinaryBuilder,
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

    pub fn cast_extensions() -> Vec<Arc<dyn CastExtension>> {
        vec![
            Arc::new(
                DefaultExtensionCast::new(Uuid::NAME)
                    .with_default_cast_to_string(Some(Arc::new(DFUuid(Uuid)))),
            ),
            Arc::new(ParseUuid),
        ]
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
pub struct ParseUuid;

impl CastExtension for ParseUuid {
    fn can_cast_fields(&self, from: &Field, to: &Field) -> Result<bool> {
        if from.extension_type_name().is_some() {
            return Ok(false);
        }

        if let Some(to_extension_name) = to.extension_type_name()
            && to_extension_name == Uuid::NAME
        {
            Ok(matches!(
                from.data_type(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ))
        } else {
            Ok(false)
        }
    }

    fn cast_array_fields(
        &self,
        value: &ArrayRef,
        from: &Field,
        to: &Field,
        options: &CastOptions,
    ) -> Result<ArrayRef> {
        if !self.can_cast_fields(from, to)? {
            return _internal_err!("Unhandled cast");
        }

        match from.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                if options.safe {
                    return _exec_err!("Cast from UUID to string must be explicit");
                }

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

                return Ok(Arc::new(builder.finish()));
            }
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
