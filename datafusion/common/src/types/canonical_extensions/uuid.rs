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
use crate::types::extension::DFExtensionType;
use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use std::fmt::Write;
use uuid::{Bytes, Uuid};

/// Defines the extension type logic for the canonical `arrow.uuid` extension type.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism.
impl DFExtensionType for arrow_schema::extension::Uuid {
    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> crate::Result<Option<ArrayFormatter<'fmt>>> {
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
        let uuid = Uuid::from_bytes(bytes);
        write!(f, "{uuid}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScalarValue;

    #[test]
    pub fn test_pretty_print_uuid() {
        let my_uuid = Uuid::nil();
        let uuid = ScalarValue::FixedSizeBinary(16, Some(my_uuid.as_bytes().to_vec()))
            .to_array_of_size(1)
            .unwrap();

        let extension_type = arrow_schema::extension::Uuid {};
        let formatter = extension_type
            .create_array_formatter(uuid.as_ref(), &FormatOptions::default())
            .unwrap()
            .unwrap();

        assert_eq!(
            formatter.value(0).to_string(),
            "00000000-0000-0000-0000-000000000000"
        );
    }
}
