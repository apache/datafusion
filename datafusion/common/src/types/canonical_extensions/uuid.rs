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
#[derive(Debug)]
pub struct UuidDFExtensionType();

impl UuidDFExtensionType {
    /// Create a new instance of [`UuidDFExtensionType`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for UuidDFExtensionType {
    fn default() -> Self {
        Self::new()
    }
}

impl DFExtensionType for UuidDFExtensionType {
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
            write!(f, "arrow.uuid({})", self.null_str)?;
            return Ok(());
        }

        let bytes = Bytes::try_from(self.array.value(idx))
            .expect("FixedSizeBinaryArray length checked in create_array_formatter");
        let uuid = Uuid::from_bytes(bytes);
        write!(f, "arrow.uuid({uuid})")?;
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

        let extension_type = UuidDFExtensionType::new();
        let formatter = extension_type
            .create_array_formatter(uuid.as_ref(), &FormatOptions::default())
            .unwrap()
            .unwrap();

        assert_eq!(
            formatter.value(0).to_string(),
            "arrow.uuid(00000000-0000-0000-0000-000000000000)"
        );
    }
}
