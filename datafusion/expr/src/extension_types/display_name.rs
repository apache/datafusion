// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::registry::ExtensionTypeRegistry;
use arrow::array::StringBuilder;
use arrow_schema::Field;
use std::fmt::Write;

/// Appends the name of the type of `field` shown by schema introspection, such as `DESCRIBE`,
/// `SHOW COLUMNS` and `information_schema.columns`, to `builder`.
///
/// If the field has an extension type registered in `registry` that provides a
/// [`display_name`](datafusion_common::types::DFExtensionType::display_name), that name is
/// appended. Otherwise, the storage type is appended. The storage type is also appended if
/// there is no registry, or if the extension type is not registered or cannot be created from
/// the metadata of the field, so that the schema of such a table can still be inspected.
pub fn append_field_type_display_name(
    registry: Option<&dyn ExtensionTypeRegistry>,
    field: &Field,
    builder: &mut StringBuilder,
) {
    let extension_type = registry
        .and_then(|registry| registry.create_extension_type_for_field(field).ok())
        .flatten();
    match extension_type
        .as_ref()
        .and_then(|extension_type| extension_type.display_name())
    {
        Some(name) => builder.append_value(name),
        None => {
            // Write the storage type straight into the builder, then finish the row.
            // Writing to a `StringBuilder` does not fail.
            let _ = write!(builder, "{}", field.data_type());
            builder.append_value("");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::MemoryExtensionTypeRegistry;
    use arrow::array::Array;
    use arrow_schema::DataType;
    use arrow_schema::extension::{EXTENSION_TYPE_NAME_KEY, Uuid};
    use std::collections::HashMap;

    fn display_names(
        registry: Option<&dyn ExtensionTypeRegistry>,
        fields: &[Field],
    ) -> Vec<String> {
        let mut builder = StringBuilder::new();
        for field in fields {
            append_field_type_display_name(registry, field, &mut builder);
        }
        let array = builder.finish();
        (0..array.len())
            .map(|i| array.value(i).to_owned())
            .collect()
    }

    #[test]
    fn test_append_field_type_display_name() {
        let registry = MemoryExtensionTypeRegistry::new_with_canonical_extension_types();

        let uuid = Field::new("u", DataType::FixedSizeBinary(16), true)
            .with_extension_type(Uuid);
        let plain = Field::new("p", DataType::FixedSizeBinary(16), true);
        let unregistered = Field::new("x", DataType::Int32, true).with_metadata(
            HashMap::from([(EXTENSION_TYPE_NAME_KEY.to_owned(), "example.x".to_owned())]),
        );
        // `arrow.uuid` with a storage type that the extension type does not support
        let invalid =
            Field::new("i", DataType::Int32, true).with_metadata(HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                "arrow.uuid".to_owned(),
            )]));
        let fields = [uuid, plain, unregistered, invalid];

        assert_eq!(
            display_names(Some(&registry), &fields),
            ["UUID", "FixedSizeBinary(16)", "Int32", "Int32"]
        );

        let empty_registry = MemoryExtensionTypeRegistry::new_empty();
        assert_eq!(
            display_names(Some(&empty_registry), &fields[..1]),
            ["FixedSizeBinary(16)"]
        );
        assert_eq!(display_names(None, &fields[..1]), ["FixedSizeBinary(16)"]);
    }
}
