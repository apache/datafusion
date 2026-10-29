// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::registry::ExtensionTypeRegistry;
use arrow_schema::Field;

/// Returns the name of the type of `field` shown by schema introspection, such as `DESCRIBE`,
/// `SHOW COLUMNS` and `information_schema.columns`.
///
/// If the field has a registered extension type that provides a
/// [`display_name`](datafusion_common::types::DFExtensionType::display_name), that name is
/// returned. Otherwise, the storage type is returned. The storage type is also returned if the
/// extension type is not registered or cannot be created from the metadata of the field, so
/// that the schema of such a table can still be inspected.
pub fn field_type_display_name(
    registry: &dyn ExtensionTypeRegistry,
    field: &Field,
) -> String {
    registry
        .create_extension_type_for_field(field)
        .ok()
        .flatten()
        .and_then(|extension_type| extension_type.display_name())
        .unwrap_or_else(|| field.data_type().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::MemoryExtensionTypeRegistry;
    use arrow_schema::DataType;
    use arrow_schema::extension::{EXTENSION_TYPE_NAME_KEY, Uuid};
    use std::collections::HashMap;

    #[test]
    fn test_field_type_display_name() {
        let registry = MemoryExtensionTypeRegistry::new_with_canonical_extension_types();

        let uuid = Field::new("u", DataType::FixedSizeBinary(16), true)
            .with_extension_type(Uuid);
        assert_eq!(field_type_display_name(&registry, &uuid), "UUID");

        let plain = Field::new("p", DataType::FixedSizeBinary(16), true);
        assert_eq!(
            field_type_display_name(&registry, &plain),
            "FixedSizeBinary(16)"
        );

        let unregistered = Field::new("x", DataType::Int32, true).with_metadata(
            HashMap::from([(EXTENSION_TYPE_NAME_KEY.to_owned(), "example.x".to_owned())]),
        );
        assert_eq!(field_type_display_name(&registry, &unregistered), "Int32");

        // `arrow.uuid` with a storage type that the extension type does not support
        let invalid =
            Field::new("i", DataType::Int32, true).with_metadata(HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                "arrow.uuid".to_owned(),
            )]));
        assert_eq!(field_type_display_name(&registry, &invalid), "Int32");

        let empty_registry = MemoryExtensionTypeRegistry::new_empty();
        assert_eq!(
            field_type_display_name(&empty_registry, &uuid),
            "FixedSizeBinary(16)"
        );
    }
}
