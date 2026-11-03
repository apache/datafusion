// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::Result;
use crate::types::extension::DFExtensionType;
use arrow::datatypes::DataType;
use arrow_schema::extension::{ExtensionType, Json};

/// Defines the extension type logic for the canonical `arrow.json` extension type. This extension
/// type defines that a particular string field stores JSON values.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism. See also
/// [`Json`] for the implementation of arrow-rs, which this type uses internally.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#json>
#[derive(Debug, Clone)]
pub struct DFJson {
    inner: Json,
    storage_type: DataType,
}

impl DFJson {
    /// Creates a new [`DFJson`], validating that the storage type is compatible with the
    /// extension type.
    pub fn try_new(
        data_type: &DataType,
        metadata: <Json as ExtensionType>::Metadata,
    ) -> Result<Self> {
        Ok(Self {
            inner: <Json as ExtensionType>::try_new(data_type, metadata)?,
            storage_type: data_type.clone(),
        })
    }
}

impl DFExtensionType for DFJson {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.inner.serialize_metadata()
    }
}
