// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::Result;
use crate::types::extension::DFExtensionType;
use arrow::datatypes::DataType;
use arrow_schema::extension::{ExtensionType, FixedShapeTensor};

/// Defines the extension type logic for the canonical `arrow.fixed_shape_tensor` extension type.
/// This extension type can be used to store a [tensor](https://en.wikipedia.org/wiki/Tensor) of
/// a fixed shape.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism. See also
/// [`FixedShapeTensor`] for the implementation of arrow-rs, which this type uses internally.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor>
#[derive(Debug, Clone)]
pub struct DFFixedShapeTensor {
    inner: FixedShapeTensor,
    /// The storage type of the tensor.
    ///
    /// While we could reconstruct the storage type from the inner [`FixedShapeTensor`], we may
    /// choose a different name for the field within the [`DataType::FixedSizeList`] which can
    /// cause problems down the line (e.g., checking for equality).
    storage_type: DataType,
}

impl DFFixedShapeTensor {
    /// Creates a new [`DFFixedShapeTensor`], validating that the storage type is compatible with
    /// the extension type.
    pub fn try_new(
        data_type: &DataType,
        metadata: <FixedShapeTensor as ExtensionType>::Metadata,
    ) -> Result<Self> {
        Ok(Self {
            inner: <FixedShapeTensor as ExtensionType>::try_new(data_type, metadata)?,
            storage_type: data_type.clone(),
        })
    }
}

impl DFExtensionType for DFFixedShapeTensor {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.inner.serialize_metadata()
    }
}
