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
use crate::types::extension::DFExtensionType;
use arrow::datatypes::DataType;
use arrow_schema::extension::{ExtensionType, VariableShapeTensor};

/// Defines the extension type logic for the canonical `arrow.variable_shape_tensor` extension type.
/// This extension type can be used to store a [tensor](https://en.wikipedia.org/wiki/Tensor) with
/// variable shape that can change for each element.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism. See also
/// [`VariableShapeTensor`] for the implementation of arrow-rs, which this type uses internally.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor>
#[derive(Debug, Clone)]
pub struct DFVariableShapeTensor {
    inner: VariableShapeTensor,
    /// While we could reconstruct the storage type from the inner [`VariableShapeTensor`], we may
    /// choose a different name for the field within the [`DataType::List`] which can cause problems
    /// down the line (e.g., checking for equality).
    storage_type: DataType,
}

impl DFVariableShapeTensor {
    /// Creates a new [`DFVariableShapeTensor`], validating that the storage type is compatible with
    /// the extension type.
    pub fn try_new(
        data_type: &DataType,
        metadata: <VariableShapeTensor as ExtensionType>::Metadata,
    ) -> Result<Self> {
        Ok(Self {
            inner: <VariableShapeTensor as ExtensionType>::try_new(data_type, metadata)?,
            storage_type: data_type.clone(),
        })
    }
}

impl DFExtensionType for DFVariableShapeTensor {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.inner.serialize_metadata()
    }
}
