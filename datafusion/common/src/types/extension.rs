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

use crate::error::Result;
use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use std::fmt::Debug;
use std::sync::Arc;

/// A cheaply cloneable pointer to a [`DFExtensionType`].
pub type DFExtensionTypeRef = Arc<dyn DFExtensionType>;

/// Represents an implementation of a DataFusion extension type.
///
/// This allows users to customize the behavior of DataFusion for certain types. Having this ability
/// is necessary because extension types affect how columns should be treated by the query engine.
/// This effect includes which operations are possible on a column and what are the expected results
/// from these operations. The extension type mechanism allows users to define how these operations
/// apply to a particular extension type.
///
/// For example, adding two values of [`Int64`](arrow::datatypes::DataType::Int64) is a sensible
/// thing to do. However, if the same column is annotated with an extension type like `custom.id`,
/// the correct interpretation of a column changes. Adding together two `custom.id` values, even
/// though they are stored as integers, may no longer make sense.
///
/// Note that DataFusion's extension type support is still young and therefore might not cover all
/// relevant use cases. Currently, the following operations can be customized:
/// - Pretty-printing values in record batches
///
/// # Relation to Arrow's `ExtensionType`
///
/// The purpose of Arrow's `ExtensionType` trait, for the time being, is to allow reading and
/// writing extension type metadata in a type-safe manner. The trait does not provide any
/// customization options. Therefore, downstream users (such as DataFusion) have the flexibility to
/// implement the extension type mechanism according to their needs. [`DFExtensionType`] is
/// DataFusion's implementation of this extension type mechanism.
///
/// Furthermore, the current trait in arrow-rs is not dyn-compatible, which we need for implementing
/// extension type registries. In the future, the two implementations may increasingly converge.
///
/// # Examples
///
/// Examples for using the extension type machinery can be found in the DataFusion examples
/// directory.
pub trait DFExtensionType: Debug + Send + Sync {
    /// Returns an [`ArrayFormatter`] that can format values of this type.
    ///
    /// If `Ok(None)` is returned, the default implementation will be used.
    /// If an error is returned, there was an error creating the formatter.
    fn create_array_formatter<'fmt>(
        &self,
        _array: &'fmt dyn Array,
        _options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        Ok(None)
    }
}
