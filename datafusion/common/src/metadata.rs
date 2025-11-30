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

use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::{DataType, Field};
use hashbrown::HashMap;

use crate::{DataFusionError, ScalarValue, error::_plan_err};

/// A [`ScalarValue`] with optional [`FieldMetadata`]
#[derive(Debug, Clone)]
pub struct ScalarAndMetadata {
    pub value: ScalarValue,
    pub metadata: Option<FieldMetadata>,
}

impl ScalarAndMetadata {
    /// Create a new Literal from a scalar value with optional [`FieldMetadata`]
    pub fn new(value: ScalarValue, metadata: Option<FieldMetadata>) -> Self {
        Self { value, metadata }
    }

    /// Access the underlying [ScalarValue] storage
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    /// Access the [FieldMetadata] attached to this value, if any
    pub fn metadata(&self) -> Option<&FieldMetadata> {
        self.metadata.as_ref()
    }

    /// Consume self and return components
    pub fn into_inner(self) -> (ScalarValue, Option<FieldMetadata>) {
        (self.value, self.metadata)
    }

    /// Cast this values's storage type
    ///
    /// This operation assumes that if the underlying [ScalarValue] can be casted
    /// to a given type that any extension type represented by the metadata is also
    /// valid.
    pub fn cast_storage_to(
        &self,
        target_type: &DataType,
    ) -> Result<Self, DataFusionError> {
        let new_value = self.value().cast_to(target_type)?;
        Ok(Self::new(new_value, self.metadata.clone()))
    }
}

/// create a new ScalarAndMetadata from a ScalarValue without
/// any metadata
impl From<ScalarValue> for ScalarAndMetadata {
    fn from(value: ScalarValue) -> Self {
        Self::new(value, None)
    }
}

/// Assert equality of data types where one or both sides may have field metadata
///
/// This currently compares absent metadata (e.g., one side was a DataType) and
/// empty metadata (e.g., one side was a field where the field had no metadata)
/// as equal and uses byte-for-byte comparison for the keys and values of the
/// fields, even though this is potentially too strict for some cases (e.g.,
/// extension types where extension metadata is represented by JSON, or cases
/// where field metadata is orthogonal to the interpretation of the data type).
///
/// Returns a planning error with suitably formatted type representations if
/// actual and expected do not compare to equal.
pub fn check_metadata_with_storage_equal(
    actual: (
        &DataType,
        Option<&std::collections::HashMap<String, String>>,
    ),
    expected: (
        &DataType,
        Option<&std::collections::HashMap<String, String>>,
    ),
    what: &str,
    context: &str,
) -> Result<(), DataFusionError> {
    if actual.0 != expected.0 {
        return _plan_err!(
            "Expected {what} of type {}, got {}{context}",
            format_type_and_metadata(expected.0, expected.1),
            format_type_and_metadata(actual.0, actual.1)
        );
    }

    let metadata_equal = match (actual.1, expected.1) {
        (None, None) => true,
        (None, Some(expected_metadata)) => expected_metadata.is_empty(),
        (Some(actual_metadata), None) => actual_metadata.is_empty(),
        (Some(actual_metadata), Some(expected_metadata)) => {
            actual_metadata == expected_metadata
        }
    };

    if !metadata_equal {
        return _plan_err!(
            "Expected {what} of type {}, got {}{context}",
            format_type_and_metadata(expected.0, expected.1),
            format_type_and_metadata(actual.0, actual.1)
        );
    }

    Ok(())
}

/// Given a data type represented by storage and optional metadata, generate
/// a user-facing string
///
/// This function exists to reduce the number of Field debug strings that are
/// used to communicate type information in error messages and plan explain
/// renderings.
pub fn format_type_and_metadata(
    data_type: &DataType,
    metadata: Option<&std::collections::HashMap<String, String>>,
) -> String {
    match metadata {
        Some(metadata) if !metadata.is_empty() => {
            format!("{data_type}<{metadata:?}>")
        }
        _ => data_type.to_string(),
    }
}

/// Literal metadata
///
/// Stores metadata associated with a literal expressions
/// and is designed to be fast to `clone`.
///
/// This structure is used to store metadata associated with a literal expression, and it
/// corresponds to the `metadata` field on [`Field`].
///
/// # Example: Create [`FieldMetadata`] from a [`Field`]
/// ```
/// # use std::collections::HashMap;
/// # use datafusion_common::metadata::FieldMetadata;
/// # use arrow::datatypes::{Field, DataType};
/// # let field = Field::new("c1", DataType::Int32, true)
/// #  .with_metadata(HashMap::from([("foo".to_string(), "bar".to_string())]));
/// // Create a new `FieldMetadata` instance from a `Field`
/// let metadata = FieldMetadata::new_from_field(&field);
/// // There is also a `From` impl:
/// let metadata = FieldMetadata::from(&field);
/// ```
///
/// # Example: Update a [`Field`] with [`FieldMetadata`]
/// ```
/// # use datafusion_common::metadata::FieldMetadata;
/// # use arrow::datatypes::{Field, DataType};
/// # let field = Field::new("c1", DataType::Int32, true);
/// # let metadata = FieldMetadata::new_from_field(&field);
/// // Add any metadata from `FieldMetadata` to `Field`
/// let updated_field = metadata.add_to_field(field);
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct FieldMetadata {
    /// The inner metadata of a literal expression, which is a map of string
    /// keys to string values.
    ///
    /// Note this is not a `HashMap` because `HashMap` does not provide
    /// implementations for traits like `Debug` and `Hash`.
    inner: Arc<BTreeMap<String, String>>,
}

impl Default for FieldMetadata {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl FieldMetadata {
    /// Create a new empty metadata instance.
    pub fn new_empty() -> Self {
        Self {
            inner: Arc::new(BTreeMap::new()),
        }
    }

    /// Merges two optional `FieldMetadata` instances, overwriting any existing
    /// keys in `m` with keys from `n` if present.
    ///
    /// This function is commonly used in alias operations, particularly for literals
    /// with metadata. When creating an alias expression, the metadata from the original
    /// expression (such as a literal) is combined with any metadata specified on the alias.
    ///
    /// # Arguments
    ///
    /// * `m` - The first metadata (typically from the original expression like a literal)
    /// * `n` - The second metadata (typically from the alias definition)
    ///
    /// # Merge Strategy
    ///
    /// - If both metadata instances exist, they are merged with `n` taking precedence
    /// - Keys from `n` will overwrite keys from `m` if they have the same name
    /// - If only one metadata instance exists, it is returned unchanged
    /// - If neither exists, `None` is returned
    ///
    /// # Example usage
    /// ```rust
    /// use datafusion_common::metadata::FieldMetadata;
    /// use std::collections::BTreeMap;
    ///
    /// // Create metadata for a literal expression
    /// let literal_metadata = Some(FieldMetadata::from(BTreeMap::from([
    ///     ("source".to_string(), "constant".to_string()),
    ///     ("type".to_string(), "int".to_string()),
    /// ])));
    ///
    /// // Create metadata for an alias
    /// let alias_metadata = Some(FieldMetadata::from(BTreeMap::from([
    ///     ("description".to_string(), "answer".to_string()),
    ///     ("source".to_string(), "user".to_string()), // This will override literal's "source"
    /// ])));
    ///
    /// // Merge the metadata
    /// let merged = FieldMetadata::merge_options(
    ///     literal_metadata.as_ref(),
    ///     alias_metadata.as_ref(),
    /// );
    ///
    /// // Result contains: {"source": "user", "type": "int", "description": "answer"}
    /// assert!(merged.is_some());
    /// ```
    pub fn merge_options(
        m: Option<&FieldMetadata>,
        n: Option<&FieldMetadata>,
    ) -> Option<FieldMetadata> {
        match (m, n) {
            (Some(m), Some(n)) => {
                let mut merged = m.clone();
                merged.extend(n.clone());
                Some(merged)
            }
            (Some(m), None) => Some(m.clone()),
            (None, Some(n)) => Some(n.clone()),
            (None, None) => None,
        }
    }

    /// Create a new metadata instance from a `Field`'s metadata.
    pub fn new_from_field(field: &Field) -> Self {
        let inner = field
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new metadata instance from a map of string keys to string values.
    pub fn new(inner: BTreeMap<String, String>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Get the inner metadata as a reference to a `BTreeMap`.
    pub fn inner(&self) -> &BTreeMap<String, String> {
        &self.inner
    }

    /// Return the inner metadata
    pub fn into_inner(self) -> Arc<BTreeMap<String, String>> {
        self.inner
    }

    /// Adds metadata from `other` into `self`, overwriting any existing keys.
    pub fn extend(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        let other = Arc::unwrap_or_clone(other.into_inner());
        Arc::make_mut(&mut self.inner).extend(other);
    }

    /// Returns true if the metadata is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of key-value pairs in the metadata.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Convert this `FieldMetadata` into a `HashMap<String, String>`
    pub fn to_hashmap(&self) -> std::collections::HashMap<String, String> {
        self.inner
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Updates the metadata on the Field with this metadata, if it is not empty.
    pub fn add_to_field(&self, field: Field) -> Field {
        if self.inner.is_empty() {
            return field;
        }

        field.with_metadata(self.to_hashmap())
    }
}

impl From<&Field> for FieldMetadata {
    fn from(field: &Field) -> Self {
        Self::new_from_field(field)
    }
}

impl From<BTreeMap<String, String>> for FieldMetadata {
    fn from(inner: BTreeMap<String, String>) -> Self {
        Self::new(inner)
    }
}

impl From<std::collections::HashMap<String, String>> for FieldMetadata {
    fn from(map: std::collections::HashMap<String, String>) -> Self {
        Self::new(map.into_iter().collect())
    }
}

/// From reference
impl From<&std::collections::HashMap<String, String>> for FieldMetadata {
    fn from(map: &std::collections::HashMap<String, String>) -> Self {
        let inner = map
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self::new(inner)
    }
}

/// From hashbrown map
impl From<HashMap<String, String>> for FieldMetadata {
    fn from(map: HashMap<String, String>) -> Self {
        let inner = map.into_iter().collect();
        Self::new(inner)
    }
}

impl From<&HashMap<String, String>> for FieldMetadata {
    fn from(map: &HashMap<String, String>) -> Self {
        let inner = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self::new(inner)
    }
}
