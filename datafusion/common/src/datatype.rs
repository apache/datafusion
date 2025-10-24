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

//! [DataTypeExt] extension trait for converting DataTypes to Fields

use crate::{
    arrow::datatypes::{DataType, Field, FieldRef},
    metadata::FieldMetadata,
};
use std::{fmt::Display, sync::Arc};

/// DataFusion extension methods for Arrow [`DataType`]
pub trait DataTypeExt {
    /// Convert the type to field with nullable type and "" name
    ///
    /// This is used to track the places where we convert a [`DataType`]
    /// into a nameless field to interact with an API that is
    /// capable of representing an extension type and/or nullability.
    fn into_nullable_field(self) -> Field;

    /// Convert the type to field ref with nullable type and "" name
    ///
    /// Concise wrapper around [`DataTypeExt::into_nullable_field`] that
    /// constructs a [`FieldRef`].
    fn into_nullable_field_ref(self) -> FieldRef;
}

impl DataTypeExt for DataType {
    fn into_nullable_field(self) -> Field {
        Field::new("", self, true)
    }

    fn into_nullable_field_ref(self) -> FieldRef {
        Arc::new(Field::new("", self, true))
    }
}

/// DataFusion extension methods for Arrow [`Field`]
pub trait FieldExt {
    /// Returns a new Field representing a List of this Field's DataType.
    fn into_list(self) -> Self;

    /// Return a new Field representing this Field as the item type of a FixedSizeList
    fn into_fixed_size_list(self, list_size: i32) -> Self;

    /// Create a field with the default list field name ("item")
    ///
    /// Note that lists are allowed to have an arbitrarily named field;
    /// however, a name other than 'item' will cause it to fail an
    /// == check against a more idiomatically created list in
    /// arrow-rs which causes issues.
    fn into_list_item(self) -> Self;
}

impl FieldExt for Field {
    fn into_list(self) -> Self {
        DataType::List(Arc::new(self.into_list_item())).into_nullable_field()
    }

    fn into_fixed_size_list(self, list_size: i32) -> Self {
        DataType::FixedSizeList(self.into_list_item().into(), list_size)
            .into_nullable_field()
    }

    fn into_list_item(self) -> Self {
        if self.name() != Field::LIST_FIELD_DEFAULT_NAME {
            self.with_name(Field::LIST_FIELD_DEFAULT_NAME)
        } else {
            self
        }
    }
}

impl FieldExt for Arc<Field> {
    fn into_list(self) -> Self {
        DataType::List(self.into_list_item())
            .into_nullable_field()
            .into()
    }

    fn into_fixed_size_list(self, list_size: i32) -> Self {
        DataType::FixedSizeList(self.into_list_item(), list_size)
            .into_nullable_field()
            .into()
    }

    fn into_list_item(self) -> Self {
        if self.name() != Field::LIST_FIELD_DEFAULT_NAME {
            Arc::unwrap_or_clone(self)
                .with_name(Field::LIST_FIELD_DEFAULT_NAME)
                .into()
        } else {
            self
        }
    }
}

#[derive(Debug, Clone, Copy, Eq)]
pub struct SerializedTypeView<'a, 'b, 'c> {
    arrow_type: &'a DataType,
    extension_name: Option<&'b str>,
    extension_metadata: Option<&'c str>,
}

impl<'a, 'b, 'c> SerializedTypeView<'a, 'b, 'c> {
    pub fn new(
        arrow_type: &'a DataType,
        extension_name: Option<&'b str>,
        extension_metadata: Option<&'c str>,
    ) -> Self {
        Self {
            arrow_type,
            extension_name,
            extension_metadata,
        }
    }

    pub fn from_type_and_metadata(
        arrow_type: &'a DataType,
        metadata: impl IntoIterator<Item = (&'b String, &'b String)>,
    ) -> Self
    where
        'b: 'c,
    {
        let mut extension_name = None;
        let mut extension_metadata = None;
        for (k, v) in metadata {
            match k.as_str() {
                "ARROW:extension:name" => extension_name.replace(v.as_str()),
                "ARROW:extension:metadata" => extension_metadata.replace(v.as_str()),
                _ => None,
            };
        }

        Self::new(arrow_type, extension_name, extension_metadata)
    }

    pub fn data_type(&self) -> Option<&DataType> {
        if self.extension_name.is_some() {
            None
        } else {
            Some(self.arrow_type)
        }
    }

    pub fn arrow_type(&self) -> &DataType {
        self.arrow_type
    }

    pub fn extension_name(&self) -> Option<&str> {
        self.extension_name
    }

    pub fn extension_metadata(&self) -> Option<&str> {
        if let Some(metadata) = self.extension_metadata {
            if !metadata.is_empty() {
                return Some(metadata);
            }
        }

        None
    }

    pub fn to_field(&self) -> Field {
        if let Some(extension_name) = self.extension_name() {
            self.arrow_type.clone().into_nullable_field().with_metadata(
                [
                    (
                        "ARROW:extension:name".to_string(),
                        extension_name.to_string(),
                    ),
                    (
                        "ARROW:extension:metadata".to_string(),
                        self.extension_metadata().unwrap_or("").to_string(),
                    ),
                ]
                .into(),
            )
        } else {
            self.arrow_type.clone().into_nullable_field()
        }
    }

    pub fn to_field_ref(&self) -> FieldRef {
        self.to_field().into()
    }
}

impl Display for SerializedTypeView<'_, '_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.extension_name(), self.extension_metadata()) {
            (Some(name), None) => write!(f, "{}<{}>", name, self.arrow_type()),
            (Some(name), Some(metadata)) => {
                write!(f, "{}({})<{}>", name, metadata, self.arrow_type())
            }
            _ => write!(f, "{}", self.arrow_type()),
        }
    }
}

impl PartialEq<SerializedTypeView<'_, '_, '_>> for SerializedTypeView<'_, '_, '_> {
    fn eq(&self, other: &SerializedTypeView) -> bool {
        self.arrow_type() == other.arrow_type()
            && self.extension_name() == other.extension_name()
            && self.extension_metadata() == other.extension_metadata()
    }
}

impl<'a> From<&'a DataType> for SerializedTypeView<'a, 'static, 'static> {
    fn from(value: &'a DataType) -> Self {
        Self::new(value, None, None)
    }
}

impl<'a> From<&'a Field> for SerializedTypeView<'a, 'a, 'a> {
    fn from(value: &'a Field) -> Self {
        Self::from_type_and_metadata(value.data_type(), value.metadata())
    }
}

impl<'a> From<&'a FieldRef> for SerializedTypeView<'a, 'a, 'a> {
    fn from(value: &'a FieldRef) -> Self {
        Self::from_type_and_metadata(value.data_type(), value.metadata())
    }
}

impl<'a, 'b> From<(&'a DataType, &'b FieldMetadata)> for SerializedTypeView<'a, 'b, 'b> {
    fn from(value: (&'a DataType, &'b FieldMetadata)) -> Self {
        Self::from_type_and_metadata(value.0, value.1.inner())
    }
}

impl<'a, 'b> From<(&'a DataType, Option<&'b FieldMetadata>)>
    for SerializedTypeView<'a, 'b, 'b>
{
    fn from(value: (&'a DataType, Option<&'b FieldMetadata>)) -> Self {
        if let Some(metadata) = value.1 {
            Self::from_type_and_metadata(value.0, metadata.inner())
        } else {
            value.0.into()
        }
    }
}
