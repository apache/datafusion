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

//! [`DataTypeExt`] and [`FieldExt`] extension trait for working with DataTypes to Fields

use crate::arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;

/// DataFusion extension methods for Arrow [`DataType`]
pub trait DataTypeExt {
    /// Convert the type to field with nullable type and "" name
    ///
    /// This is used to track the places where we convert a [`DataType`]
    /// into a nameless field to interact with an API that is
    /// capable of representing an extension type and/or nullability.
    ///
    /// For example, it will convert a `DataType::Int32` into
    /// `Field::new("", DataType::Int32, true)`.
    ///
    /// ```
    /// # use datafusion_common::datatype::DataTypeExt;
    /// # use arrow::datatypes::DataType;
    /// let dt = DataType::Utf8;
    /// let field = dt.into_nullable_field();
    /// // result is a nullable Utf8 field with "" name
    /// assert_eq!(field.name(), "");
    /// assert_eq!(field.data_type(), &DataType::Utf8);
    /// assert!(field.is_nullable());
    /// ```
    fn into_nullable_field(self) -> Field;

    /// Convert the type to [`FieldRef`] with nullable type and "" name
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

/// DataFusion extension methods for Arrow [`Field`] and [`FieldRef`]
pub trait FieldExt {
    /// Returns a new Field representing a List of this Field's DataType.
    ///
    /// For example if input represents an `Int32`, the return value will
    /// represent a `List<Int32>`.
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::datatype::FieldExt;
    /// // Int32 field
    /// let int_field = Field::new("my_int", DataType::Int32, true);
    /// // convert to a List field
    /// let list_field = int_field.into_list();
    /// // List<Int32>
    /// // Note that the item field name has been renamed to "item"
    /// assert_eq!(list_field.data_type(), &DataType::List(Arc::new(
    ///     Field::new("item", DataType::Int32, true)
    /// )));
    ///
    fn into_list(self) -> Self;

    /// Return a new Field representing this Field as the item type of a
    /// [`DataType::FixedSizeList`]
    ///
    /// For example if input represents an `Int32`, the return value will
    /// represent a `FixedSizeList<Int32, size>`.
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::datatype::FieldExt;
    /// // Int32 field
    /// let int_field = Field::new("my_int", DataType::Int32, true);
    /// // convert to a FixedSizeList field of size 3
    /// let fixed_size_list_field = int_field.into_fixed_size_list(3);
    /// // FixedSizeList<Int32, 3>
    /// // Note that the item field name has been renamed to "item"
    /// assert_eq!(
    ///   fixed_size_list_field.data_type(),
    ///   &DataType::FixedSizeList(Arc::new(
    ///    Field::new("item", DataType::Int32, true)),
    ///    3
    /// ));
    ///
    fn into_fixed_size_list(self, list_size: i32) -> Self;

    /// Update the field to have the default list field name ("item")
    ///
    /// Lists are allowed to have an arbitrarily named field; however, a name
    /// other than 'item' will cause it to fail an == check against a more
    /// idiomatically created list in arrow-rs which causes issues.
    ///
    /// For example, if input represents an `Int32` field named "my_int",
    /// the return value will represent an `Int32` field named "item".
    ///
    /// Example:
    /// ```
    /// # use arrow::datatypes::Field;
    /// # use datafusion_common::datatype::FieldExt;
    /// let my_field = Field::new("my_int", arrow::datatypes::DataType::Int32, true);
    /// let item_field = my_field.into_list_item();
    /// assert_eq!(item_field.name(), Field::LIST_FIELD_DEFAULT_NAME);
    /// assert_eq!(item_field.name(), "item");
    /// ```
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
