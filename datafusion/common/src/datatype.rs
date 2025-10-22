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

use crate::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

/// DataFusion extension methods for Arrow [`DataType`]
pub trait DataTypeExt {
    /// convert the type to field with nullable type and "" name
    fn into_nullable_field(self) -> Field;

    /// convert the type to field ref with nullable type and "" name
    fn into_nullable_field_ref(self) -> Arc<Field>;

    //
}

impl DataTypeExt for DataType {
    fn into_nullable_field(self) -> Field {
        Field::new("", self, true)
    }

    fn into_nullable_field_ref(self) -> Arc<Field> {
        Arc::new(Field::new("", self, true))
    }
}

/// DataFusion extension methods for Arrow [`Field`]
pub trait FieldExt {
    /// Returns a new Field representing a List of this Field's DataType.
    fn into_list(self) -> Self;

    /// Return a new Field representing this Field as the item type of a FixedSizeList
    fn into_fixed_size_list(self, list_size: i32) -> Self;

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
        if self.name() != "item" {
            self.with_name("item")
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
        if self.name() != "item" {
            Arc::unwrap_or_clone(self).with_name("item").into()
        } else {
            self
        }
    }
}
