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

use std::{fmt::Display, sync::Arc};

use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::logical_type::extension::{ExtensionType, ExtensionTypeRef};
use crate::logical_type::field::{LogicalField, LogicalFieldRef};
use crate::logical_type::fields::LogicalFields;

pub mod type_signature;
pub mod extension;
pub mod registry;
pub mod schema;
pub mod field;
pub mod fields;

#[derive(Clone, Debug)]
pub enum LogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit, Option<Arc<str>>),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    Binary,
    FixedSizeBinary(i32),
    LargeBinary,
    Utf8,
    LargeUtf8,
    List(LogicalFieldRef),
    FixedSizeList(LogicalFieldRef, i32),
    LargeList(LogicalFieldRef),
    Struct(LogicalFields),
    Map(LogicalFieldRef, bool),
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Extension(ExtensionTypeRef),
    // TODO: tbd union
}

impl PartialEq for LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.type_signature() == other.type_signature()
    }
}

impl Eq for LogicalType {}

impl std::hash::Hash for LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_signature().hash(state)
    }
}

impl Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<&DataType> for LogicalType {
    fn from(value: &DataType) -> Self {
        value.clone().into()
    }
}

impl From<DataType> for LogicalType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Null => LogicalType::Null,
            DataType::Boolean => LogicalType::Boolean,
            DataType::Int8 => LogicalType::Int8,
            DataType::Int16 => LogicalType::Int16,
            DataType::Int32 => LogicalType::Int32,
            DataType::Int64 => LogicalType::Int64,
            DataType::UInt8 => LogicalType::UInt8,
            DataType::UInt16 => LogicalType::UInt16,
            DataType::UInt32 => LogicalType::UInt32,
            DataType::UInt64 => LogicalType::UInt64,
            DataType::Float16 => LogicalType::Float16,
            DataType::Float32 => LogicalType::Float32,
            DataType::Float64 => LogicalType::Float64,
            DataType::Timestamp(tu, z) => LogicalType::Timestamp(tu, z),
            DataType::Date32 => LogicalType::Date32,
            DataType::Date64 => LogicalType::Date64,
            DataType::Time32(tu) => LogicalType::Time32(tu),
            DataType::Time64(tu) => LogicalType::Time64(tu),
            DataType::Duration(tu) => LogicalType::Duration(tu),
            DataType::Interval(iu) => LogicalType::Interval(iu),
            DataType::Binary | DataType::BinaryView => LogicalType::Binary,
            DataType::FixedSizeBinary(len) => LogicalType::FixedSizeBinary(len),
            DataType::LargeBinary => LogicalType::LargeBinary,
            DataType::Utf8 | DataType::Utf8View => LogicalType::Utf8,
            DataType::LargeUtf8 => LogicalType::LargeUtf8,
            DataType::List(f) | DataType::ListView(f) => LogicalType::List(LogicalFieldRef::new(f.as_ref().into())),
            DataType::FixedSizeList(f, len) => LogicalType::FixedSizeList(LogicalFieldRef::new(f.as_ref().into()), len),
            DataType::LargeList(f) | DataType::LargeListView(f) => LogicalType::LargeList(LogicalFieldRef::new(f.as_ref().into())),
            DataType::Struct(fields) => LogicalType::Struct(fields.into()),
            DataType::Dictionary(_, dt) => dt.as_ref().into(),
            DataType::Decimal128(p, s) => LogicalType::Decimal128(p, s),
            DataType::Decimal256(p, s) => LogicalType::Decimal256(p, s),
            DataType::Map(f, sorted) => LogicalType::Map(LogicalFieldRef::new(f.as_ref().into()), sorted),
            DataType::RunEndEncoded(_, f) => f.data_type().into(),
            DataType::Union(_, _) => unimplemented!(), // TODO: tbd union
        }
    }
}

impl LogicalType {

    pub fn new_list(data_type: LogicalType, nullable: bool) -> Self {
        LogicalType::List(Arc::new(LogicalField::new_list_field(data_type, nullable)))
    }

    pub fn new_large_list(data_type: LogicalType, nullable: bool) -> Self {
        LogicalType::LargeList(Arc::new(LogicalField::new_list_field(data_type, nullable)))
    }

    pub fn new_fixed_size_list(data_type: LogicalType, size: i32, nullable: bool) -> Self {
        LogicalType::FixedSizeList(Arc::new(LogicalField::new_list_field(data_type, nullable)), size)
    }

    pub fn is_floating(&self) -> bool {
        matches!(self, Self::Float16 | Self::Float32 | Self::Float64)
    }
}
