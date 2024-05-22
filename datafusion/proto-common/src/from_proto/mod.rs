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

use std::convert::{TryFrom, TryInto};

use arrow::datatypes::Field;

use crate::protobuf_common as protobuf;
use datafusion_common::Column;
use datafusion_common::DataFusionError;

#[derive(Debug)]
pub enum Error {
    General(String),

    DataFusionError(DataFusionError),

    MissingRequiredField(String),

    AtLeastOneValue(String),

    UnknownEnumVariant { name: String, value: i32 },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),

            Self::DataFusionError(desc) => {
                write!(f, "DataFusion error: {desc:?}")
            }

            Self::MissingRequiredField(name) => {
                write!(f, "Missing required field {name}")
            }
            Self::AtLeastOneValue(name) => {
                write!(f, "Must have at least one {name}, found 0")
            }
            Self::UnknownEnumVariant { name, value } => {
                write!(f, "Unknown i32 value for {name} enum: {value}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<DataFusionError> for Error {
    fn from(e: DataFusionError) -> Self {
        Error::DataFusionError(e)
    }
}

impl Error {
    fn required(field: impl Into<String>) -> Error {
        Error::MissingRequiredField(field.into())
    }

    fn unknown(name: impl Into<String>, value: i32) -> Error {
        Error::UnknownEnumVariant {
            name: name.into(),
            value,
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = Error>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    ///
    /// Returns None if the option is None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> Result<Option<T>, Error>;

    /// Converts an optional protobuf field to a different type, returning an error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn required(self, field: impl Into<String>) -> Result<T, Error>;
}

impl<T, U> FromOptionalField<U> for Option<T>
    where
        T: TryInto<U, Error = Error>,
{
    fn optional(self) -> Result<Option<U>, Error> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, Error> {
        match self {
            None => Err(Error::required(field)),
            Some(t) => t.try_into(),
        }
    }
}

impl From<protobuf::Column> for Column {
    fn from(c: protobuf::Column) -> Self {
        let protobuf::Column { relation, name } = c;

        Self::new(relation.map(|r| r.relation), name)
    }
}

impl From<&protobuf::Column> for Column {
    fn from(c: &protobuf::Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&protobuf::Field> for Field {
    type Error = Error;
    fn try_from(field: &protobuf::Field) -> Result<Self, Self::Error> {
        let datatype = field.arrow_type.as_deref().required("arrow_type")?;
        let field = if field.dict_id != 0 {
            Self::new_dict(
                field.name.as_str(),
                datatype,
                field.nullable,
                field.dict_id,
                field.dict_ordered,
            )
                .with_metadata(field.metadata.clone())
        } else {
            Self::new(field.name.as_str(), datatype, field.nullable)
                .with_metadata(field.metadata.clone())
        };
        Ok(field)
    }
}
