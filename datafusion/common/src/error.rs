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

//! DataFusion error types

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;
use std::sync::Arc;

use crate::utils::quote_identifier;
use crate::{Column, DFSchema, OwnedTableReference};
#[cfg(feature = "avro")]
use apache_avro::Error as AvroError;
use arrow::error::ArrowError;
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T, E = DataFusionError> = result::Result<T, E>;

/// Result type for operations that could result in an [DataFusionError] and needs to be shared (wrapped into `Arc`).
pub type SharedResult<T> = result::Result<T, Arc<DataFusionError>>;

/// Error type for generic operations that could result in DataFusionError::External
pub type GenericError = Box<dyn Error + Send + Sync>;

/// DataFusion error
#[derive(Debug)]
pub enum DataFusionError {
    /// Error returned by arrow.
    ArrowError(ArrowError),
    /// Wraps an error from the Parquet crate
    #[cfg(feature = "parquet")]
    ParquetError(ParquetError),
    /// Wraps an error from the Avro crate
    #[cfg(feature = "avro")]
    AvroError(AvroError),
    /// Wraps an error from the object_store crate
    #[cfg(feature = "object_store")]
    ObjectStore(object_store::Error),
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned when SQL is syntactically incorrect.
    SQL(ParserError),
    /// Error returned on a branch that we know it is possible
    /// but to which we still have no implementation for.
    /// Often, these errors are tracked in our issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in DataFusion.
    /// This error should not happen in normal usage of DataFusion.
    ///
    /// DataFusions has internal invariants that the compiler is not
    /// always able to check.  This error is raised when one of those
    /// invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid. Examples include
    /// impossible casts.
    Plan(String),
    /// This error happens with schema-related errors, such as schema inference not possible
    /// and non-unique column names.
    SchemaError(SchemaError),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
    /// This error is thrown when a consumer cannot acquire memory from the Memory Manager
    /// we can just cancel the execution of the partition.
    ResourcesExhausted(String),
    /// Errors originating from outside DataFusion's core codebase.
    /// For example, a custom S3Error from the crate datafusion-objectstore-s3
    External(GenericError),
    /// Error with additional context
    Context(String, Box<DataFusionError>),
    /// Errors originating from either mapping LogicalPlans to/from Substrait plans
    /// or serializing/deserializing protobytes to Substrait plans
    Substrait(String),
}

#[macro_export]
macro_rules! context {
    ($desc:expr, $err:expr) => {
        $err.context(format!("{} at {}:{}", $desc, file!(), line!()))
    };
}

#[macro_export]
macro_rules! plan_err {
    ($desc:expr) => {
        Err(datafusion_common::DataFusionError::Plan(format!(
            "{} at {}:{}",
            $desc,
            file!(),
            line!()
        )))
    };
}

/// Schema-related errors
#[derive(Debug)]
pub enum SchemaError {
    /// Schema contains a (possibly) qualified and unqualified field with same unqualified name
    AmbiguousReference { field: Column },
    /// Schema contains duplicate qualified field name
    DuplicateQualifiedField {
        qualifier: Box<OwnedTableReference>,
        name: String,
    },
    /// Schema contains duplicate unqualified field name
    DuplicateUnqualifiedField { name: String },
    /// No field with this name
    FieldNotFound {
        field: Box<Column>,
        valid_fields: Vec<Column>,
    },
}

/// Create a "field not found" DataFusion::SchemaError
pub fn field_not_found<R: Into<OwnedTableReference>>(
    qualifier: Option<R>,
    name: &str,
    schema: &DFSchema,
) -> DataFusionError {
    DataFusionError::SchemaError(SchemaError::FieldNotFound {
        field: Box::new(Column::new(qualifier, name)),
        valid_fields: schema
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect(),
    })
}

/// Convenience wrapper over [`field_not_found`] for when there is no qualifier
pub fn unqualified_field_not_found(name: &str, schema: &DFSchema) -> DataFusionError {
    DataFusionError::SchemaError(SchemaError::FieldNotFound {
        field: Box::new(Column::new_unqualified(name)),
        valid_fields: schema
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect(),
    })
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldNotFound {
                field,
                valid_fields,
            } => {
                write!(f, "No field named {}", field.quoted_flat_name())?;
                if !valid_fields.is_empty() {
                    write!(
                        f,
                        ". Valid fields are {}",
                        valid_fields
                            .iter()
                            .map(|field| field.quoted_flat_name())
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                write!(f, ".")
            }
            Self::DuplicateQualifiedField { qualifier, name } => {
                write!(
                    f,
                    "Schema contains duplicate qualified field name {}.{}",
                    qualifier.to_quoted_string(),
                    quote_identifier(name)
                )
            }
            Self::DuplicateUnqualifiedField { name } => {
                write!(
                    f,
                    "Schema contains duplicate unqualified field name {}",
                    quote_identifier(name)
                )
            }
            Self::AmbiguousReference { field } => {
                if field.relation.is_some() {
                    write!(
                        f,
                        "Schema contains qualified field name {} and unqualified field name {} which would be ambiguous",
                        field.quoted_flat_name(),
                        quote_identifier(&field.name)
                    )
                } else {
                    write!(
                        f,
                        "Ambiguous reference to unqualified field {}",
                        field.quoted_flat_name()
                    )
                }
            }
        }
    }
}

impl Error for SchemaError {}

impl From<std::fmt::Error> for DataFusionError {
    fn from(_e: std::fmt::Error) -> Self {
        DataFusionError::Execution("Fail to format".to_string())
    }
}

impl From<io::Error> for DataFusionError {
    fn from(e: io::Error) -> Self {
        DataFusionError::IoError(e)
    }
}

impl From<ArrowError> for DataFusionError {
    fn from(e: ArrowError) -> Self {
        DataFusionError::ArrowError(e)
    }
}

impl From<DataFusionError> for ArrowError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e) => e,
            DataFusionError::External(e) => ArrowError::ExternalError(e),
            other => ArrowError::ExternalError(Box::new(other)),
        }
    }
}

#[cfg(feature = "parquet")]
impl From<ParquetError> for DataFusionError {
    fn from(e: ParquetError) -> Self {
        DataFusionError::ParquetError(e)
    }
}

#[cfg(feature = "avro")]
impl From<AvroError> for DataFusionError {
    fn from(e: AvroError) -> Self {
        DataFusionError::AvroError(e)
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::Error> for DataFusionError {
    fn from(e: object_store::Error) -> Self {
        DataFusionError::ObjectStore(e)
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::path::Error> for DataFusionError {
    fn from(e: object_store::path::Error) -> Self {
        DataFusionError::ObjectStore(e.into())
    }
}

impl From<ParserError> for DataFusionError {
    fn from(e: ParserError) -> Self {
        DataFusionError::SQL(e)
    }
}

impl From<GenericError> for DataFusionError {
    fn from(err: GenericError) -> Self {
        DataFusionError::External(err)
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            DataFusionError::ArrowError(ref desc) => write!(f, "Arrow error: {desc}"),
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {desc}")
            }
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(ref desc) => {
                write!(f, "Avro error: {desc}")
            }
            DataFusionError::IoError(ref desc) => write!(f, "IO error: {desc}"),
            DataFusionError::SQL(ref desc) => {
                write!(f, "SQL error: {desc:?}")
            }
            DataFusionError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {desc}")
            }
            DataFusionError::Internal(ref desc) => {
                write!(f, "Internal error: {desc}. This was likely caused by a bug in DataFusion's \
                    code and we would welcome that you file an bug report in our issue tracker")
            }
            DataFusionError::Plan(ref desc) => {
                write!(f, "Error during planning: {desc}")
            }
            DataFusionError::SchemaError(ref desc) => {
                write!(f, "Schema error: {desc}")
            }
            DataFusionError::Execution(ref desc) => {
                write!(f, "Execution error: {desc}")
            }
            DataFusionError::ResourcesExhausted(ref desc) => {
                write!(f, "Resources exhausted: {desc}")
            }
            DataFusionError::External(ref desc) => {
                write!(f, "External error: {desc}")
            }
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(ref desc) => {
                write!(f, "Object Store error: {desc}")
            }
            DataFusionError::Context(ref desc, ref err) => {
                write!(f, "{}\ncaused by\n{}", desc, *err)
            }
            DataFusionError::Substrait(ref desc) => {
                write!(f, "Substrait error: {desc}")
            }
        }
    }
}

impl Error for DataFusionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataFusionError::ArrowError(e) => Some(e),
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(e) => Some(e),
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(e) => Some(e),
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(e) => Some(e),
            DataFusionError::IoError(e) => Some(e),
            DataFusionError::SQL(e) => Some(e),
            DataFusionError::NotImplemented(_) => None,
            DataFusionError::Internal(_) => None,
            DataFusionError::Plan(_) => None,
            DataFusionError::SchemaError(e) => Some(e),
            DataFusionError::Execution(_) => None,
            DataFusionError::ResourcesExhausted(_) => None,
            DataFusionError::External(e) => Some(e.as_ref()),
            DataFusionError::Context(_, e) => Some(e.as_ref()),
            DataFusionError::Substrait(_) => None,
        }
    }
}

impl From<DataFusionError> for io::Error {
    fn from(e: DataFusionError) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

impl DataFusionError {
    /// Get deepest underlying [`DataFusionError`]
    ///
    /// [`DataFusionError`]s sometimes form a chain, such as `DataFusionError::ArrowError()` in order to conform
    /// to the correct error signature. Thus sometimes there is a chain several layers deep that can obscure the
    /// original error. This function finds the lowest level DataFusionError possible.
    ///
    /// For example,  `find_root` will return`DataFusionError::ResourceExhausted` given the input
    /// ```text
    /// DataFusionError::ArrowError
    ///   ArrowError::External
    ///    Box(DataFusionError::Context)
    ///      DataFusionError::ResourceExhausted
    /// ```
    ///
    /// This may be the same as `self`.
    pub fn find_root(&self) -> &Self {
        // Note: This is a non-recursive algorithm so we do not run
        // out of stack space, even for long error chains.

        let mut last_datafusion_error = self;
        let mut root_error: &dyn Error = self;
        while let Some(source) = root_error.source() {
            // walk the next level
            root_error = source;
            // remember the lowest datafusion error so far
            if let Some(e) = root_error.downcast_ref::<DataFusionError>() {
                last_datafusion_error = e;
            } else if let Some(e) = root_error.downcast_ref::<Arc<DataFusionError>>() {
                // As `Arc<T>::source()` calls through to `T::source()` we need to
                // explicitly match `Arc<DataFusionError>` to capture it
                last_datafusion_error = e.as_ref();
            }
        }
        // return last checkpoint (which may be the original error)
        last_datafusion_error
    }

    /// wraps self in Self::Context with a description
    pub fn context(self, description: impl Into<String>) -> Self {
        Self::Context(description.into(), Box::new(self))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::error::DataFusionError;
    use arrow::error::ArrowError;

    #[test]
    fn arrow_error_to_datafusion() {
        let res = return_arrow_error().unwrap_err();
        assert_eq!(
            res.to_string(),
            "External error: Error during planning: foo"
        );
    }

    #[test]
    fn datafusion_error_to_arrow() {
        let res = return_datafusion_error().unwrap_err();
        assert_eq!(res.to_string(), "Arrow error: Schema error: bar");
    }

    #[test]
    fn test_find_root_error() {
        do_root_test(
            DataFusionError::Context(
                "it happened!".to_string(),
                Box::new(DataFusionError::ResourcesExhausted("foo".to_string())),
            ),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::ArrowError(ArrowError::ExternalError(Box::new(
                DataFusionError::ResourcesExhausted("foo".to_string()),
            ))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::External(Box::new(DataFusionError::ResourcesExhausted(
                "foo".to_string(),
            ))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::External(Box::new(ArrowError::ExternalError(Box::new(
                DataFusionError::ResourcesExhausted("foo".to_string()),
            )))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::ArrowError(ArrowError::ExternalError(Box::new(
                ArrowError::ExternalError(Box::new(DataFusionError::ResourcesExhausted(
                    "foo".to_string(),
                ))),
            ))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::External(Box::new(Arc::new(
                DataFusionError::ResourcesExhausted("foo".to_string()),
            ))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );

        do_root_test(
            DataFusionError::External(Box::new(Arc::new(ArrowError::ExternalError(
                Box::new(DataFusionError::ResourcesExhausted("foo".to_string())),
            )))),
            DataFusionError::ResourcesExhausted("foo".to_string()),
        );
    }

    /// Model what happens when implementing SendableRecordBatchStream:
    /// DataFusion code needs to return an ArrowError
    fn return_arrow_error() -> arrow::error::Result<()> {
        // Expect the '?' to work
        Err(DataFusionError::Plan("foo".to_string()).into())
    }

    /// Model what happens when using arrow kernels in DataFusion
    /// code: need to turn an ArrowError into a DataFusionError
    fn return_datafusion_error() -> crate::error::Result<()> {
        // Expect the '?' to work
        Err(ArrowError::SchemaError("bar".to_string()).into())
    }

    fn do_root_test(e: DataFusionError, exp: DataFusionError) {
        let e = e.find_root();

        // DataFusionError does not implement Eq, so we use a string comparison + some cheap "same variant" test instead
        assert_eq!(e.to_string(), exp.to_string(),);
        assert_eq!(std::mem::discriminant(e), std::mem::discriminant(&exp),)
    }
}

#[macro_export]
macro_rules! internal_err {
    ($($arg:tt)*) => {
        Err(DataFusionError::Internal(format!($($arg)*)))
    };
}

/// Unwrap an `Option` if possible. Otherwise return an `DataFusionError::Internal`.
/// In normal usage of DataFusion the unwrap should always succeed.
///
/// Example: `let values = unwrap_or_internal_err!(values)`
#[macro_export]
macro_rules! unwrap_or_internal_err {
    ($Value: ident) => {
        $Value.ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{} should not be None",
                stringify!($Value)
            ))
        })?
    };
}
