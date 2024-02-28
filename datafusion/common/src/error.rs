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
#[cfg(feature = "backtrace")]
use std::backtrace::{Backtrace, BacktraceStatus};

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
    ///
    /// 2nd argument is for optional backtrace
    ArrowError(ArrowError, Option<String>),
    /// Error when reading / writing Parquet data.
    #[cfg(feature = "parquet")]
    ParquetError(ParquetError),
    /// Error when reading Avro data.
    #[cfg(feature = "avro")]
    AvroError(AvroError),
    /// Error when reading / writing to / from an object_store (e.g. S3 or LocalFile)
    #[cfg(feature = "object_store")]
    ObjectStore(object_store::Error),
    /// Error when an I/O operation fails
    IoError(io::Error),
    /// Error when SQL is syntactically incorrect.
    ///
    /// 2nd argument is for optional backtrace    
    SQL(ParserError, Option<String>),
    /// Error when a feature is not yet implemented.
    ///
    /// These errors are sometimes returned for features that are still in
    /// development and are not entirely complete. Often, these errors are
    /// tracked in our issue tracker.
    NotImplemented(String),
    /// Error due to bugs in DataFusion
    ///
    /// This error should not happen in normal usage of DataFusion. It results
    /// from something that wasn't expected/anticipated by the implementation
    /// and that is most likely a bug (the error message even encourages users
    /// to open a bug report). A user should not be able to trigger internal
    /// errors under normal circumstances by feeding in malformed queries, bad
    /// data, etc.
    ///
    /// Note that I/O errors (or any error that happens due to external systems)
    /// do NOT fall under this category. See other variants such as
    /// [`Self::IoError`] and [`Self::External`].
    ///
    /// DataFusions has internal invariants that the compiler is not always able
    /// to check. This error is raised when one of those invariants does not
    /// hold for some reason.
    Internal(String),
    /// Error during planning of the query.
    ///
    /// This error happens when the user provides a bad query or plan, for
    /// example the user attempts to call a function that doesn't exist, or if
    /// the types of a function call are not supported.
    Plan(String),
    /// Error for invalid or unsupported configuration options.
    Configuration(String),
    /// Error when there is a problem with the query related to schema.
    ///
    /// This error can be returned in cases such as when schema inference is not
    /// possible and when column names are not unique.
    ///
    /// 2nd argument is for optional backtrace    
    /// Boxing the optional backtrace to prevent <https://rust-lang.github.io/rust-clippy/master/index.html#/result_large_err>
    SchemaError(SchemaError, Box<Option<String>>),
    /// Error during execution of the query.
    ///
    /// This error is returned when an error happens during execution due to a
    /// malformed input. For example, the user passed malformed arguments to a
    /// SQL method, opened a CSV file that is broken, or tried to divide an
    /// integer by zero.
    Execution(String),
    /// Error when resources (such as memory of scratch disk space) are exhausted.
    ///
    /// This error is thrown when a consumer cannot acquire additional memory
    /// or other resources needed to execute the query from the Memory Manager.
    ResourcesExhausted(String),
    /// Errors originating from outside DataFusion's core codebase.
    ///
    /// For example, a custom S3Error from the crate datafusion-objectstore-s3
    External(GenericError),
    /// Error with additional context
    Context(String, Box<DataFusionError>),
    /// Errors from either mapping LogicalPlans to/from Substrait plans
    /// or serializing/deserializing protobytes to Substrait plans
    Substrait(String),
}

#[macro_export]
macro_rules! context {
    ($desc:expr, $err:expr) => {
        $err.context(format!("{} at {}:{}", $desc, file!(), line!()))
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
        DataFusionError::ArrowError(e, None)
    }
}

impl From<DataFusionError> for ArrowError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e, _) => e,
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
        DataFusionError::SQL(e, None)
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
            DataFusionError::ArrowError(ref desc, ref backtrace) => {
                let backtrace = backtrace.clone().unwrap_or("".to_owned());
                write!(f, "Arrow error: {desc}{backtrace}")
            }
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {desc}")
            }
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(ref desc) => {
                write!(f, "Avro error: {desc}")
            }
            DataFusionError::IoError(ref desc) => {
                write!(f, "IO error: {desc}")
            }
            DataFusionError::SQL(ref desc, ref backtrace) => {
                let backtrace: String = backtrace.clone().unwrap_or("".to_owned());
                write!(f, "SQL error: {desc:?}{backtrace}")
            }
            DataFusionError::Configuration(ref desc) => {
                write!(f, "Invalid or Unsupported Configuration: {desc}")
            }
            DataFusionError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {desc}")
            }
            DataFusionError::Internal(ref desc) => {
                write!(f, "Internal error: {desc}.\nThis was likely caused by a bug in DataFusion's \
                    code and we would welcome that you file an bug report in our issue tracker")
            }
            DataFusionError::Plan(ref desc) => {
                write!(f, "Error during planning: {desc}")
            }
            DataFusionError::SchemaError(ref desc, ref backtrace) => {
                let backtrace: &str =
                    &backtrace.as_ref().clone().unwrap_or("".to_owned());
                write!(f, "Schema error: {desc}{backtrace}")
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
            DataFusionError::ArrowError(e, _) => Some(e),
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(e) => Some(e),
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(e) => Some(e),
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(e) => Some(e),
            DataFusionError::IoError(e) => Some(e),
            DataFusionError::SQL(e, _) => Some(e),
            DataFusionError::NotImplemented(_) => None,
            DataFusionError::Internal(_) => None,
            DataFusionError::Configuration(_) => None,
            DataFusionError::Plan(_) => None,
            DataFusionError::SchemaError(e, _) => Some(e),
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
    const BACK_TRACE_SEP: &'static str = "\n\nbacktrace: ";

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

    pub fn strip_backtrace(&self) -> String {
        self.to_string()
            .split(Self::BACK_TRACE_SEP)
            .collect::<Vec<&str>>()
            .first()
            .unwrap_or(&"")
            .to_string()
    }

    /// To enable optional rust backtrace in DataFusion:
    /// - [`Setup Env Variables`]<https://doc.rust-lang.org/std/backtrace/index.html#environment-variables>
    /// - Enable `backtrace` cargo feature
    ///
    /// Example:
    /// cargo build --features 'backtrace'
    /// RUST_BACKTRACE=1 ./app
    #[inline(always)]
    pub fn get_back_trace() -> String {
        #[cfg(feature = "backtrace")]
        {
            let back_trace = Backtrace::capture();
            if back_trace.status() == BacktraceStatus::Captured {
                return format!("{}{}", Self::BACK_TRACE_SEP, back_trace);
            }

            "".to_owned()
        }

        #[cfg(not(feature = "backtrace"))]
        "".to_owned()
    }
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

macro_rules! with_dollar_sign {
    ($($body:tt)*) => {
        macro_rules! __with_dollar_sign { $($body)* }
        __with_dollar_sign!($);
    }
}

/// Add a macros for concise  DataFusionError::* errors declaration
/// supports placeholders the same way as `format!`
/// Examples:
///     plan_err!("Error")
///     plan_err!("Error {}", val)
///     plan_err!("Error {:?}", val)
///     plan_err!("Error {val}")
///     plan_err!("Error {val:?}")
///
/// `NAME_ERR` -  macro name for wrapping Err(DataFusionError::*)
/// `NAME_DF_ERR` -  macro name for wrapping DataFusionError::*. Needed to keep backtrace opportunity
/// in construction where DataFusionError::* used directly, like `map_err`, `ok_or_else`, etc
macro_rules! make_error {
    ($NAME_ERR:ident, $NAME_DF_ERR: ident, $ERR:ident) => {
        with_dollar_sign! {
            ($d:tt) => {
                /// Macro wraps `$ERR` to add backtrace feature
                #[macro_export]
                macro_rules! $NAME_DF_ERR {
                    ($d($d args:expr),*) => {
                        $crate::DataFusionError::$ERR(
                            format!(
                                "{}{}",
                                format!($d($d args),*),
                                $crate::DataFusionError::get_back_trace(),
                            ).into()
                        )
                    }
                }

                /// Macro wraps Err(`$ERR`) to add backtrace feature
                #[macro_export]
                macro_rules! $NAME_ERR {
                    ($d($d args:expr),*) => {
                        Err($crate::DataFusionError::$ERR(
                            format!(
                                "{}{}",
                                format!($d($d args),*),
                                $crate::DataFusionError::get_back_trace(),
                            ).into()
                        ))
                    }
                }
            }
        }
    };
}

// Exposes a macro to create `DataFusionError::Plan` with optional backtrace
make_error!(plan_err, plan_datafusion_err, Plan);

// Exposes a macro to create `DataFusionError::Internal` with optional backtrace
make_error!(internal_err, internal_datafusion_err, Internal);

// Exposes a macro to create `DataFusionError::NotImplemented` with optional backtrace
make_error!(not_impl_err, not_impl_datafusion_err, NotImplemented);

// Exposes a macro to create `DataFusionError::Execution` with optional backtrace
make_error!(exec_err, exec_datafusion_err, Execution);

// Exposes a macro to create `DataFusionError::Substrait` with optional backtrace
make_error!(substrait_err, substrait_datafusion_err, Substrait);

// Exposes a macro to create `DataFusionError::SQL` with optional backtrace
#[macro_export]
macro_rules! sql_datafusion_err {
    ($ERR:expr) => {
        DataFusionError::SQL($ERR, Some(DataFusionError::get_back_trace()))
    };
}

// Exposes a macro to create `Err(DataFusionError::SQL)` with optional backtrace
#[macro_export]
macro_rules! sql_err {
    ($ERR:expr) => {
        Err(datafusion_common::sql_datafusion_err!($ERR))
    };
}

// Exposes a macro to create `DataFusionError::ArrowError` with optional backtrace
#[macro_export]
macro_rules! arrow_datafusion_err {
    ($ERR:expr) => {
        DataFusionError::ArrowError($ERR, Some(DataFusionError::get_back_trace()))
    };
}

// Exposes a macro to create `Err(DataFusionError::ArrowError)` with optional backtrace
#[macro_export]
macro_rules! arrow_err {
    ($ERR:expr) => {
        Err(datafusion_common::arrow_datafusion_err!($ERR))
    };
}

// Exposes a macro to create `DataFusionError::SchemaError` with optional backtrace
#[macro_export]
macro_rules! schema_datafusion_err {
    ($ERR:expr) => {
        DataFusionError::SchemaError(
            $ERR,
            Box::new(Some(DataFusionError::get_back_trace())),
        )
    };
}

// Exposes a macro to create `Err(DataFusionError::SchemaError)` with optional backtrace
#[macro_export]
macro_rules! schema_err {
    ($ERR:expr) => {
        Err(DataFusionError::SchemaError(
            $ERR,
            Box::new(Some(DataFusionError::get_back_trace())),
        ))
    };
}

// To avoid compiler error when using macro in the same crate:
// macros from the current crate cannot be referred to by absolute paths
pub use internal_datafusion_err as _internal_datafusion_err;
pub use internal_err as _internal_err;
pub use not_impl_err as _not_impl_err;
pub use plan_err as _plan_err;
pub use schema_err as _schema_err;

/// Create a "field not found" DataFusion::SchemaError
pub fn field_not_found<R: Into<OwnedTableReference>>(
    qualifier: Option<R>,
    name: &str,
    schema: &DFSchema,
) -> DataFusionError {
    schema_datafusion_err!(SchemaError::FieldNotFound {
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
    schema_datafusion_err!(SchemaError::FieldNotFound {
        field: Box::new(Column::new_unqualified(name)),
        valid_fields: schema
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect(),
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::error::DataFusionError;
    use arrow::error::ArrowError;

    #[test]
    fn datafusion_error_to_arrow() {
        let res = return_arrow_error().unwrap_err();
        assert!(res
            .to_string()
            .starts_with("External error: Error during planning: foo"));
    }

    #[test]
    fn arrow_error_to_datafusion() {
        let res = return_datafusion_error().unwrap_err();
        assert_eq!(res.strip_backtrace(), "Arrow error: Schema error: bar");
    }

    // RUST_BACKTRACE=1 cargo test --features backtrace --package datafusion-common --lib -- error::test::test_backtrace
    #[cfg(feature = "backtrace")]
    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn test_enabled_backtrace() {
        let res: Result<(), DataFusionError> = plan_err!("Err");
        let err = res.unwrap_err().to_string();
        assert!(err.contains(DataFusionError::BACK_TRACE_SEP));
        assert_eq!(
            err.split(DataFusionError::BACK_TRACE_SEP)
                .collect::<Vec<&str>>()
                .first()
                .unwrap(),
            &"Error during planning: Err"
        );
        assert!(!err
            .split(DataFusionError::BACK_TRACE_SEP)
            .collect::<Vec<&str>>()
            .get(1)
            .unwrap()
            .is_empty());
    }

    #[cfg(not(feature = "backtrace"))]
    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn test_disabled_backtrace() {
        let res: Result<(), DataFusionError> = plan_err!("Err");
        let res = res.unwrap_err().to_string();
        assert!(!res.contains(DataFusionError::BACK_TRACE_SEP));
        assert_eq!(res, "Error during planning: Err");
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
            DataFusionError::ArrowError(
                ArrowError::ExternalError(Box::new(DataFusionError::ResourcesExhausted(
                    "foo".to_string(),
                ))),
                None,
            ),
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
            DataFusionError::ArrowError(
                ArrowError::ExternalError(Box::new(ArrowError::ExternalError(Box::new(
                    DataFusionError::ResourcesExhausted("foo".to_string()),
                )))),
                None,
            ),
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

    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn test_make_error_parse_input() {
        let res: Result<(), DataFusionError> = plan_err!("Err");
        let res = res.unwrap_err();
        assert_eq!(res.strip_backtrace(), "Error during planning: Err");

        let extra1 = "extra1";
        let extra2 = "extra2";

        let res: Result<(), DataFusionError> = plan_err!("Err {} {}", extra1, extra2);
        let res = res.unwrap_err();
        assert_eq!(
            res.strip_backtrace(),
            "Error during planning: Err extra1 extra2"
        );

        let res: Result<(), DataFusionError> =
            plan_err!("Err {:?} {:#?}", extra1, extra2);
        let res = res.unwrap_err();
        assert_eq!(
            res.strip_backtrace(),
            "Error during planning: Err \"extra1\" \"extra2\""
        );

        let res: Result<(), DataFusionError> = plan_err!("Err {extra1} {extra2}");
        let res = res.unwrap_err();
        assert_eq!(
            res.strip_backtrace(),
            "Error during planning: Err extra1 extra2"
        );

        let res: Result<(), DataFusionError> = plan_err!("Err {extra1:?} {extra2:#?}");
        let res = res.unwrap_err();
        assert_eq!(
            res.strip_backtrace(),
            "Error during planning: Err \"extra1\" \"extra2\""
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
        assert_eq!(e.strip_backtrace(), exp.strip_backtrace());
        assert_eq!(std::mem::discriminant(e), std::mem::discriminant(&exp),)
    }
}
