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

//! # Error Handling in DataFusion
//!
//! In DataFusion, there are two types of errors that can be raised:
//!
//! 1. Expected errors – These indicate invalid operations performed by the caller,
//!    such as attempting to open a non-existent file. Different categories exist to
//!    distinguish their sources (e.g., [`DataFusionError::ArrowError`],
//!    [`DataFusionError::IoError`], etc.).
//!
//! 2. Unexpected errors – Represented by [`DataFusionError::Internal`], these
//!    indicate that an internal invariant has been broken, suggesting a potential
//!    bug in the system.
//!
//! There are several convenient macros for throwing errors. For example, use
//! `exec_err!` for expected errors.
//! For invariant checks, you can use `assert_or_internal_err!`,
//! `assert_eq_or_internal_err!`, `assert_ne_or_internal_err!` for easier assertions.
//! On the performance-critical path, use `debug_assert!` instead to reduce overhead.

#[cfg(feature = "backtrace")]
use std::backtrace::{Backtrace, BacktraceStatus};

use std::borrow::Cow;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;
use std::sync::Arc;

use crate::utils::datafusion_strsim::normalized_levenshtein;
use crate::utils::quote_identifier;
use crate::{Column, DFSchema, Diagnostic, TableReference};
#[cfg(feature = "avro")]
use apache_avro::Error as AvroError;
use arrow::error::ArrowError;
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
#[cfg(feature = "sql")]
use sqlparser::parser::ParserError;
use tokio::task::JoinError;

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
    ArrowError(Box<ArrowError>, Option<String>),
    /// Error when reading / writing Parquet data.
    #[cfg(feature = "parquet")]
    ParquetError(Box<ParquetError>),
    /// Error when reading Avro data.
    #[cfg(feature = "avro")]
    AvroError(Box<AvroError>),
    /// Error when reading / writing to / from an object_store (e.g. S3 or LocalFile)
    #[cfg(feature = "object_store")]
    ObjectStore(Box<object_store::Error>),
    /// Error when an I/O operation fails
    IoError(io::Error),
    /// Error when SQL is syntactically incorrect.
    ///
    /// 2nd argument is for optional backtrace
    #[cfg(feature = "sql")]
    SQL(Box<ParserError>, Option<String>),
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
    SchemaError(Box<SchemaError>, Box<Option<String>>),
    /// Error during execution of the query.
    ///
    /// This error is returned when an error happens during execution due to a
    /// malformed input. For example, the user passed malformed arguments to a
    /// SQL method, opened a CSV file that is broken, or tried to divide an
    /// integer by zero.
    Execution(String),
    /// [`JoinError`] during execution of the query.
    ///
    /// This error can't occur for unjoined tasks, such as execution shutdown.
    ExecutionJoin(Box<JoinError>),
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
    /// Error wrapped together with additional contextual information intended
    /// for end users, to help them understand what went wrong by providing
    /// human-readable messages, and locations in the source query that relate
    /// to the error in some way.
    Diagnostic(Box<Diagnostic>, Box<DataFusionError>),
    /// A collection of one or more [`DataFusionError`]. Useful in cases where
    /// DataFusion can recover from an erroneous state, and produce more errors
    /// before terminating. e.g. when planning a SELECT clause, DataFusion can
    /// synchronize to the next `SelectItem` if the previous one had errors. The
    /// end result is that the user can see errors about all `SelectItem`,
    /// instead of just the first one.
    Collection(Vec<DataFusionError>),
    /// A [`DataFusionError`] which shares an underlying [`DataFusionError`].
    ///
    /// This is useful when the same underlying [`DataFusionError`] is passed
    /// to multiple receivers. For example, when the source of a repartition
    /// errors and the error is propagated to multiple consumers.
    Shared(Arc<DataFusionError>),
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
    AmbiguousReference { field: Box<Column> },
    /// Schema contains duplicate qualified field name
    DuplicateQualifiedField {
        qualifier: Box<TableReference>,
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
                let lower_valid_fields = valid_fields
                    .iter()
                    .map(|column| column.flat_name().to_lowercase())
                    .collect::<Vec<String>>();

                let valid_fields_names = valid_fields
                    .iter()
                    .map(|column| column.flat_name())
                    .collect::<Vec<String>>();
                if lower_valid_fields.contains(&field.flat_name().to_lowercase()) {
                    write!(
                        f,
                        ". Column names are case sensitive. You can use double quotes to refer to the \"{}\" column \
                        or set the datafusion.sql_parser.enable_ident_normalization configuration",
                        field.quoted_flat_name()
                    )?;
                }
                let field_name = field.name();
                if let Some(matched) = valid_fields_names
                    .iter()
                    .filter(|str| normalized_levenshtein(str, field_name) >= 0.5)
                    .collect::<Vec<&String>>()
                    .first()
                {
                    write!(f, ". Did you mean '{matched}'?")?;
                } else if !valid_fields.is_empty() {
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
        DataFusionError::ArrowError(Box::new(e), Some(DataFusionError::get_back_trace()))
    }
}

impl From<DataFusionError> for ArrowError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e, _) => *e,
            DataFusionError::External(e) => ArrowError::ExternalError(e),
            other => ArrowError::ExternalError(Box::new(other)),
        }
    }
}

impl From<&Arc<DataFusionError>> for DataFusionError {
    fn from(e: &Arc<DataFusionError>) -> Self {
        if let DataFusionError::Shared(e_inner) = e.as_ref() {
            // don't re-wrap
            DataFusionError::Shared(Arc::clone(e_inner))
        } else {
            DataFusionError::Shared(Arc::clone(e))
        }
    }
}

#[cfg(feature = "parquet")]
impl From<ParquetError> for DataFusionError {
    fn from(e: ParquetError) -> Self {
        DataFusionError::ParquetError(Box::new(e))
    }
}

#[cfg(feature = "avro")]
impl From<AvroError> for DataFusionError {
    fn from(e: AvroError) -> Self {
        DataFusionError::AvroError(Box::new(e))
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::Error> for DataFusionError {
    fn from(e: object_store::Error) -> Self {
        DataFusionError::ObjectStore(Box::new(e))
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::path::Error> for DataFusionError {
    fn from(e: object_store::path::Error) -> Self {
        DataFusionError::ObjectStore(Box::new(e.into()))
    }
}

#[cfg(feature = "sql")]
impl From<ParserError> for DataFusionError {
    fn from(e: ParserError) -> Self {
        DataFusionError::SQL(Box::new(e), None)
    }
}

impl From<GenericError> for DataFusionError {
    fn from(err: GenericError) -> Self {
        // If the error is already a DataFusionError, not wrapping it.
        if err.is::<DataFusionError>() {
            if let Ok(e) = err.downcast::<DataFusionError>() {
                *e
            } else {
                unreachable!()
            }
        } else {
            DataFusionError::External(err)
        }
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let error_prefix = self.error_prefix();
        let message = self.message();
        write!(f, "{error_prefix}{message}")
    }
}

impl Error for DataFusionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataFusionError::ArrowError(e, _) => Some(e.as_ref()),
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(e) => Some(e.as_ref()),
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(e) => Some(e.as_ref()),
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(e) => Some(e.as_ref()),
            DataFusionError::IoError(e) => Some(e),
            #[cfg(feature = "sql")]
            DataFusionError::SQL(e, _) => Some(e.as_ref()),
            DataFusionError::NotImplemented(_) => None,
            DataFusionError::Internal(_) => None,
            DataFusionError::Configuration(_) => None,
            DataFusionError::Plan(_) => None,
            DataFusionError::SchemaError(e, _) => Some(e.as_ref()),
            DataFusionError::Execution(_) => None,
            DataFusionError::ExecutionJoin(e) => Some(e.as_ref()),
            DataFusionError::ResourcesExhausted(_) => None,
            DataFusionError::External(e) => Some(e.as_ref()),
            DataFusionError::Context(_, e) => Some(e.as_ref()),
            DataFusionError::Substrait(_) => None,
            DataFusionError::Diagnostic(_, e) => Some(e.as_ref()),
            // Can't really make a Collection fit into the mold of "an error has
            // at most one source", but returning the first one is probably good
            // idea. Especially since `DataFusionError::Collection` is mostly
            // meant for consumption by the end user, so shouldn't interfere
            // with programmatic usage too much. Plus, having 1 or 5 errors
            // doesn't really change the fact that the query is invalid and
            // can't be executed.
            DataFusionError::Collection(errs) => errs.first().map(|e| e as &dyn Error),
            DataFusionError::Shared(e) => Some(e.as_ref()),
        }
    }
}

impl From<DataFusionError> for io::Error {
    fn from(e: DataFusionError) -> Self {
        io::Error::other(e)
    }
}

impl DataFusionError {
    /// The separator between the error message and the backtrace
    pub const BACK_TRACE_SEP: &'static str = "\n\nbacktrace: ";

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

    /// Strips backtrace out of the error message
    /// If backtrace enabled then error has a format "message" [`Self::BACK_TRACE_SEP`] "backtrace"
    /// The method strips the backtrace and outputs "message"
    pub fn strip_backtrace(&self) -> String {
        (*self
            .to_string()
            .split(Self::BACK_TRACE_SEP)
            .collect::<Vec<&str>>()
            .first()
            .unwrap_or(&""))
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

    /// Return a [`DataFusionErrorBuilder`] to build a [`DataFusionError`]
    pub fn builder() -> DataFusionErrorBuilder {
        DataFusionErrorBuilder::default()
    }

    fn error_prefix(&self) -> &'static str {
        match self {
            DataFusionError::ArrowError(_, _) => "Arrow error: ",
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(_) => "Parquet error: ",
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(_) => "Avro error: ",
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(_) => "Object Store error: ",
            DataFusionError::IoError(_) => "IO error: ",
            #[cfg(feature = "sql")]
            DataFusionError::SQL(_, _) => "SQL error: ",
            DataFusionError::NotImplemented(_) => {
                "This feature is not implemented: "
            }
            DataFusionError::Internal(_) => "Internal error: ",
            DataFusionError::Plan(_) => "Error during planning: ",
            DataFusionError::Configuration(_) => {
                "Invalid or Unsupported Configuration: "
            }
            DataFusionError::SchemaError(_, _) => "Schema error: ",
            DataFusionError::Execution(_) => "Execution error: ",
            DataFusionError::ExecutionJoin(_) => "ExecutionJoin error: ",
            DataFusionError::ResourcesExhausted(_) => {
                "Resources exhausted: "
            }
            DataFusionError::External(_) => "External error: ",
            DataFusionError::Context(_, _) => "",
            DataFusionError::Substrait(_) => "Substrait error: ",
            DataFusionError::Diagnostic(_, _) => "",
            DataFusionError::Collection(errs) => {
                errs.first().expect("cannot construct DataFusionError::Collection with 0 errors, but got one such case").error_prefix()
            }
            DataFusionError::Shared(_) => "",
        }
    }

    pub fn message(&self) -> Cow<'_, str> {
        match *self {
            DataFusionError::ArrowError(ref desc, ref backtrace) => {
                let backtrace = backtrace.clone().unwrap_or_else(|| "".to_owned());
                Cow::Owned(format!("{desc}{backtrace}"))
            }
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(ref desc) => Cow::Owned(desc.to_string()),
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::IoError(ref desc) => Cow::Owned(desc.to_string()),
            #[cfg(feature = "sql")]
            DataFusionError::SQL(ref desc, ref backtrace) => {
                let backtrace: String =
                    backtrace.clone().unwrap_or_else(|| "".to_owned());
                Cow::Owned(format!("{desc:?}{backtrace}"))
            }
            DataFusionError::Configuration(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::NotImplemented(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::Internal(ref desc) => Cow::Owned(format!(
                "{desc}.\nThis issue was likely caused by a bug in DataFusion's code. \
                Please help us to resolve this by filing a bug report in our issue tracker: \
                https://github.com/apache/datafusion/issues"
            )),
            DataFusionError::Plan(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::SchemaError(ref desc, ref backtrace) => {
                let backtrace: &str =
                    &backtrace.as_ref().clone().unwrap_or_else(|| "".to_owned());
                Cow::Owned(format!("{desc}{backtrace}"))
            }
            DataFusionError::Execution(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::ExecutionJoin(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::ResourcesExhausted(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::External(ref desc) => Cow::Owned(desc.to_string()),
            #[cfg(feature = "object_store")]
            DataFusionError::ObjectStore(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::Context(ref desc, ref err) => {
                Cow::Owned(format!("{desc}\ncaused by\n{}", *err))
            }
            DataFusionError::Substrait(ref desc) => Cow::Owned(desc.to_string()),
            DataFusionError::Diagnostic(_, ref err) => Cow::Owned(err.to_string()),
            // Returning the message of the first error is probably fine enough,
            // and makes `DataFusionError::Collection` a transparent wrapped,
            // unless the end user explicitly calls `DataFusionError::iter`.
            DataFusionError::Collection(ref errs) => errs
                .first()
                .expect("cannot construct DataFusionError::Collection with 0 errors")
                .message(),
            DataFusionError::Shared(ref desc) => Cow::Owned(desc.to_string()),
        }
    }

    /// Wraps the error with contextual information intended for end users
    pub fn with_diagnostic(self, diagnostic: Diagnostic) -> Self {
        Self::Diagnostic(Box::new(diagnostic), Box::new(self))
    }

    /// Wraps the error with contextual information intended for end users.
    /// Takes a function that inspects the error and returns the diagnostic to
    /// wrap it with.
    pub fn with_diagnostic_fn<F: FnOnce(&DataFusionError) -> Diagnostic>(
        self,
        f: F,
    ) -> Self {
        let diagnostic = f(&self);
        self.with_diagnostic(diagnostic)
    }

    /// Gets the [`Diagnostic`] associated with the error, if any. If there is
    /// more than one, only the outermost [`Diagnostic`] is returned.
    pub fn diagnostic(&self) -> Option<&Diagnostic> {
        struct DiagnosticsIterator<'a> {
            head: &'a DataFusionError,
        }

        impl<'a> Iterator for DiagnosticsIterator<'a> {
            type Item = &'a Diagnostic;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    if let DataFusionError::Diagnostic(diagnostics, source) = self.head {
                        self.head = source.as_ref();
                        return Some(diagnostics);
                    }

                    if let Some(source) = self
                        .head
                        .source()
                        .and_then(|source| source.downcast_ref::<DataFusionError>())
                    {
                        self.head = source;
                    } else {
                        return None;
                    }
                }
            }
        }

        DiagnosticsIterator { head: self }.next()
    }

    /// Return an iterator over this [`DataFusionError`] and any other
    /// [`DataFusionError`]s in a [`DataFusionError::Collection`].
    ///
    /// Sometimes DataFusion is able to collect multiple errors in a SQL query
    /// before terminating, e.g. across different expressions in a SELECT
    /// statements or different sides of a UNION. This method returns an
    /// iterator over all the errors in the collection.
    ///
    /// For this to work, the top-level error must be a
    /// `DataFusionError::Collection`, not something that contains it.
    pub fn iter(&self) -> impl Iterator<Item = &DataFusionError> {
        struct ErrorIterator<'a> {
            queue: VecDeque<&'a DataFusionError>,
        }

        impl<'a> Iterator for ErrorIterator<'a> {
            type Item = &'a DataFusionError;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    let popped = self.queue.pop_front()?;
                    match popped {
                        DataFusionError::Collection(errs) => self.queue.extend(errs),
                        _ => return Some(popped),
                    }
                }
            }
        }

        let mut queue = VecDeque::new();
        queue.push_back(self);
        ErrorIterator { queue }
    }
}

/// A builder for [`DataFusionError`]
///
/// This builder can be used to collect multiple errors and return them as a
/// [`DataFusionError::Collection`].
///
/// # Example: no errors
/// ```
/// # use datafusion_common::DataFusionError;
/// let mut builder = DataFusionError::builder();
/// // ok_or returns the value if no errors have been added
/// assert_eq!(builder.error_or(42).unwrap(), 42);
/// ```
///
/// # Example: with errors
/// ```
/// # use datafusion_common::{assert_contains, DataFusionError};
/// let mut builder = DataFusionError::builder();
/// builder.add_error(DataFusionError::Internal("foo".to_owned()));
/// // ok_or returns the value if no errors have been added
/// assert_contains!(
///     builder.error_or(42).unwrap_err().to_string(),
///     "Internal error: foo"
/// );
/// ```
#[derive(Debug, Default)]
pub struct DataFusionErrorBuilder(Vec<DataFusionError>);

impl DataFusionErrorBuilder {
    /// Create a new [`DataFusionErrorBuilder`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Add an error to the in progress list
    ///
    /// # Example
    /// ```
    /// # use datafusion_common::{assert_contains, DataFusionError};
    /// let mut builder = DataFusionError::builder();
    /// builder.add_error(DataFusionError::Internal("foo".to_owned()));
    /// assert_contains!(
    ///     builder.error_or(42).unwrap_err().to_string(),
    ///     "Internal error: foo"
    /// );
    /// ```
    pub fn add_error(&mut self, error: DataFusionError) {
        self.0.push(error);
    }

    /// Add an error to the in progress list, returning the builder
    ///
    /// # Example
    /// ```
    /// # use datafusion_common::{assert_contains, DataFusionError};
    /// let builder = DataFusionError::builder()
    ///     .with_error(DataFusionError::Internal("foo".to_owned()));
    /// assert_contains!(
    ///     builder.error_or(42).unwrap_err().to_string(),
    ///     "Internal error: foo"
    /// );
    /// ```
    pub fn with_error(mut self, error: DataFusionError) -> Self {
        self.0.push(error);
        self
    }

    /// Returns `Ok(ok)` if no errors were added to the builder,
    /// otherwise returns a `Result::Err`
    pub fn error_or<T>(self, ok: T) -> Result<T, DataFusionError> {
        match self.0.len() {
            0 => Ok(ok),
            1 => Err(self.0.into_iter().next().expect("length matched 1")),
            _ => Err(DataFusionError::Collection(self.0)),
        }
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

/// Assert a condition, returning `DataFusionError::Internal` on failure.
///
/// # Examples
///
/// ```text
/// assert_or_internal_err!(predicate);
/// assert_or_internal_err!(predicate, "human readable message");
/// assert_or_internal_err!(predicate, format!("details: {}", value));
/// ```
#[macro_export]
macro_rules! assert_or_internal_err {
    ($cond:expr) => {
        if !$cond {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {}",
                stringify!($cond)
            )));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {}: {}",
                stringify!($cond),
                format!($($arg)+)
            )));
        }
    };
}

/// Assert equality, returning `DataFusionError::Internal` on failure.
///
/// # Examples
///
/// ```text
/// assert_eq_or_internal_err!(actual, expected);
/// assert_eq_or_internal_err!(left_expr, right_expr, "values must match");
/// assert_eq_or_internal_err!(lhs, rhs, "metadata: {}", extra);
/// ```
#[macro_export]
macro_rules! assert_eq_or_internal_err {
    ($left:expr, $right:expr $(,)?) => {{
        let left_val = &$left;
        let right_val = &$right;
        if left_val != right_val {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {} == {} (left: {:?}, right: {:?})",
                stringify!($left),
                stringify!($right),
                left_val,
                right_val
            )));
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let left_val = &$left;
        let right_val = &$right;
        if left_val != right_val {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {} == {} (left: {:?}, right: {:?}): {}",
                stringify!($left),
                stringify!($right),
                left_val,
                right_val,
                format!($($arg)+)
            )));
        }
    }};
}

/// Assert inequality, returning `DataFusionError::Internal` on failure.
///
/// # Examples
///
/// ```text
/// assert_ne_or_internal_err!(left, right);
/// assert_ne_or_internal_err!(lhs_expr, rhs_expr, "values must differ");
/// assert_ne_or_internal_err!(a, b, "context {}", info);
/// ```
#[macro_export]
macro_rules! assert_ne_or_internal_err {
    ($left:expr, $right:expr $(,)?) => {{
        let left_val = &$left;
        let right_val = &$right;
        if left_val == right_val {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {} != {} (left: {:?}, right: {:?})",
                stringify!($left),
                stringify!($right),
                left_val,
                right_val
            )));
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let left_val = &$left;
        let right_val = &$right;
        if left_val == right_val {
            return Err(DataFusionError::Internal(format!(
                "Assertion failed: {} != {} (left: {:?}, right: {:?}): {}",
                stringify!($left),
                stringify!($right),
                left_val,
                right_val,
                format!($($arg)+)
            )));
        }
    }};
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
    ($NAME_ERR:ident, $NAME_DF_ERR: ident, $ERR:ident) => { make_error!(@inner ($), $NAME_ERR, $NAME_DF_ERR, $ERR); };
    (@inner ($d:tt), $NAME_ERR:ident, $NAME_DF_ERR:ident, $ERR:ident) => {
        ::paste::paste!{
            /// Macro wraps `$ERR` to add backtrace feature
            #[macro_export]
            macro_rules! $NAME_DF_ERR {
                ($d($d args:expr),* $d(; diagnostic=$d DIAG:expr)?) => {{
                    let err =$crate::DataFusionError::$ERR(
                        ::std::format!(
                            "{}{}",
                            ::std::format!($d($d args),*),
                            $crate::DataFusionError::get_back_trace(),
                        ).into()
                    );
                    $d (
                        let err = err.with_diagnostic($d DIAG);
                    )?
                    err
                }
            }
        }

            /// Macro wraps Err(`$ERR`) to add backtrace feature
            #[macro_export]
            macro_rules! $NAME_ERR {
                ($d($d args:expr),* $d(; diagnostic = $d DIAG:expr)?) => {{
                    let err = $crate::[<_ $NAME_DF_ERR>]!($d($d args),*);
                    $d (
                        let err = err.with_diagnostic($d DIAG);
                    )?
                    Err(err)

                }}
            }


            // Note: Certain macros are used in this  crate, but not all.
            // This macro generates a use or all of them in case they are needed
            // so we allow unused code to avoid warnings when they are not used
            #[doc(hidden)]
            #[allow(unused)]
            pub use $NAME_ERR as [<_ $NAME_ERR>];
            #[doc(hidden)]
            #[allow(unused)]
            pub use $NAME_DF_ERR as [<_ $NAME_DF_ERR>];
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

// Exposes a macro to create `DataFusionError::Configuration` with optional backtrace
make_error!(config_err, config_datafusion_err, Configuration);

// Exposes a macro to create `DataFusionError::Substrait` with optional backtrace
make_error!(substrait_err, substrait_datafusion_err, Substrait);

// Exposes a macro to create `DataFusionError::ResourcesExhausted` with optional backtrace
make_error!(resources_err, resources_datafusion_err, ResourcesExhausted);

// Exposes a macro to create `DataFusionError::SQL` with optional backtrace
#[macro_export]
macro_rules! sql_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = DataFusionError::SQL(Box::new($ERR), Some(DataFusionError::get_back_trace()));
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        err
    }};
}

// Exposes a macro to create `Err(DataFusionError::SQL)` with optional backtrace
#[macro_export]
macro_rules! sql_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = datafusion_common::sql_datafusion_err!($ERR);
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }};
}

// Exposes a macro to create `DataFusionError::ArrowError` with optional backtrace
#[macro_export]
macro_rules! arrow_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = DataFusionError::ArrowError(Box::new($ERR), Some(DataFusionError::get_back_trace()));
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        err
    }};
}

// Exposes a macro to create `Err(DataFusionError::ArrowError)` with optional backtrace
#[macro_export]
macro_rules! arrow_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {
    {
        let err = datafusion_common::arrow_datafusion_err!($ERR);
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }};
}

// Exposes a macro to create `DataFusionError::SchemaError` with optional backtrace
#[macro_export]
macro_rules! schema_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = $crate::error::DataFusionError::SchemaError(
            Box::new($ERR),
            Box::new(Some($crate::error::DataFusionError::get_back_trace())),
        );
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        err
    }};
}

// Exposes a macro to create `Err(DataFusionError::SchemaError)` with optional backtrace
#[macro_export]
macro_rules! schema_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = $crate::error::DataFusionError::SchemaError(
            Box::new($ERR),
            Box::new(Some($crate::error::DataFusionError::get_back_trace())),
        );
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }
    };
}

// To avoid compiler error when using macro in the same crate:
// macros from the current crate cannot be referred to by absolute paths
pub use schema_err as _schema_err;

/// Create a "field not found" DataFusion::SchemaError
pub fn field_not_found<R: Into<TableReference>>(
    qualifier: Option<R>,
    name: &str,
    schema: &DFSchema,
) -> DataFusionError {
    schema_datafusion_err!(SchemaError::FieldNotFound {
        field: Box::new(Column::new(qualifier, name)),
        valid_fields: schema.columns().to_vec(),
    })
}

/// Convenience wrapper over [`field_not_found`] for when there is no qualifier
pub fn unqualified_field_not_found(name: &str, schema: &DFSchema) -> DataFusionError {
    schema_datafusion_err!(SchemaError::FieldNotFound {
        field: Box::new(Column::new_unqualified(name)),
        valid_fields: schema.columns().to_vec(),
    })
}

pub fn add_possible_columns_to_diag(
    diagnostic: &mut Diagnostic,
    field: &Column,
    valid_fields: &[Column],
) {
    let field_names: Vec<String> = valid_fields
        .iter()
        .filter_map(|f| {
            if normalized_levenshtein(f.name(), field.name()) >= 0.5 {
                Some(f.flat_name())
            } else {
                None
            }
        })
        .collect();

    for name in field_names {
        diagnostic.add_note(format!("possible column {name}"), None);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::mem::size_of;
    use std::sync::Arc;

    use arrow::error::ArrowError;
    use insta::assert_snapshot;

    fn ok_result() -> Result<()> {
        Ok(())
    }

    #[test]
    fn test_assert_eq_or_internal_err_passes() -> Result<()> {
        assert_eq_or_internal_err!(1, 1);
        ok_result()
    }

    #[test]
    fn test_assert_eq_or_internal_err_fails() {
        fn check() -> Result<()> {
            assert_eq_or_internal_err!(1, 2, "expected equality");
            ok_result()
        }

        let err = check().unwrap_err();
        assert_snapshot!(
            err.to_string(),
            @r"
        Internal error: Assertion failed: 1 == 2 (left: 1, right: 2): expected equality.
        This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
        "
        );
    }

    #[test]
    fn test_assert_ne_or_internal_err_passes() -> Result<()> {
        assert_ne_or_internal_err!(1, 2);
        ok_result()
    }

    #[test]
    fn test_assert_ne_or_internal_err_fails() {
        fn check() -> Result<()> {
            assert_ne_or_internal_err!(3, 3, "values must differ");
            ok_result()
        }

        let err = check().unwrap_err();
        assert_snapshot!(
            err.to_string(),
            @r"
        Internal error: Assertion failed: 3 != 3 (left: 3, right: 3): values must differ.
        This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
        "
        );
    }

    #[test]
    fn test_assert_or_internal_err_passes() -> Result<()> {
        assert_or_internal_err!(true);
        assert_or_internal_err!(true, "message");
        ok_result()
    }

    #[test]
    fn test_assert_or_internal_err_fails_default() {
        fn check() -> Result<()> {
            assert_or_internal_err!(false);
            ok_result()
        }

        let err = check().unwrap_err();
        assert_snapshot!(
            err.to_string(),
            @r"
        Internal error: Assertion failed: false.
        This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
        "
        );
    }

    #[test]
    fn test_assert_or_internal_err_fails_with_message() {
        fn check() -> Result<()> {
            assert_or_internal_err!(false, "custom message");
            ok_result()
        }

        let err = check().unwrap_err();
        assert_snapshot!(
            err.to_string(),
            @r"
        Internal error: Assertion failed: false: custom message.
        This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
        "
        );
    }

    #[test]
    fn test_assert_or_internal_err_with_format_arguments() {
        fn check() -> Result<()> {
            assert_or_internal_err!(false, "custom {}", 42);
            ok_result()
        }

        let err = check().unwrap_err();
        assert_snapshot!(
            err.to_string(),
            @r"
        Internal error: Assertion failed: false: custom 42.
        This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
        "
        );
    }

    #[test]
    fn test_error_size() {
        // Since Errors influence the size of Result which influence the size of the stack
        // please don't allow this to grow larger
        assert_eq!(size_of::<SchemaError>(), 40);
        assert_eq!(size_of::<DataFusionError>(), 40);
    }

    #[test]
    fn datafusion_error_to_arrow() {
        let res = return_arrow_error().unwrap_err();
        assert!(
            res.to_string()
                .starts_with("External error: Error during planning: foo")
        );
    }

    #[test]
    fn arrow_error_to_datafusion() {
        let res = return_datafusion_error().unwrap_err();
        assert_eq!(res.strip_backtrace(), "Arrow error: Schema error: bar");
    }

    // To pass the test the environment variable RUST_BACKTRACE should be set to 1 to enforce backtrace
    #[cfg(feature = "backtrace")]
    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn test_enabled_backtrace() {
        match std::env::var("RUST_BACKTRACE") {
            Ok(val) if val == "1" => {}
            _ => panic!("Environment variable RUST_BACKTRACE must be set to 1"),
        };

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
        assert!(
            !err.split(DataFusionError::BACK_TRACE_SEP)
                .collect::<Vec<&str>>()
                .get(1)
                .unwrap()
                .is_empty()
        );
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
                Box::new(ArrowError::ExternalError(Box::new(
                    DataFusionError::ResourcesExhausted("foo".to_string()),
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
                Box::new(ArrowError::ExternalError(Box::new(
                    ArrowError::ExternalError(Box::new(
                        DataFusionError::ResourcesExhausted("foo".to_string()),
                    )),
                ))),
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

    #[test]
    fn external_error() {
        // assert not wrapping DataFusionError
        let generic_error: GenericError =
            Box::new(DataFusionError::Plan("test".to_string()));
        let datafusion_error: DataFusionError = generic_error.into();
        println!("{}", datafusion_error.strip_backtrace());
        assert_eq!(
            datafusion_error.strip_backtrace(),
            "Error during planning: test"
        );

        // assert wrapping other Error
        let generic_error: GenericError = Box::new(io::Error::other("io error"));
        let datafusion_error: DataFusionError = generic_error.into();
        println!("{}", datafusion_error.strip_backtrace());
        assert_eq!(
            datafusion_error.strip_backtrace(),
            "External error: io error"
        );
    }

    #[test]
    fn external_error_no_recursive() {
        let generic_error_1: GenericError = Box::new(io::Error::other("io error"));
        let external_error_1: DataFusionError = generic_error_1.into();
        let generic_error_2: GenericError = Box::new(external_error_1);
        let external_error_2: DataFusionError = generic_error_2.into();

        println!("{external_error_2}");
        assert!(
            external_error_2
                .to_string()
                .starts_with("External error: io error")
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
    fn return_datafusion_error() -> Result<()> {
        // Expect the '?' to work
        Err(ArrowError::SchemaError("bar".to_string()).into())
    }

    fn do_root_test(e: DataFusionError, exp: DataFusionError) {
        let e = e.find_root();

        // DataFusionError does not implement Eq, so we use a string comparison + some cheap "same variant" test instead
        assert_eq!(e.strip_backtrace(), exp.strip_backtrace());
        assert_eq!(std::mem::discriminant(e), std::mem::discriminant(&exp),)
    }

    #[test]
    fn test_iter() {
        let err = DataFusionError::Collection(vec![
            DataFusionError::Plan("a".to_string()),
            DataFusionError::Collection(vec![
                DataFusionError::Plan("b".to_string()),
                DataFusionError::Plan("c".to_string()),
            ]),
        ]);
        let errs = err.iter().collect::<Vec<_>>();
        assert_eq!(errs.len(), 3);
        assert_eq!(errs[0].strip_backtrace(), "Error during planning: a");
        assert_eq!(errs[1].strip_backtrace(), "Error during planning: b");
        assert_eq!(errs[2].strip_backtrace(), "Error during planning: c");
    }
}
