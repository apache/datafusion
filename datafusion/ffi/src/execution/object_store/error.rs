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

//! FFI-safe representation of [`object_store::Error`].
//!
//! The *variant* of an object store error is preserved across the boundary,
//! not just its message, so code that branches on the variant behaves the same
//! either side. That matters for callers implementing optimistic concurrency
//! control on top of [`ObjectStore::rename_if_not_exists`] or
//! [`PutMode::Create`], which detect a conflicting write via
//! [`object_store::Error::AlreadyExists`], and for conditional reads, which
//! rely on [`object_store::Error::NotModified`] and
//! [`object_store::Error::Precondition`].
//!
//! The `source` of an error is a `Box<dyn std::error::Error>` and cannot cross
//! the boundary, so it is rendered to a string. The chain of causes is
//! therefore flattened into the message but never dropped entirely.
//!
//! [`ObjectStore::rename_if_not_exists`]: object_store::ObjectStore::rename_if_not_exists
//! [`PutMode::Create`]: object_store::PutMode::Create

use object_store::Error as ObjectStoreError;
use object_store::path::Error as PathError;
use stabby::string::String as SString;

/// The variant of an [`object_store::Error`], preserved across the FFI
/// boundary.
///
/// Values are explicitly assigned so that adding a variant cannot silently
/// renumber the existing ones.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(non_camel_case_types)]
pub enum FFI_ObjectStoreErrorKind {
    Generic = 0,
    NotFound = 1,
    InvalidPath = 2,
    JoinError = 3,
    NotSupported = 4,
    AlreadyExists = 5,
    Precondition = 6,
    NotModified = 7,
    NotImplemented = 8,
    PermissionDenied = 9,
    Unauthenticated = 10,
    UnknownConfigurationKey = 11,
}

/// A stable struct for sharing [`object_store::Error`] across FFI boundaries.
///
/// `path` carries the location for the variants that have one. For
/// [`FFI_ObjectStoreErrorKind::UnknownConfigurationKey`] it carries the
/// configuration key and for [`FFI_ObjectStoreErrorKind::NotImplemented`] it
/// carries the operation, since those variants use those fields in place of a
/// path.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ObjectStoreError {
    /// Which [`object_store::Error`] variant this is.
    pub kind: FFI_ObjectStoreErrorKind,

    /// The path, configuration key, or operation associated with the error.
    /// Empty when the variant has none.
    pub path: SString,

    /// The rendered source error, or the store / implementer name for the
    /// variants that carry one instead of a source.
    pub message: SString,
}

impl From<&ObjectStoreError> for FFI_ObjectStoreError {
    fn from(err: &ObjectStoreError) -> Self {
        use FFI_ObjectStoreErrorKind as Kind;

        let (kind, path, message) = match err {
            ObjectStoreError::Generic { store, source } => {
                (Kind::Generic, (*store).to_owned(), source.to_string())
            }
            ObjectStoreError::NotFound { path, source } => {
                (Kind::NotFound, path.clone(), source.to_string())
            }
            ObjectStoreError::InvalidPath { source } => {
                (Kind::InvalidPath, String::new(), source.to_string())
            }
            ObjectStoreError::NotSupported { source } => {
                (Kind::NotSupported, String::new(), source.to_string())
            }
            ObjectStoreError::AlreadyExists { path, source } => {
                (Kind::AlreadyExists, path.clone(), source.to_string())
            }
            ObjectStoreError::Precondition { path, source } => {
                (Kind::Precondition, path.clone(), source.to_string())
            }
            ObjectStoreError::NotModified { path, source } => {
                (Kind::NotModified, path.clone(), source.to_string())
            }
            ObjectStoreError::NotImplemented {
                operation,
                implementer,
            } => (Kind::NotImplemented, operation.clone(), implementer.clone()),
            ObjectStoreError::PermissionDenied { path, source } => {
                (Kind::PermissionDenied, path.clone(), source.to_string())
            }
            ObjectStoreError::Unauthenticated { path, source } => {
                (Kind::Unauthenticated, path.clone(), source.to_string())
            }
            ObjectStoreError::UnknownConfigurationKey { store, key } => (
                Kind::UnknownConfigurationKey,
                key.clone(),
                (*store).to_owned(),
            ),
            // `JoinError` is `#[cfg(feature = "tokio")]` and any variant added
            // to `object_store::Error` in a future release lands here. Preserve
            // the rendered message under the fallback variant.
            other => (Kind::Generic, String::new(), other.to_string()),
        };

        Self {
            kind,
            path: path.as_str().into(),
            message: message.as_str().into(),
        }
    }
}

impl From<ObjectStoreError> for FFI_ObjectStoreError {
    fn from(err: ObjectStoreError) -> Self {
        Self::from(&err)
    }
}

impl From<FFI_ObjectStoreError> for ObjectStoreError {
    fn from(err: FFI_ObjectStoreError) -> Self {
        use FFI_ObjectStoreErrorKind as Kind;

        let path = err.path.to_string();
        let message = err.message.to_string();
        // `source` is a boxed error, so the original chain is rebuilt as a
        // single opaque string error carrying the rendered chain.
        let source = || -> Box<dyn std::error::Error + Send + Sync + 'static> {
            message.clone().into()
        };

        match err.kind {
            Kind::Generic => ObjectStoreError::Generic {
                // `store` is a `&'static str`, so the original value cannot be
                // rebuilt. Fold it into the message instead of leaking it.
                store: "ForeignObjectStore",
                source: if path.is_empty() {
                    source()
                } else {
                    format!("{path}: {message}").into()
                },
            },
            Kind::NotFound => ObjectStoreError::NotFound {
                path,
                source: source(),
            },
            Kind::InvalidPath => ObjectStoreError::InvalidPath {
                source: PathError::InvalidPath {
                    path: message.into(),
                },
            },
            // `tokio::task::JoinError` cannot be constructed outside tokio, so
            // a join failure surfaces as a generic error with its message.
            Kind::JoinError | Kind::NotSupported => {
                ObjectStoreError::NotSupported { source: source() }
            }
            Kind::AlreadyExists => ObjectStoreError::AlreadyExists {
                path,
                source: source(),
            },
            Kind::Precondition => ObjectStoreError::Precondition {
                path,
                source: source(),
            },
            Kind::NotModified => ObjectStoreError::NotModified {
                path,
                source: source(),
            },
            Kind::NotImplemented => ObjectStoreError::NotImplemented {
                operation: path,
                implementer: message,
            },
            Kind::PermissionDenied => ObjectStoreError::PermissionDenied {
                path,
                source: source(),
            },
            Kind::Unauthenticated => ObjectStoreError::Unauthenticated {
                path,
                source: source(),
            },
            Kind::UnknownConfigurationKey => ObjectStoreError::UnknownConfigurationKey {
                store: "ForeignObjectStore",
                key: path,
            },
        }
    }
}

/// An FFI-safe result carrying an [`FFI_ObjectStoreError`].
///
/// This mirrors [`crate::util::FFI_Result`] but preserves the object store
/// error variant rather than reducing it to a message.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_ObjectStoreResult<T> {
    Ok(T),
    Err(FFI_ObjectStoreError),
}

impl<T> From<Result<T, ObjectStoreError>> for FFI_ObjectStoreResult<T> {
    fn from(res: Result<T, ObjectStoreError>) -> Self {
        match res {
            Ok(v) => FFI_ObjectStoreResult::Ok(v),
            Err(e) => FFI_ObjectStoreResult::Err(e.into()),
        }
    }
}

impl<T> From<FFI_ObjectStoreResult<T>> for Result<T, ObjectStoreError> {
    fn from(res: FFI_ObjectStoreResult<T>) -> Self {
        match res {
            FFI_ObjectStoreResult::Ok(v) => Ok(v),
            FFI_ObjectStoreResult::Err(e) => Err(e.into()),
        }
    }
}

impl<T> FFI_ObjectStoreResult<T> {
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> FFI_ObjectStoreResult<U> {
        match self {
            FFI_ObjectStoreResult::Ok(v) => FFI_ObjectStoreResult::Ok(f(v)),
            FFI_ObjectStoreResult::Err(e) => FFI_ObjectStoreResult::Err(e),
        }
    }
}

/// Convert a [`Result<T, object_store::Error>`] into an
/// [`FFI_ObjectStoreResult`], returning early on error. Mirrors
/// [`crate::sresult_return`], which cannot be used here because it discards the
/// error variant.
#[macro_export]
macro_rules! os_result_return {
    ( $x:expr ) => {
        match $x {
            Ok(v) => v,
            Err(e) => {
                return $crate::execution::object_store::FFI_ObjectStoreResult::Err(
                    e.into(),
                );
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must survive the round trip, because callers branch on
    /// the variant to implement conditional writes.
    #[test]
    fn error_kind_round_trip() {
        use FFI_ObjectStoreErrorKind as Kind;

        let cases: Vec<(ObjectStoreError, FFI_ObjectStoreErrorKind)> = vec![
            (
                ObjectStoreError::NotFound {
                    path: "a/b".into(),
                    source: "missing".into(),
                },
                FFI_ObjectStoreErrorKind::NotFound,
            ),
            (
                ObjectStoreError::AlreadyExists {
                    path: "a/b".into(),
                    source: "exists".into(),
                },
                FFI_ObjectStoreErrorKind::AlreadyExists,
            ),
            (
                ObjectStoreError::Precondition {
                    path: "a/b".into(),
                    source: "etag".into(),
                },
                FFI_ObjectStoreErrorKind::Precondition,
            ),
            (
                ObjectStoreError::NotModified {
                    path: "a/b".into(),
                    source: "same".into(),
                },
                FFI_ObjectStoreErrorKind::NotModified,
            ),
            (
                ObjectStoreError::PermissionDenied {
                    path: "a/b".into(),
                    source: "denied".into(),
                },
                FFI_ObjectStoreErrorKind::PermissionDenied,
            ),
            (
                ObjectStoreError::Unauthenticated {
                    path: "a/b".into(),
                    source: "no creds".into(),
                },
                FFI_ObjectStoreErrorKind::Unauthenticated,
            ),
            (
                ObjectStoreError::NotSupported {
                    source: "nope".into(),
                },
                FFI_ObjectStoreErrorKind::NotSupported,
            ),
            (
                ObjectStoreError::NotImplemented {
                    operation: "put_multipart_opts".into(),
                    implementer: "TestStore".into(),
                },
                FFI_ObjectStoreErrorKind::NotImplemented,
            ),
            (
                ObjectStoreError::UnknownConfigurationKey {
                    store: "S3",
                    key: "bogus".into(),
                },
                FFI_ObjectStoreErrorKind::UnknownConfigurationKey,
            ),
            (
                ObjectStoreError::Generic {
                    store: "S3",
                    source: "boom".into(),
                },
                FFI_ObjectStoreErrorKind::Generic,
            ),
        ];

        for (original, expected_kind) in cases {
            let ffi = FFI_ObjectStoreError::from(&original);
            assert_eq!(ffi.kind, expected_kind, "for {original}");

            let restored = ObjectStoreError::from(ffi.clone());
            let restored_ffi = FFI_ObjectStoreError::from(&restored);
            assert_eq!(
                restored_ffi.kind, expected_kind,
                "kind changed on rebuild for {original}"
            );

            match expected_kind {
                // `Generic` carries a `store: &'static str` rather than a path.
                // A `&'static str` cannot be rebuilt on the far side, so the
                // originating store name is folded into the message instead.
                Kind::Generic => assert!(
                    restored.to_string().contains("S3"),
                    "originating store name should survive in the message, got {restored}"
                ),
                // Every other variant carries a path (or, for
                // `UnknownConfigurationKey` and `NotImplemented`, the field that
                // takes its place), and callers surface it to users.
                _ => {
                    assert_eq!(restored_ffi.path, ffi.path, "path changed for {original}")
                }
            }
        }
    }

    #[test]
    fn error_preserves_path_and_message() {
        let original = ObjectStoreError::AlreadyExists {
            path: "_delta_log/00000000000000000001.json".into(),
            source: "conditional put failed".into(),
        };

        let restored = ObjectStoreError::from(FFI_ObjectStoreError::from(&original));

        let ObjectStoreError::AlreadyExists { path, source } = &restored else {
            panic!("expected AlreadyExists, got {restored:?}");
        };
        assert_eq!(path, "_delta_log/00000000000000000001.json");
        assert_eq!(source.to_string(), "conditional put failed");
    }

    #[test]
    fn result_round_trip() {
        let ok: FFI_ObjectStoreResult<u32> = Ok(7).into();
        let ok: Result<u32, ObjectStoreError> = ok.into();
        assert_eq!(ok.unwrap(), 7);

        let err: FFI_ObjectStoreResult<u32> = Err(ObjectStoreError::NotFound {
            path: "x".into(),
            source: "gone".into(),
        })
        .into();
        let err: Result<u32, ObjectStoreError> = err.into();
        assert!(matches!(
            err.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));
    }
}
