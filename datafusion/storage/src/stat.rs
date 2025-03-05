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

use crate::ReadRange;
use chrono::{DateTime, Utc};

pub struct StorageStatOptions {
    /// Request will succeed if the `StorageFileMetadata::e_tag` matches
    /// otherwise returning [`Error::Precondition`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-Match: "xyzzy"
    /// If-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-Match: *
    /// ```
    pub if_match: Option<String>,
    /// Request will succeed if the `StorageFileMetadata::e_tag` does not match
    /// otherwise returning [`Error::NotModified`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-None-Match: "xyzzy"
    /// If-None-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-None-Match: *
    /// ```
    pub if_none_match: Option<String>,
    /// Request will succeed if the object has been modified since
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
    pub if_modified_since: Option<DateTime<Utc>>,
    /// Request will succeed if the object has not been modified since
    /// otherwise returning [`Error::Precondition`]
    ///
    /// Some stores, such as S3, will only return `NotModified` for exact
    /// timestamp matches, instead of for any timestamp greater than or equal.
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
    pub if_unmodified_since: Option<DateTime<Utc>>,
    /// Request a particular object version
    pub version: Option<String>,
}
