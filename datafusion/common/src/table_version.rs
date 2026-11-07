// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::SecondsFormat;

use crate::ScalarValue;

/// A specific version of a table, as requested by a time travel query.
///
/// Time travel queries read a table as it existed at some point in the past.
/// SQL dialects use a variety of syntaxes for this, all of which are
/// normalized to one of the two variants below. For example:
///
/// | SQL                                          | `TableVersion` |
/// |----------------------------------------------|----------------|
/// | `t VERSION AS OF 3`                          | `Version`      |
/// | `t AT (VERSION => 'v1')`                     | `Version`      |
/// | `t TIMESTAMP AS OF '2024-01-01'`             | `Timestamp`    |
/// | `t FOR SYSTEM_TIME AS OF now()`              | `Timestamp`    |
/// | `t AT (TIMESTAMP => now() - INTERVAL '1 h')` | `Timestamp`    |
///
/// The version expression is evaluated during planning, so catalogs only
/// ever see the resulting constant value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum TableVersion {
    /// A catalog specific version identifier, such as a snapshot id, a
    /// version number, a tag or a branch name.
    Version(ScalarValue),
    /// A point in time, in nanoseconds since the Unix epoch (UTC).
    Timestamp(i64),
}

impl TableVersion {
    /// Returns the value of this version as a [`ScalarValue`].
    ///
    /// For [`TableVersion::Timestamp`] this is a
    /// [`ScalarValue::TimestampNanosecond`] in the UTC time zone.
    pub fn value(&self) -> ScalarValue {
        match self {
            TableVersion::Version(v) => v.clone(),
            TableVersion::Timestamp(ns) => {
                ScalarValue::TimestampNanosecond(Some(*ns), Some(Arc::from("UTC")))
            }
        }
    }
}

impl Display for TableVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Strings are quoted, to tell them apart from numbers and
            // timestamps
            TableVersion::Version(v) => match v.try_as_str() {
                Some(Some(s)) => write!(f, "{s:?}"),
                _ => write!(f, "{v}"),
            },
            TableVersion::Timestamp(ns) => match timestamp_ns_to_datetime(*ns) {
                Some(dt) => write!(
                    f,
                    "{}",
                    dt.and_utc().to_rfc3339_opts(SecondsFormat::AutoSi, true)
                ),
                None => write!(f, "{ns}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display() {
        assert_eq!(
            TableVersion::Version(ScalarValue::Int64(Some(3))).to_string(),
            "3"
        );
        assert_eq!(
            TableVersion::Version(ScalarValue::from("v1")).to_string(),
            "\"v1\""
        );
        assert_eq!(
            TableVersion::Timestamp(1_704_067_200_000_000_000).to_string(),
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(
            TableVersion::Version(ScalarValue::from("a \"quoted\" tag")).to_string(),
            r#""a \"quoted\" tag""#
        );
        assert_eq!(
            TableVersion::Version(ScalarValue::Utf8View(Some("3".into()))).to_string(),
            "\"3\""
        );
    }
}
