// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{Display, Formatter};

use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::SecondsFormat;

use crate::ScalarValue;

/// Selects which state of a table to read, such as a specific version for a
/// time travel query.
///
/// A selector plays two roles:
///
/// * As a request, it is what a query asks for. It is passed to
///   `SchemaProvider::table_with_selector` to look up the table.
/// * As an identity, it is what a table provider actually reads, as reported
///   by `TableProvider::selector`. This may differ from the request: for
///   example, a [`TableSelector::Timestamp`] may resolve to the
///   [`TableSelector::Version`] of the latest snapshot committed before it.
///
/// See `SqlToRel::table_selector` for how SQL time travel clauses are mapped
/// to selectors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum TableSelector {
    /// A catalog specific version identifier, such as a snapshot id, a
    /// version number, a tag or a branch name.
    Version(ScalarValue),
    /// A point in time, in nanoseconds since the Unix epoch (UTC).
    Timestamp(i64),
}

impl Display for TableSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Strings are quoted, to tell them apart from numbers and
            // timestamps
            TableSelector::Version(v) => match v.try_as_str() {
                Some(Some(s)) => write!(f, "{s:?}"),
                _ => write!(f, "{v}"),
            },
            TableSelector::Timestamp(ns) => match timestamp_ns_to_datetime(*ns) {
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
            TableSelector::Version(ScalarValue::Int64(Some(3))).to_string(),
            "3"
        );
        assert_eq!(
            TableSelector::Version(ScalarValue::from("v1")).to_string(),
            "\"v1\""
        );
        assert_eq!(
            TableSelector::Timestamp(1_704_067_200_000_000_000).to_string(),
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(
            TableSelector::Version(ScalarValue::from("a \"quoted\" tag")).to_string(),
            r#""a \"quoted\" tag""#
        );
        assert_eq!(
            TableSelector::Version(ScalarValue::Utf8View(Some("3".into()))).to_string(),
            "\"3\""
        );
    }
}
