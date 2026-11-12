// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{Display, Formatter};

use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::SecondsFormat;

use crate::ScalarValue;

/// Selects which read of a table to perform, such as reading the table as of
/// a version or point in time for a time travel query.
///
/// A selector typically picks a point in the history of a table, such as a
/// snapshot. It determines *which* data is read, not *how* it is read: hints
/// about how to scan a table do not belong in a selector.
///
/// A selector plays two roles:
///
/// * As a request, it is what a query asks for. It is passed to
///   [`SchemaProvider::table_with_read_selector`] to look up the table.
/// * As an identity, it is what a table provider actually reads, as reported
///   by [`TableProvider::read_selector`]. This may differ from the request: for
///   example, a [`TableReadSelector::AsOfTimestamp`] may resolve to the
///   [`TableReadSelector::AsOfVersion`] of the latest snapshot committed at
///   or before it.
///
/// Scans of the same table with equal selectors are assumed to read the same
/// data. The schema of that data is determined by the table provider, so it
/// may differ between selectors, for example after the schema of the table
/// evolved.
///
/// See [`SqlToRel::read_selector`] for how SQL time travel clauses are
/// mapped to selectors.
///
/// [`SchemaProvider::table_with_read_selector`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html#method.table_with_read_selector
/// [`TableProvider::read_selector`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#method.read_selector
/// [`SqlToRel::read_selector`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html#method.read_selector
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum TableReadSelector {
    /// The state of the table as of a version: a catalog specific version
    /// identifier, such as a snapshot id, a version number, a tag or a branch
    /// name. It may identify the state of the whole catalog, such as a commit
    /// hash.
    ///
    /// As a request, a name such as a tag or a branch selects the version it
    /// points to when the query is planned. As an identity, it is exactly the
    /// version that is read.
    AsOfVersion(ScalarValue),
    /// The state of the table as of a point in time, in nanoseconds since the
    /// Unix epoch (UTC).
    ///
    /// As a request, it selects the latest state committed at or before this
    /// time, so a commit made exactly at this time is included. As an
    /// identity, it is the state of the table at this time.
    AsOfTimestamp(i64),
}

impl Display for TableReadSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Strings are quoted, to tell them apart from numbers and
            // timestamps
            TableReadSelector::AsOfVersion(v) => match v.try_as_str() {
                Some(Some(s)) => write!(f, "{s:?}"),
                _ => write!(f, "{v}"),
            },
            TableReadSelector::AsOfTimestamp(ns) => match timestamp_ns_to_datetime(*ns) {
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
            TableReadSelector::AsOfVersion(ScalarValue::Int64(Some(3))).to_string(),
            "3"
        );
        assert_eq!(
            TableReadSelector::AsOfVersion(ScalarValue::from("v1")).to_string(),
            "\"v1\""
        );
        assert_eq!(
            TableReadSelector::AsOfTimestamp(1_704_067_200_000_000_000).to_string(),
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(
            TableReadSelector::AsOfVersion(ScalarValue::from("a \"quoted\" tag"))
                .to_string(),
            r#""a \"quoted\" tag""#
        );
        assert_eq!(
            TableReadSelector::AsOfVersion(ScalarValue::Utf8View(Some("3".into())))
                .to_string(),
            "\"3\""
        );
    }
}
