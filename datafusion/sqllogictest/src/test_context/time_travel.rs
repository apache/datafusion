// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Result, ScalarValue, TableVersion, plan_err};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::SessionContext;

// ==============================================================================
// Versioned Schema Provider (sqllogictest-only)
// ==============================================================================

/// A version of table `t`
struct Version {
    version: i64,
    /// Commit time, in nanoseconds since the epoch
    committed: i64,
    rows: &'static [(i64, &'static str)],
}

/// The versions of table `t`
const VERSIONS: [Version; 3] = [
    Version {
        version: 1,
        committed: 1_704_067_200_000_000_000, // 2024-01-01T00:00:00Z
        rows: &[(1, "a")],
    },
    Version {
        version: 2,
        committed: 1_706_745_600_000_000_000, // 2024-02-01T00:00:00Z
        rows: &[(1, "a"), (2, "b")],
    },
    Version {
        version: 3,
        committed: 1_709_251_200_000_000_000, // 2024-03-01T00:00:00Z
        rows: &[(1, "a"), (2, "b"), (3, "c")],
    },
];

/// Named versions (tags) of table `t`
const TAGS: [(&str, i64); 2] = [("first", 1), ("latest", 3)];

/// A schema with a single table `t` that supports time travel queries.
///
/// The table has three versions, which can be selected by version number, by
/// tag or by timestamp. Time travel on table `no_version` returns a table that
/// does not report its version, which is invalid.
#[derive(Debug)]
struct VersionedSchema {}

impl VersionedSchema {
    fn table_version(version: i64) -> Result<Arc<dyn TableProvider>> {
        let Some(Version { rows, .. }) = VERSIONS.iter().find(|v| v.version == version)
        else {
            return plan_err!("Table t has no version {version}");
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, name)| *name),
                )),
            ],
        )?;
        // Report the version that is read, which may differ from the requested
        // version, e.g. when it was requested by tag or timestamp
        let table = MemTable::try_new(schema, vec![vec![batch]])?.with_version(Some(
            TableVersion::Version(ScalarValue::Int64(Some(version))),
        ));
        Ok(Arc::new(table))
    }
}

#[async_trait]
impl SchemaProvider for VersionedSchema {
    fn table_names(&self) -> Vec<String> {
        vec!["t".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if name != "t" {
            return Ok(None);
        }
        Self::table_version(VERSIONS[VERSIONS.len() - 1].version).map(Some)
    }

    async fn table_at(
        &self,
        name: &str,
        version: &TableVersion,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if name == "no_version" {
            // Invalid: the provider does not report the version it reads
            let schema =
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            return Ok(Some(Arc::new(MemTable::try_new(schema, vec![vec![]])?)));
        }
        if name != "t" {
            return Ok(None);
        }
        let version = match version {
            TableVersion::Version(value) => {
                if let Some(Some(tag)) = value.try_as_str() {
                    match TAGS.iter().find(|(t, _)| *t == tag) {
                        Some((_, version)) => *version,
                        None => return plan_err!("Table t has no tag '{tag}'"),
                    }
                } else {
                    match value.cast_to(&DataType::Int64)? {
                        ScalarValue::Int64(Some(version)) => version,
                        _ => return plan_err!("Invalid version {value}"),
                    }
                }
            }
            TableVersion::Timestamp(ts) => {
                match VERSIONS.iter().rev().find(|v| v.committed <= *ts) {
                    Some(v) => v.version,
                    None => return plan_err!("Table t did not exist at {version}"),
                }
            }
        };
        Self::table_version(version).map(Some)
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "t"
    }
}

/// Registers the schema `versioned`, which contains a table `t` that supports
/// time travel queries
pub(super) fn register_versioned_schema(ctx: &SessionContext) {
    ctx.catalog("datafusion")
        .expect("default catalog should exist")
        .register_schema("versioned", Arc::new(VersionedSchema {}))
        .expect("versioned schema registration should succeed");
}
