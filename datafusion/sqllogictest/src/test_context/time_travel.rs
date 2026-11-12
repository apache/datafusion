// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Result, ScalarValue, TableReadSelector, plan_err};
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

/// The latest version of table `evolving`
const EVOLVING_VERSIONS: i64 = 5;

/// A schema with tables that support time travel queries.
///
/// Table `t` has three versions, which can be selected by version number, by
/// tag or by timestamp. Table `evolving` has five versions with different
/// schemas, as created by the schema evolution of table formats such as
/// Iceberg, and can be selected by version number. Time travel on table
/// `no_version` returns a table that does not report its version, which is
/// invalid.
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
        let table =
            MemTable::try_new(schema, vec![vec![batch]])?.with_read_selector(Some(
                TableReadSelector::AsOfVersion(ScalarValue::Int64(Some(version))),
            ));
        Ok(Arc::new(table))
    }

    /// Returns a version of table `evolving`, in which:
    ///
    /// 1. the table is created as `(id INT, name VARCHAR NOT NULL, score FLOAT)`
    /// 2. column `email` is added
    /// 3. column `name` is renamed to `full_name`, and `id` and `score` are
    ///    widened to `BIGINT` and `DOUBLE`
    /// 4. column `score` is dropped, `full_name` becomes nullable and struct
    ///    column `address` is added
    /// 5. column `email` is moved first, and field `zip` is added to `address`
    fn evolving_version(version: i64) -> Result<Arc<dyn TableProvider>> {
        let emails = || -> ArrayRef {
            Arc::new(StringArray::from(vec![
                None,
                Some("b@example.com"),
                Some("c@example.com"),
            ]))
        };
        let address = |zips: bool| -> ArrayRef {
            let mut fields = vec![(
                Arc::new(Field::new("city", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![
                    Some("Ghent"),
                    Some("Leuven"),
                    None,
                    Some("Brussels"),
                ])) as ArrayRef,
            )];
            if zips {
                fields.push((
                    Arc::new(Field::new("zip", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec![
                        Some("9000"),
                        Some("3000"),
                        None,
                        Some("1000"),
                    ])) as ArrayRef,
                ));
            }
            Arc::new(StructArray::from(fields))
        };
        let address_type = |zips: bool| {
            let mut fields = vec![Field::new("city", DataType::Utf8, true)];
            if zips {
                fields.push(Field::new("zip", DataType::Utf8, true));
            }
            DataType::Struct(Fields::from(fields))
        };
        let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = match version {
            1 => (
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("score", DataType::Float32, false),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["a", "b"])),
                    Arc::new(Float32Array::from(vec![1.5, 2.5])),
                ],
            ),
            2 => (
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("score", DataType::Float32, false),
                    Field::new("email", DataType::Utf8, true),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(Float32Array::from(vec![1.5, 2.5, 3.5])),
                    emails(),
                ],
            ),
            3 => (
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("full_name", DataType::Utf8, false),
                    Field::new("score", DataType::Float64, false),
                    Field::new("email", DataType::Utf8, true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
                    emails(),
                ],
            ),
            4 => (
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("full_name", DataType::Utf8, true),
                    Field::new("email", DataType::Utf8, true),
                    Field::new("address", address_type(false), true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(StringArray::from(vec![
                        Some("a"),
                        Some("b"),
                        Some("c"),
                        None,
                    ])),
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("b@example.com"),
                        Some("c@example.com"),
                        Some("d@example.com"),
                    ])),
                    address(false),
                ],
            ),
            5 => (
                vec![
                    Field::new("email", DataType::Utf8, true),
                    Field::new("id", DataType::Int64, false),
                    Field::new("full_name", DataType::Utf8, true),
                    Field::new("address", address_type(true), true),
                ],
                vec![
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("b@example.com"),
                        Some("c@example.com"),
                        Some("d@example.com"),
                    ])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(StringArray::from(vec![
                        Some("a"),
                        Some("b"),
                        Some("c"),
                        None,
                    ])),
                    address(true),
                ],
            ),
            _ => return plan_err!("Table evolving has no version {version}"),
        };
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let table =
            MemTable::try_new(schema, vec![vec![batch]])?.with_read_selector(Some(
                TableReadSelector::AsOfVersion(ScalarValue::Int64(Some(version))),
            ));
        Ok(Arc::new(table))
    }
}

#[async_trait]
impl SchemaProvider for VersionedSchema {
    fn table_names(&self) -> Vec<String> {
        vec!["t".to_string(), "evolving".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match name {
            "t" => Self::table_version(VERSIONS[VERSIONS.len() - 1].version).map(Some),
            "evolving" => Self::evolving_version(EVOLVING_VERSIONS).map(Some),
            _ => Ok(None),
        }
    }

    async fn table_with_read_selector(
        &self,
        name: &str,
        selector: &TableReadSelector,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if name == "no_version" {
            // Invalid: the provider does not report the selector it reads
            let schema =
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            return Ok(Some(Arc::new(MemTable::try_new(schema, vec![vec![]])?)));
        }
        if name == "evolving" {
            return match selector {
                TableReadSelector::AsOfVersion(value) => {
                    match value.cast_to(&DataType::Int64)? {
                        ScalarValue::Int64(Some(version)) => {
                            Self::evolving_version(version).map(Some)
                        }
                        _ => plan_err!("Invalid version {value}"),
                    }
                }
                _ => plan_err!("Table read selector {selector} is not supported"),
            };
        }
        if name != "t" {
            return Ok(None);
        }
        let version = match selector {
            TableReadSelector::AsOfVersion(value) => {
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
            TableReadSelector::AsOfTimestamp(ts) => {
                match VERSIONS.iter().rev().find(|v| v.committed <= *ts) {
                    Some(v) => v.version,
                    None => return plan_err!("Table t did not exist at {selector}"),
                }
            }
        };
        Self::table_version(version).map(Some)
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "t" || name == "evolving"
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
