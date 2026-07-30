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

//! Protobuf conversions for the file-scan leaf types owned by this crate:
//! [`FileRange`], [`PartitionedFile`] and [`FileGroup`].
//!
//! These are the single copy of that wire logic. `datafusion-proto`'s
//! `TryFromProto` implementations for the same types are thin shims that
//! delegate here, so the format cannot drift between the central serializer and
//! the per-source `try_to_proto` hooks.
//!
//! None of these conversions need a codec or an encode/decode context: every
//! field is plain data or goes through `datafusion-proto-common`. That is why
//! they are plain [`TryFrom`] impls rather than the `try_to_proto(ctx)` /
//! `try_from_proto(node, ctx)` hooks used for plans, expressions and scan
//! configs: the standard trait can express a conversion that takes nothing but
//! the value, and the orphan rule allows it here because one side of each
//! conversion is a type this crate owns.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_proto_models::protobuf;
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::file_groups::FileGroup;
use crate::{FileRange, PartitionedFile};

impl TryFrom<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from(range: &FileRange) -> Result<Self> {
        Ok(protobuf::FileRange {
            start: range.start,
            end: range.end,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from(range: &protobuf::FileRange) -> Result<Self> {
        Ok(FileRange {
            start: range.start,
            end: range.end,
        })
    }
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from(file: &PartitionedFile) -> Result<Self> {
        let last_modified = file.object_meta.last_modified;
        let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
            ))
        })? as u64;
        Ok(protobuf::PartitionedFile {
            arrow_schema: file
                .arrow_schema
                .as_ref()
                .map(|s| s.as_ref().try_into())
                .transpose()?,
            path: file.object_meta.location.as_ref().to_owned(),
            size: file.object_meta.size,
            last_modified_ns,
            partition_values: file
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: file.range.as_ref().map(TryInto::try_into).transpose()?,
            statistics: file.statistics.as_ref().map(|s| s.as_ref().into()),
        })
    }
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from(file: &protobuf::PartitionedFile) -> Result<Self> {
        let mut pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse(file.path.as_str()).map_err(|e| {
                internal_datafusion_err!("Invalid object_store path: {e}")
            })?,
            last_modified: Utc.timestamp_nanos(file.last_modified_ns as i64),
            size: file.size,
            e_tag: None,
            version: None,
        })
        .with_partition_values(
            file.partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        );
        if let Some(proto_schema) = file.arrow_schema.as_ref() {
            pf = pf.with_arrow_schema(Arc::new(
                proto_schema.try_into().map_err(DataFusionError::from)?,
            ));
        }
        if let Some(range) = file.range.as_ref() {
            let range = FileRange::try_from(range)?;
            pf = pf.with_range(range.start, range.end);
        }
        if let Some(proto_stats) = file.statistics.as_ref() {
            pf = pf.with_statistics(Arc::new(proto_stats.try_into()?));
        }
        Ok(pf)
    }
}

impl TryFrom<&FileGroup> for protobuf::FileGroup {
    type Error = DataFusionError;

    fn try_from(group: &FileGroup) -> Result<Self> {
        Ok(protobuf::FileGroup {
            files: group
                .files()
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for FileGroup {
    type Error = DataFusionError;

    fn try_from(group: &protobuf::FileGroup) -> Result<Self> {
        Ok(FileGroup::new(
            group
                .files
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ScalarValue, Statistics};

    use super::*;

    #[test]
    fn partitioned_file_roundtrip_preserves_all_fields() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse("foo/bar.parquet")?,
            last_modified: Utc.timestamp_nanos(1_000_000_000),
            size: 1234,
            e_tag: None,
            version: None,
        })
        .with_partition_values(vec![ScalarValue::from("2024-01-01")])
        .with_range(10, 20)
        .with_arrow_schema(Arc::clone(&schema))
        .with_statistics(Arc::new(Statistics::new_unknown(&schema)));

        let encoded = protobuf::PartitionedFile::try_from(&pf)?;
        let decoded = PartitionedFile::try_from(&encoded)?;

        assert_eq!(decoded.object_meta.location, pf.object_meta.location);
        assert_eq!(decoded.object_meta.size, pf.object_meta.size);
        assert_eq!(
            decoded.object_meta.last_modified,
            pf.object_meta.last_modified
        );
        assert_eq!(decoded.partition_values, pf.partition_values);
        assert_eq!(decoded.range, pf.range);
        assert_eq!(decoded.arrow_schema.as_deref(), Some(schema.as_ref()));
        // Statistics on `PartitionedFile` span the full table schema, and
        // `PartitionedFile::with_statistics` re-derives the partition column
        // entries, so the decoded statistics are not compared field by field
        // here; this is pre-existing behavior of the wire format.
        assert!(decoded.statistics.is_some());
        Ok(())
    }

    #[test]
    fn partitioned_file_from_proto_rejects_invalid_path() {
        let proto = protobuf::PartitionedFile {
            path: "foo//bar.parquet".to_string(),
            ..Default::default()
        };

        let err = PartitionedFile::try_from(&proto).unwrap_err();
        assert!(
            err.to_string().contains("Invalid object_store path"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_group_roundtrip() -> Result<()> {
        let group = FileGroup::new(vec![
            PartitionedFile::new("a.parquet", 1),
            PartitionedFile::new("b.parquet", 2),
        ]);

        let encoded = protobuf::FileGroup::try_from(&group)?;
        let decoded = FileGroup::try_from(&encoded)?;

        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded.files()[1].object_meta.location,
            group.files()[1].object_meta.location
        );
        Ok(())
    }
}
