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

//! Narrow integer Arrow types in embedded Parquet `ARROW:schema` metadata
//! based on exact column chunk min/max statistics.
//!
//! Physical page encoding is left unchanged; only the Arrow schema hint in
//! file key/value metadata is rewritten so readers can advertise a tighter
//! type when the value domain fits.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

/// If any integer fields can be narrowed from exact row-group statistics,
/// returns a new schema with those field types updated. Returns `None` when
/// nothing changes.
pub(crate) fn try_narrow_integer_schema(
    schema: &Schema,
    row_groups: &[RowGroupMetaData],
) -> Option<Schema> {
    let column_to_minmax = extract_column_minmaxes(row_groups)?;
    let mut changed = false;
    let fields = narrow_fields(schema.fields(), &[], &column_to_minmax, &mut changed);
    if !changed {
        return None;
    }

    Some(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Creates a `HashMap` of strings (Parquet column path, like `"id"` or `"tags.list.element"`) to a two-tuple representing (min, max)
/// Note that a column is omitted if any row group lacks exact min/max for it. The min/max are converted to i64 as well
fn extract_column_minmaxes(
    row_groups: &[RowGroupMetaData],
) -> Option<HashMap<String, (i64, i64)>> {
    let mut column_to_minmax: Option<HashMap<String, (i64, i64)>> = None;

    for rg in row_groups {
        let mut curr_rg = HashMap::new();
        for column in rg.columns() {
            let path = column.column_path().string();
            let Some(stats) = column.statistics() else {
                continue;
            };
            if !stats.min_is_exact() || !stats.max_is_exact() {
                continue;
            }
            let Some((min, max)) = int_min_max(stats) else {
                continue;
            };
            curr_rg.insert(path, (min, max));
        }

        match &mut column_to_minmax {
            None => column_to_minmax = Some(curr_rg),
            Some(merged) => {
                merged.retain(|path, (file_min, file_max)| {
                    let Some((min, max)) = curr_rg.get(path) else {
                        return false;
                    };
                    *file_min = (*file_min).min(*min);
                    *file_max = (*file_max).max(*max);
                    true
                });
            }
        }
    }

    column_to_minmax
}

/// Note that we're ignoring types like `Statistics::Int96` because if we can't cast to i64 then it's too big to downcast
fn int_min_max(stats: &Statistics) -> Option<(i64, i64)> {
    match stats {
        Statistics::Int32(val) => {
            let min = val.min_opt().copied().map(i64::from)?;
            let max = val.max_opt().copied().map(i64::from)?;
            Some((min, max))
        }
        Statistics::Int64(val) => {
            let min = *val.min_opt()?;
            let max = *val.max_opt()?;
            Some((min, max))
        }
        _ => None,
    }
}

/// Narrow to the smallest possible integer type that can represent all values
pub(crate) fn narrowest_integer_type(min: i64, max: i64) -> DataType {
    if min < 0 || max < 0 {
        // Signed
        if min >= i8::MIN as i64 && max <= i8::MAX as i64 {
            DataType::Int8
        } else if min >= i16::MIN as i64 && max <= i16::MAX as i64 {
            DataType::Int16
        } else if min >= i32::MIN as i64 && max <= i32::MAX as i64 {
            DataType::Int32
        } else {
            DataType::Int64
        }
    // Unsigned
    } else if max <= u8::MAX as i64 {
        DataType::UInt8
    } else if max <= u16::MAX as i64 {
        DataType::UInt16
    } else if max <= u32::MAX as i64 {
        DataType::UInt32
    } else {
        DataType::UInt64
    }
}

/// True when `candidate` is a strictly tighter integer type than `current`.
fn is_narrower(candidate: &DataType, current: &DataType) -> bool {
    match (candidate.primitive_width(), current.primitive_width()) {
        (Some(c), Some(cur)) if c < cur => true,
        (Some(c), Some(cur))
            if c == cur
                && current.is_signed_integer()
                && candidate.is_unsigned_integer() =>
        {
            true
        }
        _ => false,
    }
}

fn narrow_fields(
    fields: &Fields,
    path_prefix: &[&str],
    column_to_minmax: &HashMap<String, (i64, i64)>,
    changed: &mut bool,
) -> Fields {
    fields
        .iter()
        .map(|field| {
            Arc::new(narrow_field(field, path_prefix, column_to_minmax, changed))
        })
        .collect()
}

fn narrow_field(
    field: &Field,
    path_prefix: &[&str],
    column_to_minmax: &HashMap<String, (i64, i64)>,
    changed: &mut bool,
) -> Field {
    let mut path: Vec<&str> = path_prefix.to_vec();
    path.push(field.name());

    let new_type = match field.data_type() {
        DataType::Struct(children) => {
            DataType::Struct(narrow_fields(children, &path, column_to_minmax, changed))
        }
        DataType::List(child) => {
            let mut list_path = path.clone();
            list_path.push("list");
            DataType::List(Arc::new(narrow_field(
                child,
                &list_path,
                column_to_minmax,
                changed,
            )))
        }
        DataType::LargeList(child) => {
            let mut list_path = path.clone();
            list_path.push("list");
            DataType::LargeList(Arc::new(narrow_field(
                child,
                &list_path,
                column_to_minmax,
                changed,
            )))
        }
        DataType::FixedSizeList(child, size) => {
            let mut list_path = path.clone();
            list_path.push("list");
            DataType::FixedSizeList(
                Arc::new(narrow_field(child, &list_path, column_to_minmax, changed)),
                *size,
            )
        }
        DataType::Map(entries, sorted) => DataType::Map(
            Arc::new(narrow_field(entries, &path, column_to_minmax, changed)),
            *sorted,
        ),
        DataType::Dictionary(key, value) if value.is_integer() => {
            match maybe_narrow_primitive(value, &path, column_to_minmax, changed) {
                Some(narrowed) => DataType::Dictionary(key.clone(), Box::new(narrowed)),
                None => field.data_type().clone(),
            }
        }
        dt if dt.is_integer() => {
            match maybe_narrow_primitive(dt, &path, column_to_minmax, changed) {
                Some(narrowed) => narrowed,
                None => dt.clone(),
            }
        }
        other => other.clone(),
    };

    Field::new(field.name(), new_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn maybe_narrow_primitive(
    current: &DataType,
    path: &[&str],
    column_to_minmax: &HashMap<String, (i64, i64)>,
    changed: &mut bool,
) -> Option<DataType> {
    let path_key = path.join(".");
    let (min, max) = *column_to_minmax.get(&path_key)?;
    let candidate = narrowest_integer_type(min, max);
    if is_narrower(&candidate, current) {
        *changed = true;
        Some(candidate)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, RecordBatch, StructArray};
    use arrow::datatypes::Field;
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn write_and_row_groups(batch: &RecordBatch) -> Vec<RowGroupMetaData> {
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_max_row_group_row_count(Some(batch.num_rows().max(1)))
            .build();
        let mut buf = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        }
        let reader = SerializedFileReader::new(Bytes::from(buf)).unwrap();
        reader.metadata().row_groups().to_vec()
    }

    #[test]
    fn narrowest_signed_and_unsigned_ladder() {
        assert_eq!(narrowest_integer_type(-1, 10), DataType::Int8);
        assert_eq!(narrowest_integer_type(-200, 10), DataType::Int16);
        assert_eq!(narrowest_integer_type(0, 200), DataType::UInt8);
        assert_eq!(narrowest_integer_type(0, 70_000), DataType::UInt32);
        assert_eq!(narrowest_integer_type(2, 19), DataType::UInt8);
    }

    #[test]
    fn narrows_flat_int64_to_uint8() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![2, 19, 7]))],
        )
        .unwrap();
        let row_groups = write_and_row_groups(&batch);
        let narrowed = try_narrow_integer_schema(&schema, &row_groups).unwrap();
        assert_eq!(narrowed.field(0).data_type(), &DataType::UInt8);
    }

    #[test]
    fn narrows_nested_struct_integer() {
        let inner = Arc::new(Field::new("n", DataType::Int32, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int32, true)])),
            true,
        )]));
        let struct_array = StructArray::from(vec![(
            Arc::clone(&inner),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
        )]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)])
                .unwrap();
        let row_groups = write_and_row_groups(&batch);
        let narrowed = try_narrow_integer_schema(&schema, &row_groups).unwrap();
        match narrowed.field(0).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields[0].data_type(), &DataType::UInt8);
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn leaves_type_when_domain_does_not_fit_narrower() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX]))],
        )
        .unwrap();
        let row_groups = write_and_row_groups(&batch);
        assert!(try_narrow_integer_schema(&schema, &row_groups).is_none());
    }

    #[test]
    fn is_narrower_requires_tighter_type() {
        assert!(is_narrower(&DataType::UInt8, &DataType::Int64));
        assert!(is_narrower(&DataType::UInt8, &DataType::Int8));
        assert!(!is_narrower(&DataType::Int64, &DataType::Int32));
        assert!(!is_narrower(&DataType::UInt8, &DataType::UInt8));
    }
}
