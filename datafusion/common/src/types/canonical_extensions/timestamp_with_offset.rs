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

use crate::Result;
use crate::ScalarValue;
use crate::error::_internal_err;
use crate::types::extension::DFExtensionType;
use arrow::array::{Array, AsArray, Int16Array};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::util::display::{ArrayFormatter, DisplayIndex, FormatOptions, FormatResult};
use arrow_schema::ArrowError;
use arrow_schema::extension::{ExtensionType, TimestampWithOffset};
use std::fmt::Write;

/// Defines the extension type logic for the canonical `arrow.timestamp_with_offset` extension type.
/// This extension type allows associating a different offset for each timestamp in a column.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism. See also
/// [`TimestampWithOffset`] for the implementation of arrow-rs, which this type uses internally.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#timestamp-with-offset>
#[derive(Debug, Clone)]
pub struct DFTimestampWithOffset {
    inner: TimestampWithOffset,
    storage_type: DataType,
}

impl DFTimestampWithOffset {
    /// Creates a new [`DFTimestampWithOffset`], validating that the storage type is compatible with
    /// the extension type.
    pub fn try_new(
        data_type: &DataType,
        metadata: <TimestampWithOffset as ExtensionType>::Metadata,
    ) -> Result<Self> {
        Ok(Self {
            inner: <TimestampWithOffset as ExtensionType>::try_new(data_type, metadata)?,
            storage_type: data_type.clone(),
        })
    }
}

impl DFExtensionType for DFTimestampWithOffset {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.inner.serialize_metadata()
    }

    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> Result<Option<ArrayFormatter<'fmt>>> {
        if array.data_type() != &self.storage_type {
            return _internal_err!(
                "Unexpected data type for TimestampWithOffset: {}",
                array.data_type()
            );
        }

        let struct_array = array.as_struct();
        let timestamp_array = struct_array
            .column_by_name("timestamp")
            .expect("Type checked above")
            .as_ref();
        let raw_offset_array = struct_array
            .column_by_name("offset_minutes")
            .expect("Type checked above");

        // Get a regular [`Int16Array`], if the offset array is a dictionary or run-length encoded.
        let offset_array = cast(&raw_offset_array, &DataType::Int16)?
            .as_primitive()
            .clone();

        let display_index = TimestampWithOffsetDisplayIndex {
            null_buffer: struct_array.nulls(),
            timestamp_array,
            offset_array,
            options: options.clone(),
        };

        Ok(Some(ArrayFormatter::new(
            Box::new(display_index),
            options.safe(),
        )))
    }
}

struct TimestampWithOffsetDisplayIndex<'a> {
    /// The inner arrays are always non-null. Use the null buffer of the struct array to check
    /// whether an element is null.
    null_buffer: Option<&'a NullBuffer>,
    timestamp_array: &'a dyn Array,
    offset_array: Int16Array,
    options: FormatOptions<'a>,
}

impl DisplayIndex for TimestampWithOffsetDisplayIndex<'_> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.null_buffer.map(|nb| nb.is_null(idx)).unwrap_or(false) {
            write!(f, "{}", self.options.null())?;
            return Ok(());
        }

        let offset_minutes = self.offset_array.value(idx);
        let offset = format_offset(offset_minutes);

        // The timestamp array must be UTC, so we can ignore the timezone.
        let scalar = match self.timestamp_array.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                let ts = self
                    .timestamp_array
                    .as_primitive::<TimestampSecondType>()
                    .value(idx);
                ScalarValue::TimestampSecond(Some(ts), Some(offset.into()))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let ts = self
                    .timestamp_array
                    .as_primitive::<TimestampMillisecondType>()
                    .value(idx);
                ScalarValue::TimestampMillisecond(Some(ts), Some(offset.into()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let ts = self
                    .timestamp_array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(idx);
                ScalarValue::TimestampMicrosecond(Some(ts), Some(offset.into()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let ts = self
                    .timestamp_array
                    .as_primitive::<TimestampNanosecondType>()
                    .value(idx);
                ScalarValue::TimestampNanosecond(Some(ts), Some(offset.into()))
            }
            _ => unreachable!("TimestampWithOffset storage must be a Timestamp array"),
        };

        let array = scalar.to_array().map_err(|_| {
            ArrowError::ComputeError("Failed to convert scalar to array".to_owned())
        })?;
        let formatter = ArrayFormatter::try_new(&array, &self.options)?;
        formatter.value(0).write(f)?;

        Ok(())
    }
}

/// Formats the offset in the format `+/-HH:MM`, which can be used as an offset in the regular
/// timestamp types.
fn format_offset(minutes: i16) -> String {
    let sign = if minutes >= 0 { '+' } else { '-' };
    let minutes = minutes.abs();
    let hours = minutes / 60;
    let minutes = minutes % 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, DictionaryArray, Int16Array, Int32Array, RunArray, StructArray,
        TimestampSecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields, Int16Type, Int32Type};
    use chrono::{TimeZone, Utc};
    use std::sync::Arc;

    #[test]
    fn test_pretty_print_timestamp_with_offset() -> Result<(), ArrowError> {
        let ts = Utc
            .with_ymd_and_hms(2024, 4, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        let offset_array = Arc::new(Int16Array::from(vec![60, -105, 0]));

        run_formatting_test(
            vec![ts, ts, ts],
            offset_array,
            Some(NullBuffer::from(vec![true, true, false])),
            FormatOptions::default().with_null("NULL"),
            &[
                "2024-04-01T01:00:00+01:00",
                "2024-03-31T22:15:00-01:45",
                "NULL",
            ],
        )
    }

    #[test]
    fn test_pretty_print_dictionary_offset() -> Result<(), ArrowError> {
        let ts = Utc
            .with_ymd_and_hms(2024, 4, 1, 12, 0, 0)
            .unwrap()
            .timestamp();

        let offset_array = Arc::new(DictionaryArray::<Int16Type>::new(
            Int16Array::from(vec![0, 1, 0]),
            Arc::new(Int16Array::from(vec![60, -60])),
        ));

        run_formatting_test(
            vec![ts, ts, ts],
            offset_array,
            None,
            FormatOptions::default(),
            &[
                "2024-04-01T13:00:00+01:00",
                "2024-04-01T11:00:00-01:00",
                "2024-04-01T13:00:00+01:00",
            ],
        )
    }

    #[test]
    fn test_pretty_print_rle_offset() -> Result<(), ArrowError> {
        let ts = Utc
            .with_ymd_and_hms(2024, 4, 1, 12, 0, 0)
            .unwrap()
            .timestamp();

        let run_ends = Int32Array::from(vec![2]);
        let values = Int16Array::from(vec![120]);
        let offset_array = Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values)?);

        run_formatting_test(
            vec![ts, ts],
            offset_array,
            None,
            FormatOptions::default(),
            &["2024-04-01T14:00:00+02:00", "2024-04-01T14:00:00+02:00"],
        )
    }

    /// Create valid fields with flexible offset types
    fn create_fields_custom_offset(time_unit: TimeUnit, offset_type: DataType) -> Fields {
        let ts_field = Field::new(
            "timestamp",
            DataType::Timestamp(time_unit, Some("UTC".into())),
            false,
        );
        let offset_field = Field::new("offset_minutes", offset_type, false);
        Fields::from(vec![ts_field, offset_field])
    }

    /// Helper to construct the arrays, run the formatter, and assert the expected strings.
    fn run_formatting_test(
        timestamps: Vec<i64>,
        offset_array: Arc<dyn Array>,
        null_buffer: Option<NullBuffer>,
        options: FormatOptions,
        expected: &[&str],
    ) -> Result<(), ArrowError> {
        let fields = create_fields_custom_offset(
            TimeUnit::Second,
            offset_array.data_type().clone(),
        );

        let struct_array = StructArray::try_new(
            fields,
            vec![
                Arc::new(TimestampSecondArray::from(timestamps).with_timezone("UTC")),
                offset_array,
            ],
            null_buffer,
        )?;

        let formatter = DFTimestampWithOffset::try_new(struct_array.data_type(), ())?
            .create_array_formatter(&struct_array, &options)?
            .unwrap();

        for (i, expected_str) in expected.iter().enumerate() {
            assert_eq!(formatter.value(i).to_string(), *expected_str);
        }

        Ok(())
    }
}
