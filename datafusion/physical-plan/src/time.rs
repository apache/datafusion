use std::time::SystemTime;

use arrow::{
    compute::{max, min},
    datatypes::TimestampMillisecondType,
};
use arrow_array::{
    Array, Int64Array, PrimitiveArray, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray,
};
use chrono::NaiveDateTime;
use datafusion_common::DataFusionError;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum TimestampUnit {
    StringIso8601(String),
    Int64Seconds,
    Int64Millis,
}

#[derive(Debug)]
pub struct RecordBatchWatermark {
    pub min_timestamp: SystemTime,
    pub max_timestamp: SystemTime,
}

pub fn system_time_from_epoch(epoch: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(epoch as u64)
}

impl RecordBatchWatermark {
    pub fn try_from(
        record_batch: &RecordBatch,
        metadata_column: &str,
    ) -> Result<Self, DataFusionError> {
        let metadata = record_batch.column_by_name(metadata_column).unwrap();
        let metadata_struct = metadata.as_any().downcast_ref::<StructArray>().unwrap();

        let ts_column = metadata_struct
            .column_by_name("canonical_timestamp")
            .unwrap();
        let ts_array = ts_column
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap();

        let max_timestamp = system_time_from_epoch(
            max::<TimestampMillisecondType>(&ts_array).unwrap() as _,
        );
        let min_timestamp =
            system_time_from_epoch(min::<TimestampMillisecondType>(&ts_array).unwrap());
        let result = RecordBatchWatermark {
            min_timestamp,
            max_timestamp,
        };
        Ok(result)
    }
}

pub fn array_to_timestamp_array(
    array: &dyn Array,
    data_type: TimestampUnit,
) -> TimestampMillisecondArray {
    match data_type {
        TimestampUnit::StringIso8601(fmt) => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let naive_datetimes: Vec<NaiveDateTime> = string_array
                .iter()
                .map(|s| NaiveDateTime::parse_from_str(s.unwrap(), fmt.as_str()).unwrap())
                .collect();

            let timestamps: Vec<i64> = naive_datetimes
                .into_iter()
                .map(|dt| dt.and_utc().timestamp_millis())
                .collect();

            let primitive_array = PrimitiveArray::from(timestamps);
            TimestampMillisecondArray::from(primitive_array)
        }
        TimestampUnit::Int64Millis => {
            let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let timestamps: Vec<i64> = int64_array.values().to_vec();
            let primitive_array = PrimitiveArray::from(timestamps);
            TimestampMillisecondArray::from(primitive_array)
        }
        TimestampUnit::Int64Seconds => {
            let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let timestamps: Vec<i64> = int64_array.values().to_vec();
            let millisecond_timestamps: Vec<i64> =
                timestamps.into_iter().map(|ts| ts * 1000).collect();
            let primitive_array = PrimitiveArray::from(millisecond_timestamps);
            TimestampMillisecondArray::from(primitive_array)
        }
    }
}
