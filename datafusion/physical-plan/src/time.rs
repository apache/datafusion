use std::time::SystemTime;

use arrow::{
    compute::{max, min},
    datatypes::TimestampMillisecondType,
};
use arrow_array::{
    Array, Int64Array, PrimitiveArray, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use chrono::NaiveDateTime;
use datafusion_common::DataFusionError;
use std::time::Duration;
use tracing::info;

struct Time {}
#[derive(Debug, Clone)]
pub enum TimestampUnit {
    String_ISO_8601(String),
    Int64_Seconds,
    Int64_Millis,
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
        timestamp_column: &str,
    ) -> Result<Self, DataFusionError> {
        let ts_column = record_batch.column_by_name(timestamp_column).unwrap();
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
        TimestampUnit::String_ISO_8601(fmt) => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let naive_datetimes: Vec<NaiveDateTime> = string_array
                .iter()
                .map(|s| NaiveDateTime::parse_from_str(s.unwrap(), fmt.as_str()).unwrap())
                .collect();

            let timestamps: Vec<i64> = naive_datetimes
                .into_iter()
                .map(|dt| dt.timestamp_millis())
                .collect();

            let primitive_array = PrimitiveArray::from_vec(timestamps, None);
            TimestampMillisecondArray::from(primitive_array)
        }
        TimestampUnit::Int64_Millis => {
            let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let timestamps: Vec<i64> = int64_array.values().to_vec();
            let primitive_array = PrimitiveArray::from_vec(timestamps, None);
            TimestampMillisecondArray::from(primitive_array)
        }
        TimestampUnit::Int64_Seconds => {
            let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let timestamps: Vec<i64> = int64_array.values().to_vec();
            let millisecond_timestamps: Vec<i64> =
                timestamps.into_iter().map(|ts| ts * 1000).collect();
            let primitive_array = PrimitiveArray::from_vec(millisecond_timestamps, None);
            TimestampMillisecondArray::from(primitive_array)
        }
        _ => panic!("Unsupported data type: {:?}", data_type),
    }
}
