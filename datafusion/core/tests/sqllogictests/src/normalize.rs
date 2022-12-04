use std::sync::Arc;

use arrow::{
    array::{
        as_largestring_array, as_string_array, ArrayRef, LargeStringArray, StringArray,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};

fn normalize_string(v: Option<&str>) -> Option<&str> {
    v.map(|v| {
        // All empty strings are replaced with this value
        if v.is_empty() {
            "(empty)"
        } else {
            v
        }
    })
}

/// Normalizes the content of a RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied:
/// 1. Null Values (TODO)
/// 2. [Empty Strings]
///
/// [Empty Strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
pub fn normalize_batch(batch: RecordBatch) -> RecordBatch {
    let new_columns = batch
        .columns()
        .iter()
        .map(|array| {
            match array.data_type() {
                DataType::Utf8 => {
                    let arr: StringArray = as_string_array(array.as_ref())
                        .iter()
                        .map(normalize_string)
                        .collect();
                    Arc::new(arr) as ArrayRef
                }
                DataType::LargeUtf8 => {
                    let arr: LargeStringArray = as_largestring_array(array.as_ref())
                        .iter()
                        .map(normalize_string)
                        .collect();
                    Arc::new(arr) as ArrayRef
                }
                // todo normalize dictionary values

                // no normalization on this type
                _ => array.clone(),
            }
        })
        .collect();

    RecordBatch::try_new(batch.schema(), new_columns).expect("creating normalized batch")
}
