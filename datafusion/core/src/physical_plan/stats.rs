use arrow::{
    array::{Array, AsArray},
    datatypes::{
        Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type,
    },
};
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::Statistics;

/// This is a debugging function
///
/// This calculates statistics about a stream of record batches for debugging purposes
///
/// Potential future improvments:
/// 1. User defined observers
/// 2. aggregating statistics
/// 3. TBD
#[derive(Debug)]
pub struct StatisticsStream {
    // Map column name --> Statistics
    //cols: BTreeMap<String, ArrayStatistics>
}

impl StatisticsStream {
    pub fn new() -> Self {
        Self {}
    }

    // Record statistics for this batch
    pub fn observe_batch(&self, batch: &RecordBatch) {
        println!("---------------------");
        for (f, array) in batch.schema().fields().iter().zip(batch.columns().iter()) {
            let col_name = f.name();
            let stats = ArrayStatistics::from(array.as_ref());
            println!("Statistics for {col_name}: {}", stats.summary());
        }
    }
}

#[derive(Debug)]
enum ArrayStatistics {
    /// statistics for a dictionaryarray
    Dictionary {
        keys: Statistics,
        values: Statistics,
    },
    Primitive(Statistics),
}

impl ArrayStatistics {
    // TODO make this `impl Display`
    fn summary(&self) -> String {
        match self {
            ArrayStatistics::Dictionary { keys, values } => {
                let overall = combine_stats(keys, values);
                // TODO add overall stats (and then breakdown)
                format!(
                    "{:<16}{}\n{:<16}{}\n{:<16}{}",
                    "DictionaryArray:",
                    stat_summary(&overall),
                    "  Keys:",
                    stat_summary(keys),
                    "  Values:",
                    stat_summary(values)
                )
            }
            ArrayStatistics::Primitive(stats) => {
                format!("PrimitiveArray: {}", stat_summary(stats))
            }
        }
    }
}

//fn overall_dict_stats(

// TODO make this impl display
//
fn stat_summary(stats: &Statistics) -> String {
    let num_rows = match stats.num_rows.as_ref() {
        Some(num_rows) => format!("{num_rows:>6} rows "),
        None => "".to_string(),
    };

    let total_byte_size = match stats.total_byte_size.as_ref() {
        Some(total_byte_size) => format!("{total_byte_size:>20} bytes "),
        None => "".to_string(),
    };

    let is_exact = if stats.is_exact { "(Exact)" } else { "" };

    // TODO column statistics, etc

    format!("{num_rows}{total_byte_size}{is_exact}")
}

impl From<&dyn Array> for ArrayStatistics {
    fn from(value: &dyn Array) -> Self {
        match value.data_type() {
            DataType::Dictionary(key_type, _) => {
                let (keys, values) = match key_type.as_ref() {
                    DataType::Int8 => {
                        let dict = value.as_dictionary::<Int8Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::Int16 => {
                        let dict = value.as_dictionary::<Int16Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::Int32 => {
                        let dict = value.as_dictionary::<Int32Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::Int64 => {
                        let dict = value.as_dictionary::<Int64Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::UInt8 => {
                        let dict = value.as_dictionary::<UInt8Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::UInt16 => {
                        let dict = value.as_dictionary::<UInt16Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::UInt32 => {
                        let dict = value.as_dictionary::<UInt32Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    DataType::UInt64 => {
                        let dict = value.as_dictionary::<UInt64Type>();
                        (dict.keys() as &dyn Array, dict.values() as &dyn Array)
                    }
                    _ => {
                        // todo real error
                        unreachable!("unsupported key type")
                    }
                };
                ArrayStatistics::Dictionary {
                    keys: make_stats(keys),
                    values: make_stats(values),
                }
            }
            _ => ArrayStatistics::Primitive(make_stats(value)),
        }
    }
}

/// create the lower level statistics from an array
/// (TODO can put this into memory exec, or something)
fn make_stats(array: &dyn Array) -> Statistics {
    Statistics {
        num_rows: Some(array.len()),
        total_byte_size: Some(array.get_array_memory_size()),
        column_statistics: None,
        is_exact: true,
    }
}

fn combine_stats(l: &Statistics, r: &Statistics) -> Statistics {
    Statistics {
        num_rows: add_opts(l.num_rows, r.num_rows),
        total_byte_size: add_opts(l.total_byte_size, r.total_byte_size),
        column_statistics: None,
        is_exact: true,
    }
}

fn add_opts(l: Option<usize>, r: Option<usize>) -> Option<usize> {
    match (l, r) {
        (Some(l), Some(r)) => Some(l + r),
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}

// TODO make this a sendable record batchs stream so it can be connected to input/output

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::Int32Type;
    use arrow_array::{DictionaryArray, Int32Array};

    use super::ArrayStatistics;

    #[test]
    fn ints() {
        let ints = Int32Array::from(vec![Some(1), None, Some(3)]);
        let stats = ArrayStatistics::from(&ints as &dyn Array);

        assert_eq!(
            "PrimitiveArray:      3 rows                  224 bytes (Exact)",
            stats.summary()
        )
    }

    #[test]
    fn dictionary() {
        let d1: DictionaryArray<Int32Type> =
            vec![Some("one"), None, Some("three")].into_iter().collect();

        let stats = ArrayStatistics::from(&d1 as &dyn Array);

        let expected = vec![
            "DictionaryArray:     5 rows                 2456 bytes (Exact)",
            "  Keys:              3 rows                  224 bytes (Exact)",
            "  Values:            2 rows                 2232 bytes (Exact)",
        ];

        let actual: Vec<_> = stats.summary().split("\n").map(|s| s.to_string()).collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected: {expected:#?}\n\nactual: {actual:#?}"
        )
    }
}
