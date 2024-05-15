use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::Result;
use parquet::file::statistics::Statistics as ParquetStatistics;

/// statistics extracted from `Statistics` as Arrow `ArrayRef`s
///
/// # Note:
/// If the corresponding `Statistics` is not present, or has no information for
/// a column, a NULL is present in the  corresponding array entry
pub struct ArrowStatistics {
    /// min values
    min: ArrayRef,
    /// max values
    max: ArrayRef,
    /// Row counts (UInt64Array)
    row_count: ArrayRef,
    /// Null Counts (UInt64Array)
    null_count: ArrayRef,
}

/// Extract `ArrowStatistics` from the  parquet [`Statistics`]
pub fn parquet_stats_to_arrow<'a>(
    arrow_datatype: &DataType,
    statistics: impl IntoIterator<Item = Option<&'a ParquetStatistics>>,
) -> Result<ArrowStatistics> {
    todo!() // MY TODO next
}

// TODO: Now I had tests that read parquet metadata, I will use that to implement the below
// struct ParquetStatisticsExtractor {
//   ...
//   }

//   // create an extractor that can extract data from parquet files
//   let extractor = ParquetStatisticsExtractor::new(arrow_schema, parquet_schema)

//   // get parquet statistics (one for each row group) somehow:
//   let parquet_stats: Vec<&Statistics> = ...;

//   // extract min/max values for column "a" and "b";
//   let col_a stats = extractor.extract("a", parquet_stats.iter());
//   let col_b stats = extractor.extract("b", parquet_stats.iter());
