/// Macro for creating DataFrame.
/// # Example
/// ```
/// use datafusion::prelude::df;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!(
///    "id" => [1, 2, 3],
///    "name" => ["foo", "bar", "baz"]
///  )?;
/// df.show().await?;
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let df_empty = df!()?; // empty DataFrame
/// assert_eq!(df_empty.schema().fields().len(), 0);
/// assert_eq!(df_empty.count().await?, 0);
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! df {
    () => {{
        use std::sync::Arc;

        use datafusion::prelude::SessionContext;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;

        let ctx = SessionContext::new();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        ctx.read_batch(batch)
    }};

    ($($col_name:expr => $data:expr),+ $(,)?) => {{
        use datafusion::prelude::DataFrame;
        use datafusion::common::array_conversion::IntoArrayRef;

        let columns = vec![
            $( ($col_name, Box::new($data) as Box<dyn IntoArrayRef>) ),+
        ];
        DataFrame::from_columns(columns)
    }};
}
