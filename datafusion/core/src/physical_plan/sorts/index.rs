/// A `RowIndex` identifies a specific row in a logical stream.
///
/// Each stream is identified by an `stream_idx` and is formed from a
/// sequence of RecordBatches batches, each of which is identified by
/// a unique `batch_idx` within that stream.
///
/// This is used by `SortPreservingMergeStream` to identify which
/// the order of the tuples in the final sorted output stream.
///
/// ```text
/// ┌────┐ ┌────┐ ┌────┐           RecordBatch
/// │    │ │    │ │    │
/// │ C1 │ │... │ │ CN │◀─────── (batch_idx = 0)
/// │    │ │    │ │    │
/// └────┘ └────┘ └────┘
///
/// ┌────┐ ┌────┐ ┌────┐           RecordBatch
/// │    │ │    │ │    │
/// │ C1 │ │... │ │ CN │◀─────── (batch_idx = 1)
/// │    │ │    │ │    │
/// └────┘ └────┘ └────┘
///
///          ...
///
/// ┌────┐ ┌────┐ ┌────┐           RecordBatch
/// │    │ │    │ │    │
/// │ C1 │ │... │ │ CN │◀────── (batch_idx = N-1)
/// │    │ │    │ │    │
/// └────┘ └────┘ └────┘
///
///       "Stream"
///  of N RecordBatches
/// ```
#[derive(Debug, Clone)]
pub struct RowIndex {
    /// The index of the stream (uniquely identifies the stream)
    pub stream_idx: usize,
    /// The index of the batch within the stream's VecDequeue.
    pub batch_idx: usize,
    /// The row index within the batch
    pub row_idx: usize,
}
