use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;
use std::cmp::Ordering;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::helpers::*;

async fn right_join(
    left_stream: SendableRecordBatchStream,
    right_stream: SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>> {
    left_join(right_stream, left_stream).map(reorder_columns)
}
