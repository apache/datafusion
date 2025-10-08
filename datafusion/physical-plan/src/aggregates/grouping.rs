use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr::aggregate::AggregateExpr;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use futures::{Stream, TryStreamExt};



/// Stream that processes grouping functions and removes __grouping_id column
pub struct GroupingStream {
    /// Inner stream
    input: SendableRecordBatchStream,
    /// Schema after removing __grouping_id column and adding grouping columns
    schema: SchemaRef,
    /// Aggregate expressions that may contain GROUPING functions
    aggr_expr: Vec<AggregateExpr>,
    /// Number of group expressions
    num_group_exprs: usize,
}

impl GroupingStream {
    /// Create a new GroupingStream
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        aggr_expr: Vec<AggregateExpr>,
        num_group_exprs: usize,
    ) -> Self {
        Self {
            input,
            schema,
            aggr_expr,
            num_group_exprs,
        }
    }

    /// Process a batch by:
    /// 1. Computing GROUPING function columns
    /// 2. Removing the __grouping_id column
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.schema.fields().len());

        // Add group columns (before __grouping_id)
        for i in 0..self.num_group_exprs {
            columns.push(Arc::clone(batch.column(i)));
        }

        // Skip __grouping_id column (at position num_group_exprs)
        // and add aggregate columns
        let mut start_aggr = self.num_group_exprs + 1; // +1 to skip __grouping_id

        for expr in &self.aggr_expr {
            match expr {
                AggregateExpr::GroupingExpr(grouping_expr) => {
                    let array = grouping_expr.evaluate(&batch)?.into_array(batch.num_rows())?;
                    columns.push(array);
                }
                AggregateExpr::AggregateFunctionExpr(_) => {
                    columns.push(Arc::clone(batch.column(start_aggr)));
                    start_aggr += 1;
                }
            }
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
    }
}

impl Stream for GroupingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.try_poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(&batch)),
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for GroupingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
