use std::{pin::Pin, task::Poll};

use ahash::RandomState;
use arrow_array::{ArrayRef, RecordBatch};
use datafusion_common::{error::Result, hash_utils::create_hashes};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_expr::PhysicalExprRef;
use futures::{ready, stream::BoxStream, Stream, StreamExt};

use crate::stream::ReceiverStreamBuilder;


//
// HashExprStream evaluates `expr` on incoming batches and attaches
// the result + its hash to the output.
//

pub struct HashExprStream {
    exprs: Vec<PhysicalExprRef>,
    random_state: RandomState,
    inner: SendableRecordBatchStream,
}

pub struct WithHashedExpr {
    pub(crate) batch: RecordBatch,
    pub(crate) hashes: Vec<u64>,
    pub(crate) _exprs: Vec<ArrayRef>,
}

impl HashExprStream {
    pub fn new(
        exprs: Vec<PhysicalExprRef>,
        random_state: RandomState,
        inner: SendableRecordBatchStream,
    ) -> HashExprStream {
        HashExprStream {
            exprs,
            random_state,
            inner,
        }
    }

    fn eval(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.exprs
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|column| column.into_array(batch.num_rows()))
            })
            .collect()
    }
}

impl Stream for HashExprStream {
    type Item = Result<WithHashedExpr>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let expr = self.eval(&batch)?;
                let mut hashes = vec![0; batch.num_rows()];
                create_hashes(expr.as_slice(), &self.random_state, &mut hashes)?;
                let exprs = self.eval(&batch)?;
                Poll::Ready(Some(Ok(WithHashedExpr {
                    batch,
                    hashes,
                    _exprs: exprs,
                })))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

// Could be more generic?
pub(crate) struct HashCoalescerBuilder {
    inner: ReceiverStreamBuilder<WithHashedExpr>,
}

impl HashCoalescerBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: ReceiverStreamBuilder::new(capacity),
        }
    }

    pub(crate) fn run_input(&mut self, mut stream: BoxStream<'static, Result<WithHashedExpr>>) {
        let output = self.inner.tx();

        self.inner.spawn(async move {
            while let Some(item) = stream.next().await {
                let is_err = item.is_err();

                if output.send(item).await.is_err() {
                    return Ok(());
                }

                if is_err {
                    return Ok(());
                }
            }

            Ok(())
        });
    }

    pub fn build(self) -> BoxStream<'static, Result<WithHashedExpr>> {
        self.inner.build()
    }
}
