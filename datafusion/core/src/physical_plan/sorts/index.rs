// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
/// ┌────┐ ┌────┐ ┌────┐           RecordBatch
/// │    │ │    │ │    │
/// │ C1 │ │... │ │ CN │◀─────── (batch_idx = 1)
/// │    │ │    │ │    │
/// └────┘ └────┘ └────┘
/// ┌────┐
/// │    │         ...
/// │ C1 │
/// │    │        ┌────┐           RecordBatch
/// └────┘        │    │
///               │ CN │◀────── (batch_idx = M-1)
///               │    │
///               └────┘
///
///"Stream"s each with           Stream N has M
///   a potentially              RecordBatches
///different number of
///   RecordBatches
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
