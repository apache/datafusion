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

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct BatchCursor<C> {
    /// The index into SortOrderBuilder::batches
    /// TODO: this will become a BatchId, for record batch collected (and not passed across streams)
    pub batch_idx: usize,

    /// The row index within the given batch
    pub row_idx: usize,

    /// The cursor for the given batch.
    pub cursor: C,
}
