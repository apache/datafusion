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

use super::cursor::{Cursor, CursorValues};

/// A representation of a record batch,
/// passable and sliceable through multiple merge nodes
/// in a cascaded merge tree.
///
/// A `BatchCursor` encapsulates the ability to sort merge each
/// sliced portition of a record batch, with minimal overhead.
///
/// ```text
/// ┌────────────────────────┐
/// │ CursorValues   Batch   │           ┌──────────────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│      BatchTracker    │
/// │ │  1..10   │ │   A   │ ┼ ─ │       └──────────────────────┘
/// │ ├──────────┤ ├───────┤ │   │            Holds batches
/// │ │  11..20  │ │   B   │ ┼ ─ ┘          and assigns BatchId
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///             │
///             │
///             ▼
///        BatchCursors
/// ┌────────────────────────┐           ┌──────────────────────┐ ─ ▶ push batch
/// │    Cursor     BatchId  │    ─ ─ ─ ▶│   LoserTree (Merge)  │ ─ ▶ advance cursor
/// │ ┌──────────┐ ┌───────┐ │   │       └──────────────────────┘ ─ ▶ push row
/// │ │  1..10   │ │   A   │ ┼ ─ │       ┌──────────────────────┐         │
/// │ ├──────────┤ ├───────┤ │   │       │   SortOrderBuilder  ◀┼ ─ ─ ─ ─ ┘
/// │ │  11..20  │ │   B   │ ┼ ─ ┘       └──────────────────────┘
/// │ └──────────┘ └───────┘ │                holds sorted rows
/// └────────────────────────┘              up to ceiling size N
///                                                  │
///              ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///             │
///             ▼
///        BatchCursors
/// ┌────────────────────────┐
/// │    Cursor     BatchId  │           ┌──────────────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│   LoserTree (Merge)  │
/// │ │  1..7    │ │   A   │ ┼ ─ │       └──────────│───────────┘
/// │ ├──────────┤ ├───────┤ │   │       ┌──────────▼───────────┐
/// │ │  11..14  │ │   B   │ ┼ ─ │       │   SortOrderBuilder   |
/// │ └──────────┘ └───────┘ │   │       └──────────────────────┘
/// │     ...         ...    │   │
/// │ ┌──────────┐ ┌───────┐ │   │
/// │ │ 101..103 │ │   F   │ ┼ ─ │
/// │ ├──────────┤ ├───────┤ │   │
/// │ │ 151..157 │ │   G   │ ┼ ─ ┘
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///             ▲
///             │
///         Mirror of above.
///  LoserTree (Merge) & SortOrderBuilder
///       yielding BatchCursors
///  which represents partial batches
/// ```
///
///
/// Final merge at CascadedMerge root:
/// ```text
///
///         SortOrder
/// ┌───────────────────────────┐
/// | (B,11) (F,101) (A,1) ...  ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// └───────────────────────────┘                  |
///                                                |
///        BatchTracker                            |
/// ┌────────────────────────┐                     |
/// │    Batch      BatchId  │           ┌─────────▼────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│   CascadedMerge root │
/// │ │  <data>  │ │   A   │ ┼ ─ │       │         |            │
/// │ ├──────────┤ ├───────┤ │   │       |         ▼            |
/// │ │  <data>  │ │   B   │ ┼ ─ │       │     interleave       |
/// │ └──────────┘ └───────┘ │   │       └─────────|────────────┘
/// │     ...         ...    │   │                 |
/// │ ┌──────────┐ ┌───────┐ │   │                 ▼
/// │ │  <data>  │ │   F   │ ┼ ─ │          batch up to N rows
/// │ ├──────────┤ ├───────┤ │   │
/// │ │  <data>  │ │   G   │ ┼ ─ ┘
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///
/// ```
///
#[derive(Debug)]
pub struct BatchCursor<C: CursorValues> {
    /// The index into SortOrderBuilder::batches
    /// TODO: this will become a BatchId, for record batch collected (and not passed across streams)
    pub batch_idx: usize,

    /// The row index within the given batch.
    ///
    /// The existing logic (for the loser tree + BatchBuilder)
    /// advances the cursor +1 beyond the push_row().
    ///
    /// As such, the existing BatchBuilder's `BatchCursor::row_idx`
    /// has been tracked separately from the cursor's current offset.
    ///
    /// This will be consolidated in futures PR (removing this field).
    pub row_idx: usize,

    /// The cursor for the given batch.
    pub cursor: Cursor<C>,
}

/// Unique tracking id, assigned per record batch.
pub struct BatchId(u64);
