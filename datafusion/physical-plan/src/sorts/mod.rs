// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Sort functionalities

mod builder;
mod cursor;
mod merge;
#[cfg(test)]
mod merge_trace;
mod multi_level_merge;
mod output_memory;
pub mod partial_sort;
pub mod partitioned_topk;
pub mod sort;
pub mod sort_preserving_merge;
mod stream;
pub mod streaming_merge;

pub(crate) use builder::{
    interleave_memory_size_from_sliced, schema_needs_recursive_read_bound,
};
pub(crate) use stream::IncrementalSortIterator;

pub(crate) use output_memory::ProgressStats;
