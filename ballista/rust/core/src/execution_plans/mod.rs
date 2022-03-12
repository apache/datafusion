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

//! This module contains execution plans that are needed to distribute DataFusion's execution plans into
//! several Ballista executors.

mod distributed_query;
mod shuffle_reader;
mod shuffle_writer;
mod unresolved_shuffle;

pub use distributed_query::DistributedQueryExec;
pub use shuffle_reader::ShuffleReaderExec;
pub use shuffle_writer::ShuffleWriterExec;
pub use unresolved_shuffle::UnresolvedShuffleExec;
