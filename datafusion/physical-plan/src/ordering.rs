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

#[derive(Debug, Clone, PartialEq)]
/// Specifies aggregation grouping and/or window partitioning properties of a
/// set of expressions in terms of the existing ordering.
/// For example, if the existing ordering is `[a ASC, b ASC, c ASC]`:
/// - A `PARTITION BY b` clause will result in `Linear` mode.
/// - A `PARTITION BY a, c` or a `PARTITION BY c, a` clause will result in
///   `PartiallySorted([0])` or `PartiallySorted([1])` modes, respectively.
///   The vector stores the index of `a` in the respective PARTITION BY expression.
/// - A `PARTITION BY a, b` or a `PARTITION BY b, a` clause will result in
///   `Sorted` mode.
/// Note that the examples above are applicable for `GROUP BY` clauses too.
pub enum PartitionSearchMode {
    /// There is no partial permutation of the expressions satisfying the
    /// existing ordering.
    Linear,
    /// There is a partial permutation of the expressions satisfying the
    /// existing ordering. Indices describing the longest partial permutation
    /// are stored in the vector.
    PartiallySorted(Vec<usize>),
    /// There is a (full) permutation of the expressions satisfying the
    /// existing ordering.
    Sorted,
}
