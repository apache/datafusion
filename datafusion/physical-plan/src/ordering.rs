// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Specifies how the input to an aggregation or window operator is ordered
/// relative to their `GROUP BY` or  `PARTITION BY` expressions.
///
/// For example, if the existing ordering is `[a ASC, b ASC, c ASC]`
///
/// ## Window Functions
/// - A `PARTITION BY b` clause can use `Linear` mode.
/// - A `PARTITION BY a, c` or a `PARTITION BY c, a` can use
///   `PartiallySorted([0])` or `PartiallySorted([1])` modes, respectively.
///   (The vector stores the index of `a` in the respective PARTITION BY expression.)
/// - A `PARTITION BY a, b` or a `PARTITION BY b, a` can use `Sorted` mode.
///
/// ## Aggregations
/// - A `GROUP BY b` clause can use `Linear` mode, as the only one permutation `[b]`
///   cannot satisfy the existing ordering.
/// - A `GROUP BY a, c` or a `GROUP BY c, a` can use
///   `PartiallySorted([0])` or `PartiallySorted([1])` modes, respectively, as
///   the permutation `[a]` satisfies the existing ordering.
///   (The vector stores the index of `a` in the respective PARTITION BY expression.)
/// - A `GROUP BY a, b` or a `GROUP BY b, a` can use `Sorted` mode, as the
///   full permutation `[a, b]` satisfies the existing ordering.
///
/// Note these are the same examples as above, but with `GROUP BY` instead of
/// `PARTITION BY` to make the examples easier to read.
#[derive(Debug, Clone, PartialEq)]
pub enum InputOrderMode {
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
