// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Sort Merge Join Execution Plan Operator

pub use exec::SortMergeJoinExec;

pub(crate) mod bitwise_stream;
mod exec;
mod filter;
pub(crate) mod materializing_stream;
mod metrics;

#[cfg(test)]
mod tests;
