// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! PiecewiseMergeJoin is currently experimental

pub use exec::PiecewiseMergeJoinExec;

mod classic_join;
mod exec;
mod existence_join;
mod right_existence_join;
mod utils;
