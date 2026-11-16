// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! DataFusion benchmark runner
pub mod cancellation;
pub mod clickbench;
pub mod dict;
pub mod h2o;
pub mod hj;
pub mod imdb;
pub mod nlj;
pub mod smj;
pub mod sort_pushdown;
pub mod sort_tpch;
pub mod sql_benchmark;
pub mod sql_benchmark_runner;
pub mod sql_benchmark_suite;
pub mod statistics;
pub mod tpcds;
pub mod tpch;
pub mod util;
