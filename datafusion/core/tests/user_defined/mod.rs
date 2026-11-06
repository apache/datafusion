// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Tests for user defined Async Scalar functions
mod user_defined_async_scalar_functions;

/// Tests for user defined Scalar functions
mod user_defined_scalar_functions;

/// Tests for User Defined Aggregate Functions
mod user_defined_aggregates;

/// Tests for User Defined Plans
mod user_defined_plan;

/// Tests for User Defined Window Functions
mod user_defined_window_functions;

/// Tests for User Defined Table Functions
mod user_defined_table_functions;

/// Tests for Expression Planner
mod expr_planner;

/// Tests for Relation Planner extensions
mod relation_planner;

/// Tests for insert operations
mod insert_operation;

/// Tests for `StatisticsRequest`s flowing from a custom optimizer rule
/// through the physical planner into a custom `TableProvider`.
mod statistics_requests;

mod volatility;
