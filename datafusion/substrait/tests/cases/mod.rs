// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod aggregation_tests;
mod builtin_expr_semantics_tests;
mod consumer_integration;
mod emit_kind_tests;
mod function_test;
mod logical_plans;
mod roundtrip_logical_plan;
#[cfg(feature = "physical")]
mod roundtrip_physical_plan;
mod serialize;
mod substrait_validations;
