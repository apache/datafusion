// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Connect the component-local spill pool scenario to the shared fuzz interface.

use datafusion_common::Result;
use datafusion_physical_plan::spill::spill_pool::run_spill_pool_scenario;
use test_utils::fuzz::FuzzTest;

pub(crate) struct SpillPoolFuzzer;

impl FuzzTest for SpillPoolFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        "spill_pool_scenario"
    }

    fn prepare_dataset(&self, _: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn run_case(&self, _: &(), case_seed: u64) -> Result<()> {
        run_spill_pool_scenario(case_seed).await
    }
}
