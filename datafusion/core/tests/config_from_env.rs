// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::config::ConfigOptions;
use datafusion_common::assert_contains;
use std::env;

#[test]
fn from_env() {
    unsafe {
        // Note: these must be a single test to avoid interference from concurrent execution
        let env_key = "DATAFUSION_OPTIMIZER_FILTER_NULL_JOIN_KEYS";
        // valid testing in different cases
        for bool_option in ["true", "TRUE", "True", "tRUe"] {
            env::set_var(env_key, bool_option);
            let config = ConfigOptions::from_env().unwrap();
            env::remove_var(env_key);
            assert!(config.optimizer.filter_null_join_keys);
        }

        // invalid testing
        env::set_var(env_key, "ttruee");
        let err = ConfigOptions::from_env().unwrap_err().strip_backtrace();
        assert_contains!(
            err,
            "Error parsing 'ttruee' as bool\ncaused by\nExternal error: provided string was not `true` or `false`"
        );
        env::remove_var(env_key);

        let env_key = "DATAFUSION_EXECUTION_BATCH_SIZE";

        // for valid testing
        env::set_var(env_key, "4096");
        let config = ConfigOptions::from_env().unwrap();
        assert_eq!(config.execution.batch_size.get(), 4096);

        // for invalid testing
        env::set_var(env_key, "abc");
        let err = ConfigOptions::from_env().unwrap_err().strip_backtrace();
        assert_contains!(
            err,
            "Error parsing 'abc' as usize\ncaused by\nExternal error: invalid digit found in string"
        );

        env::remove_var(env_key);
        let config = ConfigOptions::from_env().unwrap();
        assert_eq!(config.execution.batch_size.get(), 8192); // set to its default value
    }
}
