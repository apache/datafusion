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

use datafusion::config::ConfigOptions;
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
        assert_eq!(
            err,
            "Error parsing 'ttruee' as bool\ncaused by\nExternal error: provided string was not `true` or `false`"
        );
        env::remove_var(env_key);

        let env_key = "DATAFUSION_EXECUTION_BATCH_SIZE";

        // for valid testing
        env::set_var(env_key, "4096");
        let config = ConfigOptions::from_env().unwrap();
        assert_eq!(config.execution.batch_size, 4096);

        // for invalid testing
        env::set_var(env_key, "abc");
        let err = ConfigOptions::from_env().unwrap_err().strip_backtrace();
        assert_eq!(
            err,
            "Error parsing 'abc' as usize\ncaused by\nExternal error: invalid digit found in string"
        );

        env::remove_var(env_key);
        let config = ConfigOptions::from_env().unwrap();
        assert_eq!(config.execution.batch_size, 8192); // set to its default value
    }
}
