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
fn get_config_bool_from_env() {
    let env_key = "DATAFUSION_OPTIMIZER_FILTER_NULL_JOIN_KEYS";
    env::set_var(env_key, "true");
    let config = ConfigOptions::from_env().unwrap();
    env::remove_var(env_key);
    assert!(config.built_in.optimizer.filter_null_join_keys);
}

#[test]
fn get_config_int_from_env() {
    let env_key = "DATAFUSION_EXECUTION_BATCH_SIZE";
    env::set_var(env_key, "4096");
    let config = ConfigOptions::from_env().unwrap();
    env::remove_var(env_key);
    assert_eq!(config.built_in.execution.batch_size, 4096);
}

#[test]
fn get_config_int_from_env_invalid() {
    let env_key = "DATAFUSION_EXECUTION_COALESCE_TARGET_BATCH_SIZE";
    env::set_var(env_key, "abc");
    let config = ConfigOptions::from_env().unwrap();
    env::remove_var(env_key);
    assert_eq!(config.built_in.execution.coalesce_target_batch_size, 4096); // set to its default value
}
