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

//! This example demonstrates how to extend the DataFusion configs with custom extensions.

use datafusion::{
    common::{config::ConfigExtension, extensions_options},
    config::ConfigOptions,
};

extensions_options! {
    /// My own config options.
    pub struct MyConfig {
        /// Should "foo" be replaced by "bar"?
        pub foo_to_bar: bool, default = true

        /// How many "baz" should be created?
        pub baz_count: usize, default = 1337
    }
}

impl ConfigExtension for MyConfig {
    const PREFIX: &'static str = "my_config";
}

fn main() {
    // set up config struct and register extension
    let mut config = ConfigOptions::default();
    config.extensions.insert(MyConfig::default());

    // overwrite config default
    config.set("my_config.baz_count", "42").unwrap();

    // check config state
    let my_config = config.extensions.get::<MyConfig>().unwrap();
    assert!(my_config.foo_to_bar,);
    assert_eq!(my_config.baz_count, 42,);
}
