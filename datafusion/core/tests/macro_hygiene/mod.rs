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

//! Verifies [Macro Hygiene]
//!
//! [Macro Hygiene]: https://en.wikipedia.org/wiki/Hygienic_macro

mod plan_err {
    // NO other imports!
    use datafusion_common::plan_err;

    #[test]
    fn test_macro() {
        // need type annotation for Ok variant
        let _res: Result<(), _> = plan_err!("foo");
    }
}

mod plan_datafusion_err {
    // NO other imports!
    use datafusion_common::plan_datafusion_err;

    #[test]
    fn test_macro() {
        plan_datafusion_err!("foo");
    }
}

mod record_batch {
    // NO other imports!
    use datafusion_common::record_batch;

    #[test]
    fn test_macro() {
        record_batch!(("column_name", Int32, vec![1, 2, 3])).unwrap();
    }
}

mod config_namespace {
    // NO other imports!
    use datafusion_common::config_namespace;

    #[test]
    fn test_macro() {
        config_namespace! {
            /// A config section
            pub struct Foo {
                /// Some doc comments
                pub bar: bool, default = true
            }
        }
    }
}

mod config_field {
    // NO other imports!
    use datafusion_common::config_field;

    #[test]
    fn test_macro() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct E;

        impl std::fmt::Display for E {
            fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                unimplemented!()
            }
        }

        impl std::error::Error for E {}

        #[allow(dead_code)]
        #[derive(Default)]
        struct S;

        impl std::str::FromStr for S {
            type Err = E;

            fn from_str(_s: &str) -> Result<Self, Self::Err> {
                unimplemented!()
            }
        }

        impl std::fmt::Display for S {
            fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                unimplemented!()
            }
        }

        config_field!(S);
    }
}
