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

use super::*;

#[test]
fn test_dictionary_type_coercion() {
    use DataType::*;

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Int32)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Int32)
    );

    // Since we can coerce values of Int16 to Utf8 can support this
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Utf8)
    );

    // Since we can coerce values of Utf8 to Binary can support this
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Binary)
    );

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Utf8;
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(lhs_type.clone())
    );

    let lhs_type = Utf8;
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(rhs_type.clone())
    );
}
