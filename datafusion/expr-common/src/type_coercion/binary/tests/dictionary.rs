// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::*;

#[test]
fn test_dictionary_type_coercion() {
    use DataType::*;

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Int32)
    );
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Int32)
    );

    // In comparison context, numeric is preferred over string
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Int16)
    );

    // Since we can coerce values of Utf8 to Binary can support this
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Binary)
    );

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Utf8;
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(lhs_type.clone())
    );

    let lhs_type = Utf8;
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(rhs_type.clone())
    );
}
