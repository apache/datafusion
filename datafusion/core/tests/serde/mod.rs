// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Ensure `serde` feature from `arrow-schema` crate is re-exported.
#[test]
#[cfg(feature = "serde")]
fn ensure_serde_support() {
    use datafusion::arrow::datatypes::DataType;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct WrappingStruct(DataType);

    let boolean = WrappingStruct(DataType::Boolean);

    let serialized = serde_json::to_string(&boolean).unwrap();
    assert_eq!("\"Boolean\"", serialized);

    let deserialized = serde_json::from_str(&serialized).unwrap();
    assert_eq!(boolean, deserialized);
}
