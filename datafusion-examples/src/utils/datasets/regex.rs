// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Schema for the `data/csv/regex.csv` example dataset.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("values", DataType::Utf8, false),
        Field::new("patterns", DataType::Utf8, false),
        Field::new("replacement", DataType::Utf8, false),
        Field::new("flags", DataType::Utf8, true),
    ]))
}
