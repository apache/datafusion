// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Schema for the `data/csv/cars.csv` example dataset.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("car", DataType::Utf8, false),
        Field::new("speed", DataType::Float64, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}
