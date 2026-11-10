// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod csv_to_parquet;
pub mod datasets;
pub mod example_metadata;

pub use csv_to_parquet::write_csv_to_parquet;
