// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_execution::runtime_env::RuntimeEnvBuilder;

fn main() {
    let docs = RuntimeEnvBuilder::generate_config_markdown();
    println!("{docs}");
}
