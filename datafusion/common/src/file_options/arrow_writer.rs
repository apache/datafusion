// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Options related to how Arrow files should be written

#[derive(Clone, Debug)]
pub struct ArrowWriterOptions {}

impl ArrowWriterOptions {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ArrowWriterOptions {
    fn default() -> Self {
        Self::new()
    }
}
