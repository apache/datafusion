// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Expression utilities

use crate::ScalarValue;

///  The value to which `COUNT(*)` is expanded to in
///  `COUNT(<constant>)` expressions
pub const COUNT_STAR_EXPANSION: ScalarValue = ScalarValue::Int64(Some(1));
