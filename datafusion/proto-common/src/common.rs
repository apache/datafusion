// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::{DataFusionError, internal_datafusion_err};

/// Return a `DataFusionError::Internal` with the given message
pub fn proto_error<S: Into<String>>(message: S) -> DataFusionError {
    internal_datafusion_err!("{}", message.into())
}
