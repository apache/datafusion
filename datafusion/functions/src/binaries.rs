// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::strings::{ConcatByteArrayBuilder, ConcatByteViewBuilder};
use arrow::datatypes::{BinaryType, BinaryViewType, LargeBinaryType};

pub(crate) type ConcatBinaryBuilder = ConcatByteArrayBuilder<BinaryType>;
pub(crate) type ConcatLargeBinaryBuilder = ConcatByteArrayBuilder<LargeBinaryType>;
pub(crate) type ConcatBinaryViewBuilder = ConcatByteViewBuilder<BinaryViewType>;
