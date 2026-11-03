// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! WASM-compatible `Instant` wrapper.

#[cfg(target_family = "wasm")]
/// DataFusion wrapper around [`std::time::Instant`]. Uses [`web_time::Instant`]
/// under `wasm` feature gate. It provides the same API as [`std::time::Instant`].
pub type Instant = web_time::Instant;

#[expect(clippy::disallowed_types)]
#[cfg(not(target_family = "wasm"))]
/// DataFusion wrapper around [`std::time::Instant`]. This is only a type alias.
pub type Instant = std::time::Instant;
