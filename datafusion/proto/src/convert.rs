// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Conversion traits between proto-generated types and DataFusion types.
//!
//! The `prost`-generated structs now live in `datafusion-proto-models`, while
//! their counterparts (`StringifiedPlan`, `JoinType`, `WindowFrame`, ...) live
//! in `datafusion-common` / `datafusion-expr` / `datafusion-datasource` etc.
//! Both sides are foreign to `datafusion-proto`, which means the orphan rule
//! forbids a direct `impl From<&protobuf::X> for Y` written here.
//!
//! To keep the conversion logic colocated with serialization while satisfying
//! the orphan rule, we route those conversions through the `FromProto` /
//! `TryFromProto` traits defined in this module. Their signatures mirror the
//! standard library's `From` / `TryFrom`, so callers spell the conversion
//! `Y::from_proto(&p)` / `Y::try_from_proto(&p)?` instead of
//! `(&p).into()` / `(&p).try_into()?`.

/// Infallible conversion from a proto value into a DataFusion value (or vice
/// versa). Mirrors [`From`].
pub trait FromProto<T>: Sized {
    fn from_proto(value: T) -> Self;
}

/// Fallible conversion from a proto value into a DataFusion value (or vice
/// versa). Mirrors [`TryFrom`].
pub trait TryFromProto<T>: Sized {
    type Error;
    fn try_from_proto(value: T) -> std::result::Result<Self, Self::Error>;
}
