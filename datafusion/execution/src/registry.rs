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

//! FunctionRegistry trait

use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_expr::{AggregateUDF, ScalarUDF, UserDefinedLogicalNode, WindowUDF};
use std::{collections::HashSet, sync::Arc};

/// A registry knows how to build logical expressions out of user-defined function' names
pub trait FunctionRegistry {
    /// Set of all available udfs.
    fn udfs(&self) -> HashSet<String>;

    /// Returns a reference to the udf named `name`.
    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>>;

    /// Returns a reference to the udaf named `name`.
    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>>;

    /// Returns a reference to the udwf named `name`.
    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>>;

    /// Registers a new `ScalarUDF`, returning any previously registered
    /// implementation.
    ///
    /// Returns an error (default) if the function can not be registered, for
    /// example because the registry doesn't support new functions
    fn register_udf(&mut self, _udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        not_impl_err!("Registering ScalarUDF")
    }

    // TODO add register_udaf and register_udwf
}

/// Serializer and deserializer registry for extensions like [UserDefinedLogicalNode].
pub trait SerializerRegistry: Send + Sync {
    /// Serialize this node to a byte array. This serialization should not include
    /// input plans.
    fn serialize_logical_plan(
        &self,
        node: &dyn UserDefinedLogicalNode,
    ) -> Result<Vec<u8>>;

    /// Deserialize user defined logical plan node ([UserDefinedLogicalNode]) from
    /// bytes.
    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>>;
}
