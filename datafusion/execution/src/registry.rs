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

use datafusion_common::{not_impl_err, plan_datafusion_err, Result};
use datafusion_expr::{AggregateUDF, ScalarUDF, UserDefinedLogicalNode, WindowUDF};
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc};

/// A registry knows how to build logical expressions out of user-defined function' names
pub trait FunctionRegistry {
    /// Set of all available udfs.
    fn udfs(&self) -> HashSet<String>;

    /// Returns a reference to the user defined scalar function (udf) named
    /// `name`.
    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>>;

    /// Returns a reference to the user defined aggregate function (udaf) named
    /// `name`.
    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>>;

    /// Returns a reference to the user defined window function (udwf) named
    /// `name`.
    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>>;

    /// Registers a new [`ScalarUDF`], returning any previously registered
    /// implementation.
    ///
    /// Returns an error (the default) if the function can not be registered,
    /// for example if the registry is read only.
    fn register_udf(&mut self, _udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        not_impl_err!("Registering ScalarUDF")
    }
    /// Registers a new [`AggregateUDF`], returning any previously registered
    /// implementation.
    ///
    /// Returns an error (the default) if the function can not be registered,
    /// for example if the registry is read only.
    fn register_udaf(
        &mut self,
        _udaf: Arc<AggregateUDF>,
    ) -> Result<Option<Arc<AggregateUDF>>> {
        not_impl_err!("Registering AggregateUDF")
    }
    /// Registers a new [`WindowUDF`], returning any previously registered
    /// implementation.
    ///
    /// Returns an error (the default) if the function can not be registered,
    /// for example if the registry is read only.
    fn register_udwf(&mut self, _udaf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        not_impl_err!("Registering WindowUDF")
    }

    /// Deregisters a [`ScalarUDF`], returning the implementation that was
    /// deregistered.
    ///
    /// Returns an error (the default) if the function can not be deregistered,
    /// for example if the registry is read only.
    fn deregister_udf(&mut self, _name: &str) -> Result<Option<Arc<ScalarUDF>>> {
        not_impl_err!("Deregistering ScalarUDF")
    }

    /// Deregisters a [`AggregateUDF`], returning the implementation that was
    /// deregistered.
    ///
    /// Returns an error (the default) if the function can not be deregistered,
    /// for example if the registry is read only.
    fn deregister_udaf(&mut self, _name: &str) -> Result<Option<Arc<AggregateUDF>>> {
        not_impl_err!("Deregistering AggregateUDF")
    }

    /// Deregisters a [`WindowUDF`], returning the implementation that was
    /// deregistered.
    ///
    /// Returns an error (the default) if the function can not be deregistered,
    /// for example if the registry is read only.
    fn deregister_udwf(&mut self, _name: &str) -> Result<Option<Arc<WindowUDF>>> {
        not_impl_err!("Deregistering WindowUDF")
    }
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

/// A  [`FunctionRegistry`] that uses in memory [`HashMap`]s
#[derive(Default, Debug)]
pub struct MemoryFunctionRegistry {
    /// Scalar Functions
    udfs: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate Functions
    udafs: HashMap<String, Arc<AggregateUDF>>,
    /// Window Functions
    udwfs: HashMap<String, Arc<WindowUDF>>,
}

impl MemoryFunctionRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FunctionRegistry for MemoryFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.udfs
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Function {name} not found"))
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.udafs
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Aggregate Function {name} not found"))
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        self.udwfs
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Window Function {name} not found"))
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        Ok(self.udfs.insert(udf.name().to_string(), udf))
    }
    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> Result<Option<Arc<AggregateUDF>>> {
        Ok(self.udafs.insert(udaf.name().into(), udaf))
    }
    fn register_udwf(&mut self, udaf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        Ok(self.udwfs.insert(udaf.name().into(), udaf))
    }
}
