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

use crate::expr_rewriter::FunctionRewrite;
use crate::planner::ExprPlanner;
use crate::{AggregateUDF, ScalarUDF, UserDefinedLogicalNode, WindowUDF};
use arrow::datatypes::Field;
use arrow_schema::extension::ExtensionType;
use datafusion_common::types::{DFExtensionType, DFExtensionTypeRef};
use datafusion_common::{HashMap, Result, not_impl_err, plan_datafusion_err};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

/// A registry knows how to build logical expressions out of user-defined function' names
pub trait FunctionRegistry {
    /// Returns names of all available scalar user defined functions.
    fn udfs(&self) -> HashSet<String>;

    /// Returns names of all available aggregate user defined functions.
    fn udafs(&self) -> HashSet<String>;

    /// Returns names of all available window user defined functions.
    fn udwfs(&self) -> HashSet<String>;

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

    /// Registers a new [`FunctionRewrite`] with the registry.
    ///
    /// `FunctionRewrite` rules are used to rewrite certain / operators in the
    /// logical plan to function calls.  For example `a || b` might be written to
    /// `array_concat(a, b)`.
    ///
    /// This allows the behavior of operators to be customized by the user.
    fn register_function_rewrite(
        &mut self,
        _rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> Result<()> {
        not_impl_err!("Registering FunctionRewrite")
    }

    /// Set of all registered [`ExprPlanner`]s
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>>;

    /// Registers a new [`ExprPlanner`] with the registry.
    fn register_expr_planner(
        &mut self,
        _expr_planner: Arc<dyn ExprPlanner>,
    ) -> Result<()> {
        not_impl_err!("Registering ExprPlanner")
    }
}

/// Serializer and deserializer registry for extensions like [UserDefinedLogicalNode].
pub trait SerializerRegistry: Debug + Send + Sync {
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

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }

    fn udafs(&self) -> HashSet<String> {
        self.udafs.keys().cloned().collect()
    }

    fn udwfs(&self) -> HashSet<String> {
        self.udwfs.keys().cloned().collect()
    }
}

/// A cheaply cloneable pointer to an [ExtensionTypeRegistration].
pub type ExtensionTypeRegistrationRef = Arc<dyn ExtensionTypeRegistration>;

/// The registration of an extension type. Implementations of this trait are responsible for
/// *creating* instances of [`DFExtensionType`] that represent the entire semantics of an extension
/// type.
///
/// # Why do we need a Registration?
///
/// A good question is why this trait is even necessary. Why not directly register the
/// [`DFExtensionType`] in a registration?
///
/// While this works for extension types without parameters (e.g., `arrow.uuid`), it does not work
/// for more complex extension types that may have another extension type as a parameter. For
/// example, consider an extension type `custom.shortened(n)` that aims to short the pretty-printing
/// string to `n` characters. Here, `n` is a parameter of the extension type and should be a field
/// in the concrete struct that implements the [`DFExtensionType`]. The job of the registration is
/// to read the metadata from the field and create the corresponding [`DFExtensionType`] instance
/// with the correct `n` set.
///
/// The [`DefaultExtensionTypeRegistration`] provides a convenient way of creating registrations.
pub trait ExtensionTypeRegistration: Debug + Send + Sync {
    /// The name of the extension type.
    ///
    /// This name will be used to find the correct [ExtensionTypeRegistration] when an extension
    /// type is encountered.
    fn type_name(&self) -> &str;

    /// Creates an extension type instance from the optional metadata. The name of the extension
    /// type is not a parameter as it's already defined by the registration itself.
    fn create_df_extension_type(
        &self,
        metadata: Option<&str>,
    ) -> Result<DFExtensionTypeRef>;
}

/// A cheaply cloneable pointer to an [ExtensionTypeRegistry].
pub type ExtensionTypeRegistryRef = Arc<dyn ExtensionTypeRegistry>;

/// Manages [`ExtensionTypeRegistration`]s, which allow users to register custom behavior for
/// extension types.
///
/// Each registration is connected to the extension type name, which can also be looked up to get
/// the registration.
pub trait ExtensionTypeRegistry: Debug + Send + Sync {
    /// Returns a reference to registration of an extension type named `name`.
    ///
    /// Returns an error if there is no extension type with that name.
    fn extension_type_registration(
        &self,
        name: &str,
    ) -> Result<ExtensionTypeRegistrationRef>;

    /// Creates a [`DFExtensionTypeRef`] from the type information in the `field`.
    ///
    /// The result `Ok(None)` indicates that there is no extension type metadata. Returns an error
    /// if the extension type in the metadata is not found.
    fn create_extension_type_for_field(
        &self,
        field: &Field,
    ) -> Result<Option<DFExtensionTypeRef>> {
        let Some(extension_type_name) = field.extension_type_name() else {
            return Ok(None);
        };

        let registration = self.extension_type_registration(extension_type_name)?;
        registration
            .create_df_extension_type(field.extension_type_metadata())
            .map(Some)
    }

    /// Returns all registered [ExtensionTypeRegistration].
    fn extension_type_registrations(&self) -> Vec<Arc<dyn ExtensionTypeRegistration>>;

    /// Registers a new [ExtensionTypeRegistrationRef], returning any previously registered
    /// implementation.
    ///
    /// Returns an error if the type cannot be registered, for example, if the registry is
    /// read-only.
    fn add_extension_type_registration(
        &self,
        extension_type: ExtensionTypeRegistrationRef,
    ) -> Result<Option<ExtensionTypeRegistrationRef>>;

    /// Extends the registry with the provided extension types.
    ///
    /// Returns an error if the type cannot be registered, for example, if the registry is
    /// read-only.
    fn extend(&self, extension_types: &[ExtensionTypeRegistrationRef]) -> Result<()> {
        for extension_type in extension_types.iter().cloned() {
            self.add_extension_type_registration(extension_type)?;
        }
        Ok(())
    }

    /// Deregisters an extension type registration with the name `name`, returning the
    /// implementation that was deregistered.
    ///
    /// Returns an error if the type cannot be deregistered, for example, if the registry is
    /// read-only.
    fn remove_extension_type_registration(
        &self,
        name: &str,
    ) -> Result<Option<ExtensionTypeRegistrationRef>>;
}

/// A default implementation of [ExtensionTypeRegistration] that parses the metadata from the
/// given extension type and passes it to a constructor function.
pub struct DefaultExtensionTypeRegistration<
    TExtensionType: ExtensionType + DFExtensionType + 'static,
> {
    /// A function that creates an instance of [`DFExtensionTypeRef`] from the metadata.
    factory:
        Box<dyn Fn(TExtensionType::Metadata) -> Result<TExtensionType> + Send + Sync>,
}

impl<TExtensionType: ExtensionType + DFExtensionType + 'static>
    DefaultExtensionTypeRegistration<TExtensionType>
{
    /// Creates a new registration for the given `name` and `logical_type`.
    pub fn new_arc(
        factory: impl Fn(TExtensionType::Metadata) -> Result<TExtensionType>
        + Send
        + Sync
        + 'static,
    ) -> ExtensionTypeRegistrationRef {
        Arc::new(Self {
            factory: Box::new(factory),
        })
    }
}

impl<TExtensionType: ExtensionType + DFExtensionType> ExtensionTypeRegistration
    for DefaultExtensionTypeRegistration<TExtensionType>
{
    fn type_name(&self) -> &str {
        TExtensionType::NAME
    }

    fn create_df_extension_type(
        &self,
        metadata: Option<&str>,
    ) -> Result<DFExtensionTypeRef> {
        let metadata = TExtensionType::deserialize_metadata(metadata)?;
        self.factory.as_ref()(metadata)
            .map(|extension_type| Arc::new(extension_type) as DFExtensionTypeRef)
    }
}

impl<TExtensionType: ExtensionType + DFExtensionType> Debug
    for DefaultExtensionTypeRegistration<TExtensionType>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultExtensionTypeRegistration")
            .field("type_name", &TExtensionType::NAME)
            .finish()
    }
}

/// An [`ExtensionTypeRegistry`] that uses in memory [`HashMap`]s.
#[derive(Clone, Debug)]
pub struct MemoryExtensionTypeRegistry {
    /// Holds a mapping between the name of an extension type and its logical type.
    extension_types: Arc<RwLock<HashMap<String, ExtensionTypeRegistrationRef>>>,
}

impl Default for MemoryExtensionTypeRegistry {
    fn default() -> Self {
        MemoryExtensionTypeRegistry {
            extension_types: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl MemoryExtensionTypeRegistry {
    /// Creates an empty [MemoryExtensionTypeRegistry].
    pub fn new() -> Self {
        Self {
            extension_types: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new [MemoryExtensionTypeRegistry] with the provided `types`.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the `types` is a native type.
    pub fn new_with_types(
        types: impl IntoIterator<Item = ExtensionTypeRegistrationRef>,
    ) -> Result<Self> {
        let extension_types = types
            .into_iter()
            .map(|t| (t.type_name().to_owned(), t))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            extension_types: Arc::new(RwLock::new(extension_types)),
        })
    }

    /// Returns a list of all registered types.
    pub fn all_extension_types(&self) -> Vec<ExtensionTypeRegistrationRef> {
        self.extension_types
            .read()
            .expect("Extension type registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl ExtensionTypeRegistry for MemoryExtensionTypeRegistry {
    fn extension_type_registration(
        &self,
        name: &str,
    ) -> Result<ExtensionTypeRegistrationRef> {
        self.extension_types
            .write()
            .expect("Extension type registry lock poisoned")
            .get(name)
            .ok_or_else(|| plan_datafusion_err!("Logical type not found."))
            .cloned()
    }

    fn extension_type_registrations(&self) -> Vec<Arc<dyn ExtensionTypeRegistration>> {
        self.extension_types
            .read()
            .expect("Extension type registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn add_extension_type_registration(
        &self,
        extension_type: ExtensionTypeRegistrationRef,
    ) -> Result<Option<ExtensionTypeRegistrationRef>> {
        Ok(self
            .extension_types
            .write()
            .expect("Extension type registry lock poisoned")
            .insert(extension_type.type_name().to_owned(), extension_type))
    }

    fn remove_extension_type_registration(
        &self,
        name: &str,
    ) -> Result<Option<ExtensionTypeRegistrationRef>> {
        Ok(self
            .extension_types
            .write()
            .expect("Extension type registry lock poisoned")
            .remove(name))
    }
}

impl From<HashMap<String, ExtensionTypeRegistrationRef>> for MemoryExtensionTypeRegistry {
    fn from(value: HashMap<String, ExtensionTypeRegistrationRef>) -> Self {
        Self {
            extension_types: Arc::new(RwLock::new(value)),
        }
    }
}
