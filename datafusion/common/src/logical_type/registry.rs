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

use std::collections::HashMap;

use crate::error::_plan_datafusion_err;
use crate::logical_type::extension::ExtensionTypeRef;
use crate::logical_type::type_signature::TypeSignature;

pub trait TypeRegistry {
    fn register_data_type(
        &mut self,
        extension_type: ExtensionTypeRef,
    ) -> crate::Result<Option<ExtensionTypeRef>>;

    fn data_type(&self, signature: &TypeSignature) -> crate::Result<ExtensionTypeRef>;
}


#[derive(Default, Debug)]
pub struct MemoryTypeRegistry {
    types: HashMap<TypeSignature, ExtensionTypeRef>,
}

impl TypeRegistry for MemoryTypeRegistry {
    fn register_data_type(&mut self, extension_type: ExtensionTypeRef) -> crate::Result<Option<ExtensionTypeRef>> {
        Ok(self.types.insert(extension_type.type_signature(), extension_type))
    }

    fn data_type(&self, signature: &TypeSignature) -> crate::Result<ExtensionTypeRef> {
        self.types
            .get(signature)
            .cloned()
            .ok_or_else(|| _plan_datafusion_err!("Type with signature {signature:?} not found"))
    }
}

