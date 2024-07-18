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

use datafusion::common::{plan_err, DataFusionError};
use std::collections::HashMap;
use substrait::proto::extensions::simple_extension_declaration::{
    ExtensionFunction, ExtensionType, ExtensionTypeVariation, MappingType,
};
use substrait::proto::extensions::SimpleExtensionDeclaration;

#[derive(Default)]
pub struct Extensions {
    pub functions: HashMap<u32, String>,
    pub types: HashMap<u32, String>,
    pub type_variations: HashMap<u32, String>,
}

impl Extensions {
    pub fn register_function(&mut self, function_name: String) -> u32 {
        let function_name = function_name.to_lowercase();

        // Some functions are named differently in Substrait default extensions than in DF
        // Rename those to match the Substrait extensions for interoperability
        let function_name = match function_name.as_str() {
            "substr" => "substring".to_string(),
            _ => function_name,
        };

        match self.functions.iter().find(|(_, f)| *f == &function_name) {
            Some((function_anchor, _)) => *function_anchor, // Function has been registered
            None => {
                // Function has NOT been registered
                let function_anchor = self.functions.len() as u32;
                self.functions
                    .insert(function_anchor, function_name.clone());
                function_anchor
            }
        }
    }

    pub fn register_type(&mut self, type_name: String) -> u32 {
        let type_name = type_name.to_lowercase();
        match self.types.iter().find(|(_, t)| *t == &type_name) {
            Some((type_anchor, _)) => *type_anchor, // Type has been registered
            None => {
                // Type has NOT been registered
                let type_anchor = self.types.len() as u32;
                self.types.insert(type_anchor, type_name.clone());
                type_anchor
            }
        }
    }
}

impl TryFrom<&Vec<SimpleExtensionDeclaration>> for Extensions {
    type Error = DataFusionError;

    fn try_from(
        value: &Vec<SimpleExtensionDeclaration>,
    ) -> datafusion::common::Result<Self> {
        let mut functions = HashMap::new();
        let mut types = HashMap::new();
        let mut type_variations = HashMap::new();

        for ext in value {
            match &ext.mapping_type {
                Some(MappingType::ExtensionFunction(ext_f)) => {
                    functions.insert(ext_f.function_anchor, ext_f.name.to_owned());
                }
                Some(MappingType::ExtensionType(ext_t)) => {
                    types.insert(ext_t.type_anchor, ext_t.name.to_owned());
                }
                Some(MappingType::ExtensionTypeVariation(ext_v)) => {
                    type_variations
                        .insert(ext_v.type_variation_anchor, ext_v.name.to_owned());
                }
                None => return plan_err!("Cannot parse empty extension"),
            }
        }

        Ok(Extensions {
            functions,
            types,
            type_variations,
        })
    }
}

impl From<Extensions> for Vec<SimpleExtensionDeclaration> {
    fn from(val: Extensions) -> Vec<SimpleExtensionDeclaration> {
        let mut extensions = vec![];
        for (f_anchor, f_name) in val.functions {
            let function_extension = ExtensionFunction {
                extension_uri_reference: u32::MAX,
                function_anchor: f_anchor,
                name: f_name,
            };
            let simple_extension = SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(function_extension)),
            };
            extensions.push(simple_extension);
        }

        for (t_anchor, t_name) in val.types {
            let type_extension = ExtensionType {
                extension_uri_reference: u32::MAX, // We don't register proper extension URIs yet
                type_anchor: t_anchor,
                name: t_name,
            };
            let simple_extension = SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionType(type_extension)),
            };
            extensions.push(simple_extension);
        }

        for (tv_anchor, tv_name) in val.type_variations {
            let type_variation_extension = ExtensionTypeVariation {
                extension_uri_reference: u32::MAX, // We don't register proper extension URIs yet
                type_variation_anchor: tv_anchor,
                name: tv_name,
            };
            let simple_extension = SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionTypeVariation(
                    type_variation_extension,
                )),
            };
            extensions.push(simple_extension);
        }

        extensions
    }
}
