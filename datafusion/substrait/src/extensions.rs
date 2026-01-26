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

use datafusion::common::{HashMap, plan_err};
use substrait::proto::extensions::{SimpleExtensionDeclaration, SimpleExtensionUri};
use substrait::proto::extensions::simple_extension_declaration::{
    ExtensionFunction, ExtensionType, ExtensionTypeVariation, MappingType,
};

/// Arrow's official Substrait extension types URI
pub const ARROW_EXTENSION_TYPES_URI: &str =
    "https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml";

/// Substrait uses [SimpleExtensions](https://substrait.io/extensions/#simple-extensions) to define
/// behavior of plans in addition to what's supported directly by the protobuf definitions.
/// That includes functions, but also provides support for custom types and variations for existing
/// types. This structs facilitates the use of these extensions in DataFusion.
/// TODO: DF doesn't yet use extensions for type variations <https://github.com/apache/datafusion/issues/11544>
#[derive(Default, Debug, PartialEq)]
pub struct Extensions {
    pub uris: HashMap<u32, String>,      // anchor -> URI
    pub functions: HashMap<u32, String>, // anchor -> function name
    pub types: HashMap<u32, (String, u32)>, // anchor -> (type name, uri_anchor)
    pub type_variations: HashMap<u32, String>, // anchor -> type variation name
}

impl Extensions {
    /// Registers a function and returns the anchor (reference) to it. If the function has already
    /// been registered, it returns the existing anchor.
    /// Function names are case-insensitive (converted to lowercase).
    pub fn register_function(&mut self, function_name: &str) -> u32 {
        let function_name = function_name.to_lowercase();

        // Some functions are named differently in Substrait default extensions than in DF
        // Rename those to match the Substrait extensions for interoperability
        let function_name = match function_name.as_str() {
            "substr" => "substring".to_string(),
            "log" => "logb".to_string(),
            "isnan" => "is_nan".to_string(),
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

    /// Registers a URI and returns the anchor (reference) to it. If the URI has already
    /// been registered, it returns the existing anchor.
    pub fn register_uri(&mut self, uri: &str) -> u32 {
        match self.uris.iter().find(|(_, u)| *u == uri) {
            Some((uri_anchor, _)) => *uri_anchor, // URI has been registered
            None => {
                // URI has NOT been registered
                let uri_anchor = self.uris.len() as u32;
                self.uris.insert(uri_anchor, uri.to_string());
                uri_anchor
            }
        }
    }

    /// Registers an Arrow extension type and returns the anchor (reference) to it.
    /// Uses the official Arrow extension types URI by default.
    /// If the type has already been registered, it returns the existing anchor.
    pub fn register_type(&mut self, type_name: &str) -> u32 {
        let type_name = type_name.to_lowercase();
        let uri_anchor = self.register_uri(ARROW_EXTENSION_TYPES_URI);
        match self.types.iter().find(|(_, (t, _))| *t == type_name) {
            Some((type_anchor, _)) => *type_anchor, // Type has been registered
            None => {
                // Type has NOT been registered
                let type_anchor = self.types.len() as u32;
                self.types.insert(type_anchor, (type_name.clone(), uri_anchor));
                type_anchor
            }
        }
    }
}

impl Extensions {
    /// Parse extensions from Substrait plan components
    pub fn from_substrait(
        extension_uris: &[SimpleExtensionUri],
        extensions: &[SimpleExtensionDeclaration],
    ) -> datafusion::common::Result<Self> {
        let mut uris = HashMap::new();
        let mut functions = HashMap::new();
        let mut types = HashMap::new();
        let mut type_variations = HashMap::new();

        for uri in extension_uris {
            uris.insert(uri.extension_uri_anchor, uri.uri.clone());
        }

        for ext in extensions {
            match &ext.mapping_type {
                Some(MappingType::ExtensionFunction(ext_f)) => {
                    functions.insert(ext_f.function_anchor, ext_f.name.to_owned());
                }
                Some(MappingType::ExtensionType(ext_t)) => {
                    let uri_anchor = ext_t.extension_urn_reference;
                    types.insert(ext_t.type_anchor, (ext_t.name.to_owned(), uri_anchor));
                }
                Some(MappingType::ExtensionTypeVariation(ext_v)) => {
                    type_variations
                        .insert(ext_v.type_variation_anchor, ext_v.name.to_owned());
                }
                None => return plan_err!("Cannot parse empty extension"),
            }
        }

        Ok(Extensions {
            uris,
            functions,
            types,
            type_variations,
        })
    }

    /// Get extension URIs for Substrait plan
    pub fn to_extension_uris(&self) -> Vec<SimpleExtensionUri> {
        self.uris
            .iter()
            .map(|(anchor, uri)| SimpleExtensionUri {
                extension_uri_anchor: *anchor,
                uri: uri.clone(),
            })
            .collect()
    }
}

impl From<Extensions> for Vec<SimpleExtensionDeclaration> {
    // Silence deprecation warnings for `extension_uri_reference` during the uri -> urn migration
    // See: https://github.com/substrait-io/substrait/issues/856
    #[expect(deprecated)]
    fn from(val: Extensions) -> Vec<SimpleExtensionDeclaration> {
        let mut extensions = vec![];
        for (f_anchor, f_name) in val.functions {
            let function_extension = ExtensionFunction {
                extension_uri_reference: u32::MAX,
                extension_urn_reference: u32::MAX,
                function_anchor: f_anchor,
                name: f_name,
            };
            let simple_extension = SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(function_extension)),
            };
            extensions.push(simple_extension);
        }

        for (t_anchor, (t_name, uri_anchor)) in val.types {
            let type_extension = ExtensionType {
                extension_uri_reference: uri_anchor,
                extension_urn_reference: uri_anchor,
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
                extension_urn_reference: u32::MAX, // We don't register proper extension URIs yet
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
