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

extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, LitStr};

#[proc_macro_attribute]
pub fn udf_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut description: Option<LitStr> = None;
    let mut example: Option<LitStr> = None;

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("description") {
            description = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("example") {
            example = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported property"))
        }
    });
    parse_macro_input!(args with parser);
    eprintln!("description={description:?} example={example:?}");

    // Parse the input struct
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.clone().ident;

    //eprintln!("input={input:?}");

    let expanded = quote! {
        #input

        use datafusion_pre_macros::DocumentationTest;

        impl #name {
                fn documentation_test(&self) -> Option<DocumentationTest> {
                    Some(DocumentationTest { description: #description.to_string(), syntax_example: #example.to_string() })
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}
