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
    let mut doc_section_include: Option<LitStr> = None;
    let mut doc_section_lbl: Option<LitStr> = None;
    let mut doc_section_desc: Option<LitStr> = None;

    let mut description: Option<LitStr> = None;
    let mut syntax_example: Option<LitStr> = None;
    let mut sql_example: Option<LitStr> = None;
    let mut standard_args: Vec<(Option<LitStr>, Option<LitStr>)> = vec![];
    let mut udf_args: Vec<(Option<LitStr>, Option<LitStr>)> = vec![];

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("doc_section") {
            meta.parse_nested_meta(|meta| {
                //dbg!(meta.path);
                if meta.path.is_ident("include") {
                    doc_section_include = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("label") {
                    doc_section_lbl = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("description") {
                    doc_section_desc = meta.value()?.parse()?;
                    return Ok(());
                }
                Ok(())
            })
        } else if meta.path.is_ident("description") {
            description = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("syntax_example") {
            syntax_example = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("sql_example") {
            sql_example = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("standard_argument") {
            let mut standard_arg: (Option<LitStr>, Option<LitStr>) = (None, None);
            meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    standard_arg.0 = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("expression_type") {
                    standard_arg.1 = meta.value()?.parse()?;
                    return Ok(());
                }
                standard_args.push(standard_arg.clone());
                Ok(())
            })
        } else if meta.path.is_ident("argument") {
            let mut arg: (Option<LitStr>, Option<LitStr>) = (None, None);
            meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    arg.0 = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("description") {
                    arg.1 = meta.value()?.parse()?;
                    return Ok(());
                }
                udf_args.push(arg.clone());
                Ok(())
            })
        } else {
            Err(meta.error("unsupported property"))
        }
    });
    parse_macro_input!(args with parser);

    // Parse the input struct
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.clone().ident;

    eprintln!("doc_section_include=cc{doc_section_include:?}cc");
    let doc_section_include: bool = doc_section_include.unwrap().value().parse().unwrap();

    let expanded = quote! {
        #input

        use datafusion_pre_macros::DocumentationTest;
        use datafusion_pre_macros::DocSectionTest;
        use datafusion_pre_macros::DocumentationBuilderTest;

        static DOCUMENTATION_TEST: OnceLock<DocumentationTest> = OnceLock::new();

        impl #name {
                fn documentation_test(&self) -> Option<&DocumentationTest> {
                    Some(DOCUMENTATION_TEST.get_or_init(|| {
                        DocumentationTest::builder()
                        .with_doc_section(DocSectionTest { include: #doc_section_include, label: #doc_section_lbl, description: Some("") })
                        .with_description(#description.to_string())
                        .with_syntax_example(#syntax_example.to_string())
                        .build()
                    }))
            }
        }
    };

    eprintln!("{}", expanded);

    // Return the generated code
    TokenStream::from(expanded)
}
