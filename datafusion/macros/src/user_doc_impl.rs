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

use datafusion_doc::scalar_doc_sections::doc_sections_const;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitStr, parse_macro_input};

pub fn user_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut doc_section_lbl: Option<LitStr> = None;

    let mut description: Option<LitStr> = None;
    let mut syntax_example: Option<LitStr> = None;
    let mut alt_syntax_example: Vec<Option<LitStr>> = vec![];
    let mut sql_example: Option<LitStr> = None;
    let mut standard_args: Vec<(Option<LitStr>, Option<LitStr>)> = vec![];
    let mut udf_args: Vec<(Option<LitStr>, Option<LitStr>)> = vec![];
    let mut related_udfs: Vec<Option<LitStr>> = vec![];

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("doc_section") {
            meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("label") {
                    doc_section_lbl = meta.value()?.parse()?;
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
        } else if meta.path.is_ident("alternative_syntax") {
            alt_syntax_example.push(Some(meta.value()?.parse()?));
            Ok(())
        } else if meta.path.is_ident("sql_example") {
            sql_example = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("standard_argument") {
            let mut standard_arg: (Option<LitStr>, Option<LitStr>) = (None, None);
            let m = meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    standard_arg.0 = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("prefix") {
                    standard_arg.1 = meta.value()?.parse()?;
                    return Ok(());
                }
                Ok(())
            });

            standard_args.push(standard_arg.clone());

            m
        } else if meta.path.is_ident("argument") {
            let mut arg: (Option<LitStr>, Option<LitStr>) = (None, None);
            let m = meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    arg.0 = meta.value()?.parse()?;
                    return Ok(());
                } else if meta.path.is_ident("description") {
                    arg.1 = meta.value()?.parse()?;
                    return Ok(());
                }
                Ok(())
            });

            udf_args.push(arg.clone());

            m
        } else if meta.path.is_ident("related_udf") {
            let mut arg: Option<LitStr> = None;
            let m = meta.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    arg = meta.value()?.parse()?;
                    return Ok(());
                }
                Ok(())
            });

            related_udfs.push(arg.clone());

            m
        } else {
            Err(meta.error(format!("Unsupported property: {:?}", meta.path.get_ident())))
        }
    });

    parse_macro_input!(args with parser);

    // Parse the input struct
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.clone().ident;

    if doc_section_lbl.is_none() {
        eprintln!("label for doc_section should exist");
    }
    let label = doc_section_lbl.as_ref().unwrap().value();
    // Try to find a predefined const by label first.
    // If there is no match but label exists, default value will be used for include and description
    let doc_section_option = doc_sections_const().iter().find(|ds| ds.label == label);
    let (doc_section_include, doc_section_label, doc_section_desc) =
        match doc_section_option {
            Some(section) => (section.include, section.label, section.description),
            None => (true, label.as_str(), None),
        };
    let doc_section_description = doc_section_desc
        .map(|desc| quote! { Some(#desc)})
        .unwrap_or_else(|| quote! { None });

    let sql_example = sql_example.map(|ex| {
        quote! {
            .with_sql_example(#ex)
        }
    });

    let udf_args = udf_args
        .iter()
        .map(|(name, desc)| {
            quote! {
                .with_argument(#name, #desc)
            }
        })
        .collect::<Vec<_>>();

    let standard_args = standard_args
        .iter()
        .map(|(name, desc)| {
            let desc = if let Some(d) = desc {
                quote! { #d.into() }
            } else {
                quote! { None }
            };

            quote! {
                .with_standard_argument(#name, #desc)
            }
        })
        .collect::<Vec<_>>();

    let related_udfs = related_udfs
        .iter()
        .map(|name| {
            quote! {
                .with_related_udf(#name)
            }
        })
        .collect::<Vec<_>>();

    let alt_syntax_example = alt_syntax_example.iter().map(|syn| {
        quote! {
            .with_alternative_syntax(#syn)
        }
    });

    let generated = quote! {
        #input

        impl #name {
            fn doc(&self) -> Option<&datafusion_doc::Documentation> {
                static DOCUMENTATION: std::sync::LazyLock<datafusion_doc::Documentation> =
                    std::sync::LazyLock::new(|| {
                        datafusion_doc::Documentation::builder(datafusion_doc::DocSection { include: #doc_section_include, label: #doc_section_label, description: #doc_section_description },
                    #description.to_string(), #syntax_example.to_string())
                        #sql_example
                        #(#alt_syntax_example)*
                        #(#standard_args)*
                        #(#udf_args)*
                        #(#related_udfs)*
                        .build()
                    });
                Some(&DOCUMENTATION)
            }
        }
    };

    // Debug the generated code if needed
    // if name == "ArrayAgg" {
    //     eprintln!("Generated code: {}", generated);
    // }

    // Return the generated code
    TokenStream::from(generated)
}
