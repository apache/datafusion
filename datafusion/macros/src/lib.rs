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

/// This procedural macro is intended to parse a rust custom attribute and create user documentation
/// from it by constructing a `DocumentBuilder()` automatically. The `Documentation` can be
/// retrieved from the `documentation()` method
/// declared on `AggregateUDF`, `WindowUDFImpl`, `ScalarUDFImpl` traits.
///
/// Example:
/// #[user_doc(
///     doc_section(include = "true", label = "Time and Date Functions"),
///     description = r"Converts a value to a date (`YYYY-MM-DD`)."
///     sql_example = "```sql\n\
/// \> select to_date('2023-01-31');\n\
/// +-----------------------------+\n\
/// | to_date(Utf8(\"2023-01-31\")) |\n\
/// +-----------------------------+\n\
/// | 2023-01-31                  |\n\
/// +-----------------------------+\n\"),
///     standard_argument(name = "expression", prefix = "String"),
///     argument(
///         name = "format_n",
///         description = r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
///   they appear with the first successful one being returned. If none of the formats successfully parse the expression
///   an error will be returned."
///    )
/// )]
/// #[derive(Debug)]
/// pub struct ToDateFunc {
///     signature: Signature,
/// }
///
/// will generate the following code
///
/// #[derive(Debug)] pub struct ToDateFunc { signature : Signature, }
/// use datafusion_doc :: DocSection;
/// use datafusion_doc :: DocumentationBuilder;
/// static DOCUMENTATION : OnceLock < Documentation > = OnceLock :: new();
/// impl ToDateFunc
/// {
///     fn doc(& self) -> Option < & Documentation >
///     {
///         Some(DOCUMENTATION.get_or_init(||
///         {
///             Documentation ::
///             builder().with_doc_section(DocSection
///             {
///                 include : true, label : "Time and Date Functions", description
///                 : None
///             }).with_description(r"Converts a value to a date (`YYYY-MM-DD`).")
/// .with_syntax_example("to_date('2017-05-31', '%Y-%m-%d')".to_string()).with_sql_example("```sql\n\
/// \> select to_date('2023-01-31');\n\
/// +-----------------------------+\n\
/// | to_date(Utf8(\"2023-01-31\")) |\n\
/// +-----------------------------+\n\
/// | 2023-01-31                  |\n\
/// +-----------------------------+\n\)
/// .with_standard_argument("expression", "String".into())
/// .with_argument("format_n",
///             r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
///   they appear with the first successful one being returned. If none of the formats successfully parse the expression
///   an error will be returned.").build()
///         }))
///     }
/// }
#[proc_macro_attribute]
pub fn user_doc(args: TokenStream, input: TokenStream) -> TokenStream {
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
        } else {
            Err(meta.error(format!("Unsupported property {:?}", meta.path.get_ident())))
        }
    });

    parse_macro_input!(args with parser);

    // Parse the input struct
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.clone().ident;

    let doc_section_include: bool = doc_section_include.unwrap().value().parse().unwrap();
    let doc_section_description = doc_section_desc
        .map(|desc| quote! { Some(#desc)})
        .unwrap_or(quote! { None });

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
            quote! {
                .with_standard_argument(#name, #desc.into())
            }
        })
        .collect::<Vec<_>>();

    let generated = quote! {
        #input

        use datafusion_doc::DocSection;
        use datafusion_doc::DocumentationBuilder;

        static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

        impl #name {
                fn doc(&self) -> Option<&Documentation> {
                    Some(DOCUMENTATION.get_or_init(|| {
                        Documentation::builder()
                        .with_doc_section(DocSection { include: #doc_section_include, label: #doc_section_lbl, description: #doc_section_description })
                        .with_description(#description.to_string())
                        .with_syntax_example(#syntax_example.to_string())
                        .with_sql_example(#sql_example.to_string())
                        #(#standard_args)*
                        #(#udf_args)*
                        .build()
                    }))
            }
        }
    };

    // Debug the generated code if needed
    // eprintln!("Generated code: {}", generated);

    // Return the generated code
    TokenStream::from(generated)
}
