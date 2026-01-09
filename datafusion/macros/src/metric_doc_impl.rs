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

//! Metrics documentation macros and helpers.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Lit, Path, Token, parse::Parser,
    spanned::Spanned,
};

pub fn metric_doc(args: &TokenStream, input: TokenStream) -> TokenStream {
    let args_ts = proc_macro2::TokenStream::from(args.clone());
    let parsed: syn::punctuated::Punctuated<syn::Meta, Token![,]> =
        match syn::punctuated::Punctuated::parse_terminated.parse2(args_ts.clone()) {
            Ok(p) => p,
            Err(err) => return err.to_compile_error().into(),
        };

    let is_metrics_struct = parsed.is_empty() || parsed.iter().all(is_position_arg);
    let derive_attr = if is_metrics_struct {
        quote!(#[derive(datafusion_macros::DocumentedMetrics)])
    } else {
        quote!(#[derive(datafusion_macros::DocumentedExec)])
    };
    let meta_attr = quote!(#[metric_doc_attr(#args_ts)]);
    let input_ts = proc_macro2::TokenStream::from(input);

    quote!(#derive_attr #meta_attr #input_ts).into()
}

fn is_position_arg(meta: &syn::Meta) -> bool {
    matches!(meta, syn::Meta::Path(p) if p.is_ident("common"))
}

pub fn derive_documented_metrics(input: TokenStream) -> TokenStream {
    match documented_metrics_impl(input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}

pub fn derive_documented_exec(input: TokenStream) -> TokenStream {
    match documented_exec_impl(input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}

fn documented_metrics_impl(input: TokenStream) -> syn::Result<TokenStream> {
    let input = syn::parse::<DeriveInput>(input)?;
    let ident = input.ident;

    let Data::Struct(data) = input.data else {
        return Err(syn::Error::new(
            ident.span(),
            "DocumentedMetrics can only be derived for structs",
        ));
    };

    let doc = doc_expr(&input.attrs);
    let position = match metrics_position(&input.attrs)? {
        MetricDocPosition::Common => {
            quote!(datafusion_doc::metric_doc_sections::MetricDocPosition::Common)
        }
        MetricDocPosition::Operator => {
            quote!(datafusion_doc::metric_doc_sections::MetricDocPosition::Operator)
        }
    };

    let mut fields_docs = vec![];
    for field in data.fields {
        let Some(field_ident) = field.ident else {
            return Err(syn::Error::new(
                field.span(),
                "DocumentedMetrics does not support tuple structs",
            ));
        };

        let field_type = &field.ty;
        let doc = doc_expr(&field.attrs);
        fields_docs.push(
            quote! { datafusion_doc::metric_doc_sections::MetricFieldDoc {
                name: stringify!(#field_ident),
                doc: #doc,
                type_name: stringify!(#field_type),
            } },
        );
    }

    let mod_ident = format_ident!("__datafusion_doc_metrics_{}", ident);

    Ok(quote! {
        #[allow(non_snake_case)]
        mod #mod_ident {
            use super::*;

            static DOCUMENTATION: datafusion_doc::metric_doc_sections::MetricDoc =
                datafusion_doc::metric_doc_sections::MetricDoc {
                    name: stringify!(#ident),
                    doc: #doc,
                    fields: &[#(#fields_docs),*],
                    position: #position,
                };

            impl datafusion_doc::metric_doc_sections::DocumentedMetrics for super::#ident {
                const DOC: &'static datafusion_doc::metric_doc_sections::MetricDoc =
                    &DOCUMENTATION;
            }

            datafusion_doc::metric_doc_sections::inventory::submit! {
                datafusion_doc::metric_doc_sections::MetricDocEntry(&DOCUMENTATION)
            }
        }
    }
    .into())
}

fn documented_exec_impl(input: TokenStream) -> syn::Result<TokenStream> {
    let input = syn::parse::<DeriveInput>(input)?;
    let ident = input.ident;

    let Data::Struct(_) = input.data else {
        return Err(syn::Error::new(
            ident.span(),
            "DocumentedExec can only be derived for structs",
        ));
    };

    let doc = doc_expr(&input.attrs);
    let metrics = metrics_list(&input.attrs)?;
    let metrics_refs = metrics.iter().map(|metric| {
        quote! { <#metric as datafusion_doc::metric_doc_sections::DocumentedMetrics>::DOC }
    });
    let mod_ident = format_ident!("__datafusion_doc_exec_{}", ident);

    Ok(quote! {
        #[allow(non_snake_case)]
        mod #mod_ident {
            use super::*;

            static DOCUMENTATION: datafusion_doc::metric_doc_sections::ExecDoc =
                datafusion_doc::metric_doc_sections::ExecDoc {
                    name: stringify!(#ident),
                    doc: #doc,
                    metrics: &[#(#metrics_refs),*],
                };

            impl datafusion_doc::metric_doc_sections::DocumentedExec for super::#ident {
                fn exec_doc(
                ) -> &'static datafusion_doc::metric_doc_sections::ExecDoc {
                    &DOCUMENTATION
                }
            }

            datafusion_doc::metric_doc_sections::inventory::submit! {
                datafusion_doc::metric_doc_sections::ExecDocEntry(&DOCUMENTATION)
            }
        }
    }
    .into())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum MetricDocPosition {
    Common,
    Operator,
}

fn metrics_position(attrs: &[Attribute]) -> syn::Result<MetricDocPosition> {
    let mut position = MetricDocPosition::Operator;
    for attr in attrs {
        if is_metrics_attr(attr) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("common") {
                    position = MetricDocPosition::Common;
                    return Ok(());
                }
                Err(meta.error("unsupported metric_doc attribute"))
            })?;
        }
    }
    Ok(position)
}

fn metrics_list(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut metrics = vec![];
    for attr in attrs {
        if is_metrics_attr(attr) {
            attr.parse_nested_meta(|meta| {
                metrics.push(meta.path);
                Ok(())
            })?;
        }
    }

    Ok(metrics)
}

fn doc_expr(attrs: &[Attribute]) -> proc_macro2::TokenStream {
    let mut doc_lines = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }

        if let syn::Meta::NameValue(meta) = &attr.meta
            && let Expr::Lit(ExprLit {
                lit: Lit::Str(lit_str),
                ..
            }) = &meta.value
        {
            doc_lines.push(lit_str.value());
        }
    }

    if doc_lines.is_empty() {
        quote!("")
    } else {
        let sanitized = doc_lines
            .into_iter()
            .map(|line| line.trim_end().to_string())
            .collect::<Vec<_>>();
        quote!(concat!(#(#sanitized, "\n"),*))
    }
}

fn is_metrics_attr(attr: &Attribute) -> bool {
    attr.path().is_ident("metric_doc") || attr.path().is_ident("metric_doc_attr")
}
