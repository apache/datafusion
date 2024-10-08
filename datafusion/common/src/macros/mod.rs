extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, ItemFn, Lit, Meta};
#[proc_macro_attribute]
pub fn generate_fn_with_params(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let args = parse_macro_input!(attr as AttributeArgs);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Extract parameters from the attribute
    let mut param1 = String::new();
    let mut param2 = String::new();

    if let Some(syn::NestedMeta::Meta(Meta::NameValue(meta))) = args.get(0) {
        if let Lit::Str(lit_str) = &meta.lit {
            param1 = lit_str.value();
        }
    }

    if let Some(syn::NestedMeta::Meta(Meta::NameValue(meta))) = args.get(1) {
        if let Lit::Str(lit_str) = &meta.lit {
            param2 = lit_str.value();
        }
    }

    // Get the original function's name
    let fn_name = &input_fn.sig.ident;

    // Generate the new function that prints the two parameters
    let expanded = quote! {
        #input_fn

        pub fn #fn_name() {
            println!("Param1: {}", #param1);
            println!("Param2: {}", #param2);
        }
    };

    // Return the generated code as a TokenStream
    TokenStream::from(expanded)
}