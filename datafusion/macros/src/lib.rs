extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Lit, LitStr, Meta, MetaNameValue};
use datafusion_pre_macros::DocumentationTest;

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
