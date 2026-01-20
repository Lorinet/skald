use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Meta, Lit};

#[proc_macro_derive(Message, attributes(skald))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let mut topic = String::new();

    for attr in input.attrs {
        if attr.path().is_ident("skald") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("topic") {
                    let content = meta.value()?;
                    let s: Lit = content.parse()?;
                    if let Lit::Str(lit_str) = s {
                        topic = lit_str.value();
                    }
                }
                Ok(())
            });
        }
    }

    if topic.is_empty() {
        panic!("Message derive requires #[skald(topic = \"...\")] attribute");
    }

    let expanded = quote! {
        impl skald::messaging::Message for #name {
            fn topic(&self) -> String {
                #topic.to_string()
            }
        }
    };

    TokenStream::from(expanded)
}
