//! `#[derive(Task)]` — Phase 3 item 12. Generates the [`headgate::Task`] impl with the
//! default JSON codec (payload codecs: the payload codec is per task type, JSON by default).
//!
//! ```ignore
//! #[derive(Task, serde::Serialize, serde::Deserialize)]
//! #[task(kind = "email:welcome", version = 2, aliases("email:send"))]
//! struct WelcomeEmail { to: String }
//! ```
//!
//! `kind` is required and is WIRE STATE — renaming it strands enqueued jobs unless the
//! old kind is kept in `aliases` (typed dispatch). `version` defaults to 1. `upcast` keeps its
//! default (reject unknown versions → `undecodable`); implement it by hand when a v2
//! ships. The type must implement serde's `Serialize` and `Deserialize`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, LitStr, parse_macro_input};

#[proc_macro_derive(Task, attributes(task))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let mut kind: Option<LitStr> = None;
    let mut version: Option<LitInt> = None;
    let mut aliases: Vec<LitStr> = Vec::new();

    for attr in &input.attrs {
        if !attr.path().is_ident("task") {
            continue;
        }
        let res = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                kind = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("version") {
                version = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("aliases") {
                // aliases("a", "b")
                let content;
                syn::parenthesized!(content in meta.input);
                let parsed = content.parse_terminated(|p| p.parse::<LitStr>(), syn::Token![,])?;
                aliases.extend(parsed);
                Ok(())
            } else {
                Err(meta.error("expected `kind = \"...\"`, `version = N`, or `aliases(\"...\")`"))
            }
        });
        if let Err(e) = res {
            return e.to_compile_error().into();
        }
    }

    let Some(kind) = kind else {
        return syn::Error::new_spanned(
            &input.ident,
            "#[derive(Task)] requires #[task(kind = \"...\")] — the kind is the dispatch \
             key and wire state",
        )
        .to_compile_error()
        .into();
    };
    if kind.value().is_empty() {
        return syn::Error::new_spanned(kind, "task kind must not be empty")
            .to_compile_error()
            .into();
    }
    let version = version.unwrap_or_else(|| LitInt::new("1", proc_macro2::Span::call_site()));

    let expanded = quote! {
        impl ::headgate::Task for #name {
            const TYPE: &'static str = #kind;
            const VERSION: u32 = #version;
            const ALIASES: &'static [&'static str] = &[#(#aliases),*];

            fn encode(&self) -> ::core::result::Result<::std::vec::Vec<u8>, ::headgate::CodecError> {
                ::headgate::__private::serde_json::to_vec(self)
                    .map_err(|e| ::headgate::CodecError::Malformed(e.to_string()))
            }

            fn decode(bytes: &[u8]) -> ::core::result::Result<Self, ::headgate::CodecError> {
                ::headgate::__private::serde_json::from_slice(bytes)
                    .map_err(|e| ::headgate::CodecError::Malformed(e.to_string()))
            }
        }
    };
    expanded.into()
}
