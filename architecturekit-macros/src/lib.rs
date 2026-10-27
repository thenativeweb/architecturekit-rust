//! The derive macros of architecturekit.
//!
//! Do not depend on this crate directly. Use the derives through architecturekit,
//! which re-exports them, and whose documentation describes them.
#![deny(
    ambiguous_negative_literals,
    clippy::pedantic,
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    unsafe_code,
    warnings
)]

mod attributes;
mod events;
mod rename;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Fields, GenericParam, LitStr, Result, ext::IdentExt,
    parse_macro_input, parse_quote, spanned::Spanned,
};

use crate::attributes::{ContainerAttributes, FieldAttributes};

/// Derives `architecturekit::Event` and `architecturekit::Schema` for the data
/// of an event. See `architecturekit::Event`.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_event(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Derives `architecturekit::Events` for the enum of the events a state evolves
/// by. See `architecturekit::Events`.
#[proc_macro_derive(Events)]
pub fn derive_events(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    events::expand_events(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Derives `architecturekit::Schema` for a type in the data of an event. See
/// `architecturekit::Schema`.
#[proc_macro_derive(Schema)]
pub fn derive_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_schema(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_event(input: &DeriveInput) -> Result<TokenStream2> {
    // One event type names one shape of data. A generic event would give
    // several shapes the same event type, and so the same schema.
    if let Some(param) = input.generics.params.first() {
        return Err(Error::new(
            param.span(),
            "an event can not be generic, since its event type names exactly one shape of data",
        ));
    }

    let event_type = event_type_of(input)?;
    let schema = expand_schema(input)?;
    let name = &input.ident;

    Ok(quote! {
        #schema

        impl ::architecturekit::Event for #name {
            const EVENT_TYPE: &'static str = #event_type;
        }
    })
}

fn event_type_of(input: &DeriveInput) -> Result<LitStr> {
    let mut event_type: Option<LitStr> = None;

    for attribute in input.attrs.iter().filter(|a| a.path().is_ident("event")) {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("type") {
                return Err(meta.error("expected `type = \"...\"`"));
            }
            let value: LitStr = meta.value()?.parse()?;
            if value.value().is_empty() {
                return Err(Error::new(value.span(), "the event type must not be empty"));
            }
            if event_type.replace(value).is_some() {
                return Err(meta.error("the event type is given more than once"));
            }
            Ok(())
        })?;
    }

    event_type.ok_or_else(|| {
        Error::new(
            input.ident.span(),
            "an event needs its event type, as in #[event(type = \"io.eventsourcingdb.library.book-acquired\")]",
        )
    })
}

fn expand_schema(input: &DeriveInput) -> Result<TokenStream2> {
    let container = ContainerAttributes::of(&input.attrs)?;
    let body = match &input.data {
        Data::Struct(data) => schema_of_struct(&container, &data.fields, input)?,
        Data::Enum(data) => {
            return Err(Error::new(
                data.enum_token.span,
                "the schema of an enum can not be derived, since serde writes enums in several shapes; \
                 implement architecturekit::Schema by hand",
            ));
        }
        Data::Union(data) => {
            return Err(Error::new(
                data.union_token.span,
                "the schema of a union can not be derived; implement architecturekit::Schema by hand",
            ));
        }
    };

    let mut generics = input.generics.clone();
    for param in &mut generics.params {
        if let GenericParam::Type(param) = param {
            param.bounds.push(parse_quote!(::architecturekit::Schema));
        }
    }
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let name = &input.ident;

    Ok(quote! {
        impl #impl_generics ::architecturekit::Schema for #name #type_generics #where_clause {
            fn schema() -> ::architecturekit::__private::Value {
                ::architecturekit::__private::derive(::std::any::type_name::<Self>(), || #body)
            }
        }
    })
}

fn schema_of_struct(
    container: &ContainerAttributes,
    fields: &Fields,
    input: &DeriveInput,
) -> Result<TokenStream2> {
    container.refuse_what_can_not_be_derived()?;

    match fields {
        Fields::Named(named) => {
            let mut written = Vec::new();
            for field in &named.named {
                let attributes = FieldAttributes::of(&field.attrs)?;
                if attributes.is_skipped {
                    continue;
                }
                attributes.refuse_what_can_not_be_derived()?;
                written.push((field, attributes));
            }

            if container.is_transparent {
                return match written.as_slice() {
                    [(field, _)] => {
                        let field_type = &field.ty;
                        Ok(quote! { <#field_type as ::architecturekit::Schema>::schema() })
                    }
                    _ => Err(Error::new(
                        input.ident.span(),
                        "a transparent struct needs exactly one field that is not skipped",
                    )),
                };
            }

            let entries = written.iter().map(|(field, attributes)| {
                let name = attributes.rename.clone().unwrap_or_else(|| {
                    let ident = field.ident.as_ref().map(IdentExt::unraw).map(|i| i.to_string());
                    container.rename_all.apply_to_field(&ident.unwrap_or_default())
                });
                let field_type = &field.ty;
                if attributes.is_optional {
                    quote! { (#name, <#field_type as ::architecturekit::Schema>::schema_without_null(), true) }
                } else {
                    quote! { (#name, <#field_type as ::architecturekit::Schema>::schema(), false) }
                }
            });

            Ok(quote! {
                ::architecturekit::__private::object(::std::any::type_name::<Self>(), ::std::vec![#(#entries),*])
            })
        }
        Fields::Unnamed(unnamed) => match unnamed.unnamed.first() {
            Some(field) if unnamed.unnamed.len() == 1 => {
                // serde writes a newtype struct as the value it wraps.
                let field_type = &field.ty;
                Ok(quote! { <#field_type as ::architecturekit::Schema>::schema() })
            }
            _ => Err(Error::new(
                unnamed.span(),
                "the schema of a tuple struct with several fields can not be derived, since serde writes it \
                 as an array of mixed values; use named fields, or implement architecturekit::Schema by hand",
            )),
        },
        Fields::Unit => Err(Error::new(
            input.ident.span(),
            "the schema of a unit struct can not be derived, since serde writes it as null; \
             to describe data without fields, use `struct Name {}`",
        )),
    }
}
