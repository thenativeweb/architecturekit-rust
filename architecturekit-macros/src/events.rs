use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Result, Type, spanned::Spanned};

pub(crate) fn expand_events(input: &DeriveInput) -> Result<TokenStream2> {
    if let Some(param) = input.generics.params.first() {
        return Err(Error::new(
            param.span(),
            "an enum of events can not be generic, since its variants name the event types of a state",
        ));
    }

    let Data::Enum(data) = &input.data else {
        return Err(Error::new(
            input.ident.span(),
            "Events can only be derived for an enum with one variant per event type, \
             as in enum BookEvent { Acquired(BookAcquired) }",
        ));
    };
    if data.variants.is_empty() {
        return Err(Error::new(
            input.ident.span(),
            "an enum of events needs at least one variant",
        ));
    }

    let mut variants: Vec<(&syn::Ident, &Type)> = Vec::new();
    for variant in &data.variants {
        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                variants.push((&variant.ident, &fields.unnamed[0].ty));
            }
            _ => {
                return Err(Error::new(
                    variant.span(),
                    "every variant of an enum of events holds exactly one event, as in Acquired(BookAcquired)",
                ));
            }
        }
    }

    let name = &input.ident;
    let event_types = variants
        .iter()
        .map(|(_, event)| quote! { <#event as ::architecturekit::Event>::EVENT_TYPE });
    let schemas = variants
        .iter()
        .map(|(_, event)| quote! { ::architecturekit::EventSchema::of::<#event>() });
    let event_type_arms = variants.iter().map(|(variant, event)| {
        quote! { Self::#variant(_) => <#event as ::architecturekit::Event>::EVENT_TYPE }
    });
    let to_data_arms = variants.iter().map(|(variant, _)| {
        quote! { Self::#variant(event) => ::architecturekit::__private::to_data(event) }
    });
    let from_data_checks = variants.iter().map(|(variant, event)| {
        quote! {
            if event_type == <#event as ::architecturekit::Event>::EVENT_TYPE {
                return ::std::option::Option::Some(
                    ::architecturekit::__private::from_data::<#event>(data).map(Self::#variant),
                );
            }
        }
    });

    // Two variants of the same event type would make reading ambiguous, so this
    // refuses them while compiling.
    let message = format!("two variants of {name} hold events of the same event type");
    let mut duplicate_checks = Vec::new();
    for (index, (_, left)) in variants.iter().enumerate() {
        for (_, right) in &variants[index + 1..] {
            duplicate_checks.push(quote! {
                ::std::assert!(
                    !::architecturekit::__private::same_str(
                        <#left as ::architecturekit::Event>::EVENT_TYPE,
                        <#right as ::architecturekit::Event>::EVENT_TYPE,
                    ),
                    #message,
                );
            });
        }
    }

    Ok(quote! {
        impl ::architecturekit::Events for #name {
            const EVENT_TYPES: &'static [&'static str] = &[#(#event_types),*];

            fn schemas() -> ::std::vec::Vec<::architecturekit::EventSchema> {
                ::std::vec![#(#schemas),*]
            }

            fn event_type(&self) -> &'static str {
                match self {
                    #(#event_type_arms),*
                }
            }

            fn to_data(
                &self,
            ) -> ::std::result::Result<::architecturekit::__private::Value, ::architecturekit::__private::JsonError> {
                match self {
                    #(#to_data_arms),*
                }
            }

            fn from_data(
                event_type: &str,
                data: ::architecturekit::__private::Value,
            ) -> ::std::option::Option<::std::result::Result<Self, ::architecturekit::__private::JsonError>> {
                #(#from_data_checks)*
                ::std::option::Option::None
            }
        }

        const _: () = {
            #(#duplicate_checks)*
        };
    })
}
