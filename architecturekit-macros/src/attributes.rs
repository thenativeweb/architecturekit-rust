use syn::{Attribute, Error, Lit, LitStr, Result, meta::ParseNestedMeta, spanned::Spanned, token};

use crate::rename::RenameRule;

/// What the `#[serde(...)]` attributes of a struct say about what serde writes.
#[derive(Default)]
pub(crate) struct ContainerAttributes {
    pub(crate) rename_all: RenameRule,
    pub(crate) is_transparent: bool,
    not_derivable: Option<Error>,
}

impl ContainerAttributes {
    pub(crate) fn of(attributes: &[Attribute]) -> Result<Self> {
        let mut container = Self::default();

        for attribute in attributes.iter().filter(|a| a.path().is_ident("serde")) {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename_all") {
                    if let Some(rule) = value_for_serializing(&meta)? {
                        container.rename_all = RenameRule::from_literal(&rule)?;
                    }
                } else if meta.path.is_ident("transparent") {
                    container.is_transparent = true;
                } else if let Some(name) = ["tag", "content", "into", "remote"]
                    .into_iter()
                    .find(|name| meta.path.is_ident(name))
                {
                    if container.not_derivable.is_none() {
                        container.not_derivable = Some(Error::new(
                            meta.path.span(),
                            format!(
                                "the schema of a struct with #[serde({name})] can not be derived, since serde \
                                 does not write its fields as they are; implement architecturekit::Schema by hand"
                            ),
                        ));
                    }
                    ignore(&meta)?;
                } else {
                    ignore(&meta)?;
                }
                Ok(())
            })?;
        }

        Ok(container)
    }

    pub(crate) fn refuse_what_can_not_be_derived(&self) -> Result<()> {
        self.not_derivable.clone().map_or(Ok(()), Err)
    }
}

/// What the `#[serde(...)]` attributes of a field say about how serde writes it.
#[derive(Default)]
pub(crate) struct FieldAttributes {
    pub(crate) rename: Option<String>,
    pub(crate) is_skipped: bool,
    pub(crate) is_optional: bool,
    not_derivable: Option<Error>,
}

impl FieldAttributes {
    pub(crate) fn of(attributes: &[Attribute]) -> Result<Self> {
        let mut field = Self::default();

        for attribute in attributes.iter().filter(|a| a.path().is_ident("serde")) {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    if let Some(name) = value_for_serializing(&meta)? {
                        field.rename = Some(name.value());
                    }
                } else if meta.path.is_ident("skip") || meta.path.is_ident("skip_serializing") {
                    field.is_skipped = true;
                } else if meta.path.is_ident("skip_serializing_if") {
                    // serde leaves the field out whenever the function says so,
                    // so the field is optional.
                    field.is_optional = true;
                    ignore(&meta)?;
                } else if let Some(name) = ["flatten", "serialize_with", "with"]
                    .into_iter()
                    .find(|name| meta.path.is_ident(name))
                {
                    if field.not_derivable.is_none() {
                        field.not_derivable = Some(Error::new(
                            meta.path.span(),
                            format!(
                                "the schema of a field with #[serde({name})] can not be derived, since serde \
                                 does not write it as its type describes; implement architecturekit::Schema by \
                                 hand, for example for a type that wraps the field"
                            ),
                        ));
                    }
                    ignore(&meta)?;
                } else {
                    ignore(&meta)?;
                }
                Ok(())
            })?;
        }

        Ok(field)
    }

    pub(crate) fn refuse_what_can_not_be_derived(&self) -> Result<()> {
        self.not_derivable.clone().map_or(Ok(()), Err)
    }
}

/// Reads the value of an attribute that may name what serde writes and what it
/// reads apart, as in `rename = "a"` or `rename(serialize = "a", deserialize = "b")`,
/// and returns the one for writing, if there is one.
fn value_for_serializing(meta: &ParseNestedMeta) -> Result<Option<LitStr>> {
    if meta.input.peek(syn::Token![=]) {
        return Ok(Some(meta.value()?.parse()?));
    }

    let mut serializing = None;
    meta.parse_nested_meta(|nested| {
        if nested.path.is_ident("serialize") {
            serializing = Some(nested.value()?.parse()?);
        } else {
            ignore(&nested)?;
        }
        Ok(())
    })?;
    Ok(serializing)
}

/// Consumes the value of an attribute that does not change what serde writes,
/// such as `default` or `alias`.
fn ignore(meta: &ParseNestedMeta) -> Result<()> {
    if meta.input.peek(syn::Token![=]) {
        let _: Lit = meta.value()?.parse()?;
    } else if meta.input.peek(token::Paren) {
        meta.parse_nested_meta(|nested| ignore(&nested))?;
    }
    Ok(())
}
