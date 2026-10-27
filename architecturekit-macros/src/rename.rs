use syn::{Error, LitStr, Result};

/// A rule of `#[serde(rename_all = "...")]`, applied to the names of fields the
/// same way serde applies it, since the schema has to name what serde writes.
#[derive(Clone, Copy, Default)]
pub(crate) enum RenameRule {
    #[default]
    None,
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    pub(crate) fn from_literal(literal: &LitStr) -> Result<Self> {
        match literal.value().as_str() {
            "lowercase" => Ok(Self::LowerCase),
            "UPPERCASE" => Ok(Self::UpperCase),
            "PascalCase" => Ok(Self::PascalCase),
            "camelCase" => Ok(Self::CamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnakeCase),
            "kebab-case" => Ok(Self::KebabCase),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ScreamingKebabCase),
            other => Err(Error::new(
                literal.span(),
                format!("unknown rename rule `{other}`"),
            )),
        }
    }

    pub(crate) fn apply_to_field(self, field: &str) -> String {
        match self {
            Self::None | Self::LowerCase | Self::SnakeCase => field.to_owned(),
            Self::UpperCase | Self::ScreamingSnakeCase => field.to_ascii_uppercase(),
            Self::PascalCase => {
                let mut pascal = String::new();
                let mut capitalize = true;
                for character in field.chars() {
                    if character == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(character.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(character);
                    }
                }
                pascal
            }
            Self::CamelCase => {
                let pascal = Self::PascalCase.apply_to_field(field);
                let mut characters = pascal.chars();
                characters.next().map_or_else(String::new, |first| {
                    first.to_ascii_lowercase().to_string() + characters.as_str()
                })
            }
            Self::KebabCase => field.replace('_', "-"),
            Self::ScreamingKebabCase => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
}
