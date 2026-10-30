//! Hands a struct its fields by name only. serde decodes a struct from a list
//! as well, with one value for every field, in the order of the fields, so a
//! caller who put the values in another order would set the wrong fields
//! without a word. The deserializer here refuses a list wherever a struct, or
//! a variant of an enum with named fields, is asked for, at any depth, and
//! hands everything else on as it is.
//!
//! serde decodes some values from a buffer of its own, which it reads ahead:
//! an enum with `#[serde(untagged)]` or `#[serde(tag = "...")]`, and a field
//! with `#[serde(flatten)]`. No deserializer reaches into that buffer, so a
//! list for a struct within such a value is taken by position, just as a
//! field there that the type does not know is not told.

use std::fmt;

use serde::de::{
    self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, Unexpected, VariantAccess, Visitor,
};

/// Wraps a deserializer, or anything that hands values on to be decoded, such
/// as the items of a list or the members of an object, so that whatever it
/// hands on is wrapped in turn, down to the last value.
pub(super) struct ByName<T>(pub(super) T);

/// Wraps a visitor, so that what it is handed to decode a value from is
/// wrapped in turn. A visitor for a struct refuses a list.
struct Visit<V> {
    visitor: V,
    is_struct: bool,
}

impl<V> Visit<V> {
    /// Wraps a visitor for any value but a struct.
    fn new(visitor: V) -> Self {
        Self {
            visitor,
            is_struct: false,
        }
    }

    /// Wraps a visitor for a struct, or for a variant with named fields.
    fn of_struct(visitor: V) -> Self {
        Self {
            visitor,
            is_struct: true,
        }
    }
}

/// Implements the methods of a deserializer that only hand on what they are
/// given, with the visitor wrapped.
macro_rules! hand_on_to_the_deserializer {
    ($($method:ident($($argument:ident: $type:ty),*);)*) => {$(
        fn $method<V: Visitor<'de>>(
            self,
            $($argument: $type,)*
            visitor: V,
        ) -> Result<V::Value, D::Error> {
            self.0.$method($($argument,)* Visit::new(visitor))
        }
    )*};
}

impl<'de, D: de::Deserializer<'de>> de::Deserializer<'de> for ByName<D> {
    type Error = D::Error;

    hand_on_to_the_deserializer! {
        deserialize_any();
        deserialize_bool();
        deserialize_i8();
        deserialize_i16();
        deserialize_i32();
        deserialize_i64();
        deserialize_i128();
        deserialize_u8();
        deserialize_u16();
        deserialize_u32();
        deserialize_u64();
        deserialize_u128();
        deserialize_f32();
        deserialize_f64();
        deserialize_char();
        deserialize_str();
        deserialize_string();
        deserialize_bytes();
        deserialize_byte_buf();
        deserialize_option();
        deserialize_unit();
        deserialize_unit_struct(name: &'static str);
        deserialize_newtype_struct(name: &'static str);
        deserialize_seq();
        deserialize_tuple(len: usize);
        deserialize_tuple_struct(name: &'static str, len: usize);
        deserialize_map();
        deserialize_enum(name: &'static str, variants: &'static [&'static str]);
        deserialize_identifier();
        deserialize_ignored_any();
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0
            .deserialize_struct(name, fields, Visit::of_struct(visitor))
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

/// Implements the methods of a visitor that are given a value as it is,
/// rather than something to decode it from, and only hand it on.
macro_rules! hand_on_to_the_visitor {
    ($($method:ident($($value:ident: $type:ty)?);)*) => {$(
        fn $method<E: de::Error>(self, $($value: $type)?) -> Result<V::Value, E> {
            self.visitor.$method($($value)?)
        }
    )*};
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Visit<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.visitor.expecting(formatter)
    }

    hand_on_to_the_visitor! {
        visit_bool(value: bool);
        visit_i8(value: i8);
        visit_i16(value: i16);
        visit_i32(value: i32);
        visit_i64(value: i64);
        visit_i128(value: i128);
        visit_u8(value: u8);
        visit_u16(value: u16);
        visit_u32(value: u32);
        visit_u64(value: u64);
        visit_u128(value: u128);
        visit_f32(value: f32);
        visit_f64(value: f64);
        visit_char(value: char);
        visit_str(value: &str);
        visit_borrowed_str(value: &'de str);
        visit_string(value: String);
        visit_bytes(value: &[u8]);
        visit_borrowed_bytes(value: &'de [u8]);
        visit_byte_buf(value: Vec<u8>);
        visit_none();
        visit_unit();
    }

    fn visit_some<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<V::Value, D::Error> {
        self.visitor.visit_some(ByName(deserializer))
    }

    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<V::Value, D::Error> {
        self.visitor.visit_newtype_struct(ByName(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, items: A) -> Result<V::Value, A::Error> {
        // The visitor of a struct would take the items for its fields, in
        // their order, as long as there are as many items as fields. It is
        // refused in the words serde uses for a value of the wrong kind, which
        // name what was expected, so that the text for it is the same as for
        // any other value where an object belongs.
        if self.is_struct {
            return Err(de::Error::invalid_type(Unexpected::Seq, &self.visitor));
        }

        self.visitor.visit_seq(ByName(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, members: A) -> Result<V::Value, A::Error> {
        self.visitor.visit_map(ByName(members))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        self.visitor.visit_enum(ByName(data))
    }
}

/// Hands a seed the deserializer to decode its value from, wrapped.
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for ByName<S> {
    type Value = S::Value;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<S::Value, D::Error> {
        self.0.deserialize(ByName(deserializer))
    }
}

/// Hands every item of a list on, with its seed wrapped.
impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for ByName<A> {
    type Error = A::Error;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, A::Error> {
        self.0.next_element_seed(ByName(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

/// Hands every name and value of an object on, with their seeds wrapped.
impl<'de, A: MapAccess<'de>> MapAccess<'de> for ByName<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        self.0.next_key_seed(ByName(seed))
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(&mut self, seed: S) -> Result<S::Value, A::Error> {
        self.0.next_value_seed(ByName(seed))
    }

    fn next_entry_seed<K: DeserializeSeed<'de>, S: DeserializeSeed<'de>>(
        &mut self,
        key: K,
        value: S,
    ) -> Result<Option<(K::Value, S::Value)>, A::Error> {
        self.0.next_entry_seed(ByName(key), ByName(value))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

/// Hands the variant of an enum on, wrapped, so that what it holds is wrapped
/// as well.
impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for ByName<A> {
    type Error = A::Error;
    type Variant = ByName<A::Variant>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, ByName<A::Variant>), A::Error> {
        let (value, variant) = self.0.variant_seed(ByName(seed))?;

        Ok((value, ByName(variant)))
    }
}

/// Hands what a variant holds on, and refuses a list for a variant with named
/// fields, just as for a struct.
impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for ByName<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.0.unit_variant()
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(self, seed: S) -> Result<S::Value, A::Error> {
        self.0.newtype_variant_seed(ByName(seed))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        self.0.tuple_variant(len, Visit::new(visitor))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        self.0.struct_variant(fields, Visit::of_struct(visitor))
    }
}
