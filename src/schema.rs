use std::{
    fmt,
    error,
    hash::{
        Hash,
        Hasher,
    },
    collections::{
        VecDeque,
        HashMap,
        hash_map,
    },
};

use serde::ser::{self, Error, Serializer, Serialize, Impossible};
use tantivy::schema::{
    Schema,
    SchemaBuilder,
    FAST,
    TEXT,
    STRING,
    STORED,
    Document,
};

use crate::index::IndexId;

/**
A document that can be indexed in tantivy.

The document is built by collecting and flattening the fields of some
serializable type. The document carries an index that is built of a
hash of its fields.
*/
pub struct Doc {
    index: IndexId,
    fields: Vec<(String, Value)>,
}

pub struct IndexableDoc {
    pub index: IndexId,
    pub schema: Schema,
    pub doc: Document,
}

impl Doc {
    pub fn build(doc: impl Serialize) -> Result<Self, crate::Error> {
        let mut ser = FieldCollector::new();
        doc.serialize(&mut ser)?;

        let hash = {
            let mut hasher = hash_map::DefaultHasher::new();

            for (k, v) in &ser.fields {
                (k, v.ty()).hash(&mut hasher);
            }

            hasher.finish()
        };

        Ok(Doc {
            index: hash,
            fields: ser.fields,
        })
    }

    pub fn index(&self) -> IndexId {
        self.index
    }

    pub fn indexable(&self) -> IndexableDoc {
        let schema = {
            let mut schema = SchemaBuilder::new();
            let mut seen = HashMap::new();

            for (k, v) in &self.fields {
                match seen.entry(k) {
                    hash_map::Entry::Occupied(entry) => {
                        // Ensure any duplicate entries have the same type
                        assert!(*entry.get() == v.ty());
                    },
                    hash_map::Entry::Vacant(entry) => {
                        // We only need to build each field once
                        entry.insert(v.ty());

                        match v {
                            Value::Signed(_) | Value::Unsigned(_) | Value::Float(_) => {
                                schema.add_i64_field(k, FAST);
                            },
                            Value::Bytes(_) => {
                                schema.add_bytes_field(k);
                            },
                            Value::Bool(_) => {
                                schema.add_text_field(k, STRING | STORED);
                            },
                            Value::Str(_) => {
                                schema.add_text_field(k, TEXT | STORED);
                            },
                            Value::None => (),
                        }
                    }
                }
            }

            schema.build()
        };

        let doc = {
            let mut doc = Document::new();

            for (k, v) in &self.fields {
                match v {
                    Value::Signed(v) => {
                        doc.add_i64(schema.get_field(k).expect("missing field"), *v);
                    },
                    Value::Unsigned(v) => {
                        doc.add_u64(schema.get_field(k).expect("missing field"), *v);
                    },
                    Value::Float(v) => {
                        doc.add_u64(schema.get_field(k).expect("missing field"), v.to_bits());
                    }
                    Value::Bytes(v) => {
                        doc.add_bytes(schema.get_field(k).expect("missing field"), v.to_owned());
                    },
                    Value::Bool(v) => {
                        let v = if *v { "true" } else { "false" };

                        doc.add_text(schema.get_field(k).expect("missing field"), v);
                    },
                    Value::Str(v) => {
                        doc.add_text(schema.get_field(k).expect("missing field"), v);
                    },
                    Value::None => (),
                }
            }

            doc
        };

        IndexableDoc {
            index: self.index,
            schema,
            doc,
        }
    }
}

/**
An implementation of `serde::Serializer` that collects and flattens fields.
*/
struct FieldCollector {
    path: FieldPath,
    current_field: Option<String>,
    fields: Vec<(String, Value)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Bytes(Vec<u8>),
    Str(String),
    Bool(bool),
    None,
}

impl Value {
    fn ty(&self) -> &'static str {
        match *self {
            Value::Signed(_) => "signed",
            Value::Unsigned(_) => "unsigned",
            Value::Float(_) => "float",
            Value::Bytes(_) => "bytes",
            Value::Str(_) => "string",
            Value::Bool(_) => "bool",
            Value::None => "none",
        }
    }
}

struct FieldPath {
    anonymous: u32,
    components: VecDeque<FieldComponent>,
}

struct FieldComponent {
    anonymous: u32,
    allow_child_fields: bool,
    value: String,
}

impl FieldPath {
    fn push(&mut self, allow_child_fields: bool, field: impl Into<String>) {
        self.components.push_back(FieldComponent {
            anonymous: 0,
            allow_child_fields,
            value: field.into(),
        })
    }

    fn pop(&mut self)  {
        self.components.pop_back();
    }

    fn current(&self) -> String {
        self.internal_current_to(None)
    }

    fn current_to(&self, field: impl AsRef<str>) -> String {
        self.internal_current_to(Some(field.as_ref()))
    }

    fn internal_current_to(&self, field: Option<&str>) -> String {
        self.components
            .iter()
            .map(|s| &s.value as &str)
            .chain(field)
            .fold(String::new(), |mut s, p| {
                if s.len() > 0 {
                    s.push('.');
                }

                s.push_str(p);

                s
            })
    }

    fn anonymous(&mut self) -> String {
        let mut back = self.components.back_mut();
        let anonymous = if let Some(ref mut component) = back {
            assert!(component.allow_child_fields);

            &mut component.anonymous
        } else {
            &mut self.anonymous
        };

        let field = format!("_{}", anonymous);
        *anonymous += 1;

        field
    }
}

impl FieldCollector {
    fn new() -> Self {
        FieldCollector {
            path: FieldPath {
                anonymous: 0,
                components: VecDeque::new(),
            },
            current_field: None,
            fields: Vec::new(),
        }
    }

    fn set_current_field(&mut self, field: String) {
        assert!(self.current_field.is_none());

        self.current_field = Some(field);
    }

    fn push_path(&mut self) {
        if let Some(field) = self.current_field.take() {
            self.path.push(true, field);
        }
    }

    fn push_path_no_child_fields(&mut self) {
        if let Some(field) = self.current_field.take() {
            self.path.push(false, field);
        }
    }

    fn pop_path(&mut self) {
        self.path.pop();
    }

    fn move_next_field(&mut self, value: Value) {
        let field = match self.path.components.back_mut() {
            Some(ref component) if !component.allow_child_fields => {
                assert!(self.current_field.is_none());

                self.path.current()
            },
            _ => {
                let field = self.current_field.take().unwrap_or_else(|| self.path.anonymous());
                self.path.current_to(field)
            }
        };

        self.fields.push((field, value));
    }
}

impl<'a> Serializer for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<(), Invalid> {
        self.move_next_field(Value::Bool(v));

        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<(), Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<(), Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<(), Invalid> {
        self.move_next_field(Value::Signed(v));

        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<(), Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<(), Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<(), Invalid> {
        self.move_next_field(Value::Unsigned(v));

        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), Invalid> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<(), Invalid> {
        self.move_next_field(Value::Float(v));
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), Invalid> {
        self.move_next_field(Value::Str(v.to_string()));

        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<(), Invalid> {
        self.move_next_field(Value::Str(v.to_owned()));

        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Invalid> {
        self.move_next_field(Value::Bytes(v.to_owned()));

        Ok(())
    }

    fn serialize_none(self) -> Result<(), Invalid> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Invalid> {
        self.move_next_field(Value::None);
        
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Invalid> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<(), Invalid> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!();
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Invalid> {
        self.push_path_no_child_fields();

        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Invalid> {
        self.push_path();

        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Invalid> {
        self.push_path();

        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Invalid> {
        unimplemented!();
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Invalid> {
        self.push_path();

        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Invalid> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Invalid> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeSeq for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();

        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();

        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();

        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<(), Invalid> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeMap for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        let key = key.serialize(KeyCollector).expect("invalid key");
        self.set_current_field(key);
        
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;

        Ok(())
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();

        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        self.set_current_field(key.to_string());
        value.serialize(&mut **self)?;

        Ok(())
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut FieldCollector {
    type Ok = ();
    type Error = Invalid;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Invalid>
    where
        T: ?Sized + Serialize,
    {
        self.set_current_field(key.to_string());
        value.serialize(&mut **self)?;

        Ok(())
    }

    fn end(self) -> Result<(), Invalid> {
        self.pop_path();
        Ok(())
    }
}

struct KeyCollector;

impl Serializer for KeyCollector {
    type Ok = String;
    type Error = Invalid;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<String, Invalid> {
        Ok(v.to_string())
    }

    fn serialize_i8(self, v: i8) -> Result<String, Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<String, Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<String, Invalid> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<String, Invalid> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<String, Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<String, Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<String, Invalid> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<String, Invalid> {
        Ok(v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<String, Invalid> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<String, Invalid> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<String, Invalid> {
        Ok(v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<String, Invalid> {
        Ok(v.to_owned())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<String, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_none(self) -> Result<String, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_some<T>(self, value: &T) -> Result<String, Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<String, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<String, Invalid> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<String, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String, Invalid>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<String, Invalid>
    where
        T: ?Sized + Serialize,
    {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Invalid> {
        Err(Invalid::custom("unsupported key type"))
    }
}

#[derive(Debug)]
struct Invalid(String);

impl Error for Invalid {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display
    {
        Invalid(msg.to_string())
    }
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl error::Error for Invalid {
    fn cause(&self) -> Option<&error::Error> {
        None
    }

    fn description(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use serde_derive::Serialize;
    use serde_json::json;

    use super::*;

    #[derive(Serialize)]
    struct Record {
        a: i32,
        b: String,
        c: Inner,
        d: Vec<i32>,
    }

    #[derive(Serialize)]
    struct Inner {
        a: bool,
        b: (char, char),
    }

    #[test]
    fn get_doc_fields() {
        let record = Record {
            a: 1,
            b: "Hello!".to_owned(),
            c: Inner {
                a: false,
                b: ('a', 'b'),
            },
            d: vec![13, 42],
        };

        let expected = vec![
            ("a".to_owned(), Value::Signed(1)),
            ("b".to_owned(), Value::Str("Hello!".into())),
            ("c.a".to_owned(), Value::Bool(false)),
            ("c.b._0".to_owned(), Value::Str("a".into())),
            ("c.b._1".to_owned(), Value::Str("b".into())),
            ("d".to_owned(), Value::Signed(13)),
            ("d".to_owned(), Value::Signed(42)),
        ];

        let doc = Doc::build(record).expect("failed to get document");

        assert_eq!(expected, doc.fields);
    }

    #[test]
    fn docs_with_equivalent_fields_have_same_index() {
        let a = Doc::build(json!({
            "a": 1,
            "b": "Some text"
        })).expect("failed to get document");

        let b = Doc::build(json!({
            "a": 2,
            "b": "Some other text"
        })).expect("failed to get document");

        assert_eq!(a.index(), b.index());
    }

    #[test]
    fn docs_with_different_fields_have_different_index() {
        let a = Doc::build(json!({
            "a": 1,
            "b": "Some text"
        })).expect("failed to get document");

        let b = Doc::build(json!({
            "c": 2,
            "d": "Some other text"
        })).expect("failed to get document");

        assert_ne!(a.index(), b.index());
    }
}