use std::{fmt::Display, num::NonZero};

use crate::{
    intermediate::Rc,
    types::{Cell, ColumnType, Type},
};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Default)]
pub enum JsonType {
    #[default]
    Any,
    List(Rc<Self>),
    Map(JsonObject),
    Number,
    Boolean,
    String,
    Null,
}

impl Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Any => "dyn",
            Self::List(_) => "array",
            Self::Map(_) => "object",
            Self::Number => "number",
            Self::Boolean => "bool",
            Self::String => "string",
            Self::Null => "null",
        })
    }
}

impl JsonType {
    #[must_use]
    pub const fn can_cast_to(&self, other: &ColumnType) -> bool {
        #[allow(
            clippy::match_same_arms,
            clippy::match_like_matches_macro,
            reason = "list out each type"
        )]
        match (self, other) {
            (_, ColumnType::Json(Self::Any, _)) => true,
            (Self::Any, x) => x.is_number(),
            (Self::Number, x) => x.is_number(),
            (Self::Boolean, ColumnType::Boolean) => true,
            _ => false,
        }
    }

    #[must_use]
    pub const fn can_convert_to(&self, other: &ColumnType) -> bool {
        match (self, other) {
            (Self::Any, _) => true,
            (Self::String, x) => x.is_string(),
            _ => self.can_cast_to(other),
        }
    }
}

impl From<JsonType> for ColumnType {
    fn from(value: JsonType) -> Self {
        Self::Json(value, false)
    }
}

impl From<JsonType> for Type {
    fn from(value: JsonType) -> Self {
        ColumnType::Json(value, false).into()
    }
}

impl From<JsonType> for Cell {
    fn from(value: JsonType) -> Self {
        Self::Value(ColumnType::Json(value, false))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum JsonObject {
    AnyContent,
    /// May have more fields than are known
    /// Some fields may have dynamically constructed
    /// names
    PartialKnownFields {
        /// Known fields, a `None` name indicatees that it is
        /// dynamically constructed
        fields: Vec<(Option<String>, JsonType)>,
        /// Number of unknown fields.
        ///
        /// `None` indicates that the number is not known
        ///
        /// Can not be zero (that would be [`Self::FullyKnownFields`])
        unknown_fields: Option<NonZero<usize>>,
    },
    /// All fields are known but some may have dynamically
    /// constructed names
    FullyKnownFields {
        /// Known fields, a `None` name indicatees that it is
        /// dynamically constructed
        fields: Vec<(Option<String>, JsonType)>,
    },
    /// All fields are known and have static names
    Proto {
        /// A proto message that this object conforms to
        ///
        /// This means all required fields are present in the
        /// object and there are no fields in the object that
        /// are neither required nor optional
        proto_name: Option<String>,
        fields: Vec<(String, JsonType)>,
    },
}

impl From<JsonObject> for ColumnType {
    fn from(value: JsonObject) -> Self {
        Self::Json(JsonType::Map(value), false)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{JsonType, Type};

    #[test]
    fn iter_types() {
        assert_eq!(
            Type::from(JsonType::List(JsonType::String.into())).iter_type(),
            Some(JsonType::String.into())
        );
    }
}
