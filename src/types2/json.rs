use std::num::NonZero;

use crate::types2::{Cell, ColumnType, Type};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum JsonType {
    Any,
    List(Box<Self>),
    Map(JsonObject),
    Number,
    Boolean,
    String,
    Null,
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
            (Self::Any | Self::Number, ColumnType::Integer) => true,
            _ => false,
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
        fields: Vec<(Option<String>, Self)>,
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
        fields: Vec<(Option<String>, Self)>,
    },
    /// All fields are known and have static names
    OnlyLiteralFields {
        /// A proto message that this object conforms to
        ///
        /// This means all required fields are present in the
        /// object and there are no fields in the object that
        /// are neither required nor optional
        proto_name: Option<String>,
        fields: Vec<(String, Self)>,
    },
}

impl From<JsonObject> for ColumnType {
    fn from(value: JsonObject) -> Self {
        Self::Json(JsonType::Map(value), false)
    }
}
