use crate::types::{ColumnType, Type};

use super::JsonType;

/// Ignore Type order and outo-wrap the returnvalue
macro_rules! either_way {
    (
        ($a:ident, $b:ident)
        {
            $(
                ($pattern_a:pat, $pattern_b:pat) => $arm:expr
            ),+
            $(,)?
        }
    ) => {
        match ($a, $b) {
            $(
                ($pattern_a, $pattern_b) | ($pattern_b, $pattern_a) => Some($arm.clone().into()),
            )*
            _ => None
        }

    };
}

pub fn unify(a: Type, b: Type) -> Option<Type> {
    match (a, b) {
        (a, b) if a == b => Some(a),
        (a, b) => {
            let a = a.col_type()?;
            let b = b.col_type()?;
            either_way! { (a, b) {
                (ColumnType::Inferred, a) => a,
                (ColumnType::Json(JsonType::Any, _), a @ ColumnType::Json(_, _)) => a,
                (
                    ColumnType::Json(JsonType::Any | JsonType::String, _),
                    a @ (ColumnType::Text | ColumnType::Char(_) | ColumnType::String(_))
                ) => a,
                (
                    ColumnType::Json(JsonType::Any | JsonType::Number, _),
                    a @ (
                        ColumnType::TinyInteger  | ColumnType::TinyUnsigned  |
                        ColumnType::SmallInteger | ColumnType::SmallUnsigned |
                        ColumnType::Integer      | ColumnType::Unsigned      |
                        ColumnType::BigInteger   | ColumnType::BigUnsigned
                    )
                ) => a,
                (
                    ColumnType::Json(JsonType::Any | JsonType::Boolean, _),
                    a @ ColumnType::Boolean
                ) => a,
            }}
        }
    }
}
