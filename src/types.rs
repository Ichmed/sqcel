use std::{collections::HashMap, result::Result, str::FromStr};

use sea_query::{BinOper, Func, SimpleExpr, extension::postgres::PgBinOper};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{structure::Column, transpiler::alias};

pub trait TypeConversion {
    fn try_convert(&self, expr: TypedExpression) -> Result<TypedExpression, ConversionError>;

    /// Assume `UnsafeConversions` are succesfull
    fn force_convert(&self, expr: TypedExpression) -> Result<TypedExpression, ConversionError> {
        match self.try_convert(expr) {
            Err(ConversionError::UnsafeConversion(_, x)) => Ok(x),
            x => x,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedExpression {
    pub expr: SimpleExpr,
    pub ty: Type,
}

impl TypedExpression {
    #[must_use]
    pub fn assume_is(&self, ty: Type) -> Self {
        Self { ty, ..self.clone() }
    }
}

#[derive(Debug, Clone, Error)]
pub enum ConversionError {
    #[error("Unsafe Conversion from {:?} to {:?}", .0, .1.ty)]
    UnsafeConversion(Type, TypedExpression),
    #[error("Can not convert from {:?} to {:?}", .0, .1)]
    CantConvert(Type, Type),
    #[error("Unimplemented Conversion from {:?} to {:?}", .0, .1)]
    UnimplementedConvertion(Type, Type),
}

impl From<RecordSet> for Type {
    fn from(value: RecordSet) -> Self {
        Self::RecordSet(Box::new(value))
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RecordSet(pub HashMap<String, Column>, pub bool);

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub enum Type {
    Json(JsonType),
    Sql(SqlType),
    Array(Box<Type>),
    Schema,
    RecordSet(Box<RecordSet>),
    #[default]
    Unknown,
    Null,
}

impl Type {
    #[must_use]
    pub fn inner(&self) -> Type {
        match self {
            Self::Array(t) => *t.clone(),
            Self::Json(JsonType::List) => JsonType::Any.into(),
            _ => Self::Null,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum JsonType {
    Any,
    List,
    Map,
    Number,
    Boolean,
    String,
    Null,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum SqlType {
    String,
    Boolean,
    Integer,
    UInteger,
    Float,
    Bytes,
    Number,
    /// SQL will try to match this type to
    /// whatever is needed
    Inferred,
    Time,
    Double,
    JSON,
    Null,
}

impl FromStr for SqlType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[allow(clippy::match_same_arms, reason = "Explicitly list all possibilities")]
        Ok(match s.to_lowercase().as_str() {
            "text" => Self::String,
            "bool" => Self::Boolean,
            "boolean" => Self::Boolean,
            "int" => Self::Integer,
            "integer" => Self::Integer,
            "uint" => Self::UInteger,
            "null" => Self::Null,
            "unsigned integer" => Self::UInteger,
            "float" => Self::Float,
            "double" => Self::Double,
            "bytes" => Self::Bytes,
            "timestamp with time zone" => Self::Time,
            "timestamp" => Self::Time,
            "_" => Self::Inferred,
            _ => return Err(()),
        })
    }
}

#[must_use]
pub fn simple_func(name: &str, arg: SimpleExpr) -> SimpleExpr {
    SimpleExpr::FunctionCall(Func::cust(alias(name)).arg(arg))
}

impl TypeConversion for Type {
    /// Try to convert the given expr into one of the given type
    ///
    /// The returned [`TypedExpression`] may have a type that is more
    /// specific than the one that was requested (e.g `JsonType::List` instead of
    /// `JsonType::Any` or the original type instead of `Unknown`)
    fn try_convert(&self, expr: TypedExpression) -> Result<TypedExpression, ConversionError> {
        match self {
            Type::Json(json_type) => json_type.try_convert(expr),
            Type::RecordSet(_) if matches!(expr.ty, Type::RecordSet(_)) => Ok(expr),
            Type::RecordSet(_) if expr.ty == Type::Json(JsonType::List) => Ok(TypedExpression {
                expr: simple_func("jsonb_array_elements", expr.expr),
                ty: self.clone(),
            }),
            _ => Err(ConversionError::UnimplementedConvertion(
                expr.ty,
                self.clone(),
            )),
        }
    }
}

impl TypeConversion for JsonType {
    fn try_convert(&self, expr: TypedExpression) -> Result<TypedExpression, ConversionError> {
        let ty = Type::Json(self.clone());

        match (expr.expr, expr.ty, self) {
            (
                SimpleExpr::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b),
                _,
                Self::Any,
            ) => Ok(TypedExpression {
                expr: SimpleExpr::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b),
                ty: Self::Any.into(),
            }),
            (expr, ty @ Type::Json(_), Self::Any) => Ok(TypedExpression { expr, ty }),
            (expr, Type::Json(from), to) => match (from, to) {
                (from, to) if from == *to => Ok(TypedExpression { expr, ty }),
                (_, Self::Any) => Ok(TypedExpression { expr, ty }),
                (from @ Self::Any, _) => Err(ConversionError::UnsafeConversion(
                    Type::Json(from),
                    TypedExpression { expr, ty },
                )),
                (from, _) => Err(ConversionError::CantConvert(
                    from.clone().into(),
                    to.clone().into(),
                )),
            },
            (
                expr,
                Type::Sql(
                    SqlType::Number
                    | SqlType::Float
                    | SqlType::Double
                    | SqlType::Integer
                    | SqlType::UInteger,
                ),
                Self::Any | Self::Number,
            ) => Ok(TypedExpression {
                expr: simple_func("to_jsonb", expr),
                ty: Self::Number.into(),
            }),
            (expr, Type::Sql(SqlType::String), Self::Any | Self::String) => Ok(TypedExpression {
                expr: simple_func("to_jsonb", expr.cast_as("text")),
                ty: Self::String.into(),
            }),
            (expr, Type::Sql(SqlType::Boolean), Self::Any | Self::Boolean) => Ok(TypedExpression {
                expr: simple_func("to_jsonb", expr),
                ty: Self::Boolean.into(),
            }),
            (expr, Type::RecordSet(_), Self::Any | Self::List) => Ok(TypedExpression {
                expr: simple_func("to_jsonb", simple_func("array", expr)),
                ty: Self::List.into(),
            }),
            (expr, Type::Unknown, Self::Any) => Ok(TypedExpression {
                expr: expr.cast_as(alias("jsonb")),
                ty: Type::Json(JsonType::Any),
            }),
            (_, Type::Null, Self::Any | Self::Null) => Ok(TypedExpression {
                expr: SimpleExpr::Constant(sea_query::Value::Json(Some(Box::new(
                    serde_json::Value::Null,
                ))))
                .cast_as("jsonb"),
                ty: Self::Null.into(),
            }),
            (_, a, b) => Err(ConversionError::UnimplementedConvertion(
                a.clone(),
                Type::Json(b.clone()),
            )),
        }
    }
}
impl From<JsonType> for Type {
    fn from(value: JsonType) -> Self {
        Type::Json(value)
    }
}

impl From<SqlType> for Type {
    fn from(value: SqlType) -> Self {
        Type::Sql(value)
    }
}
