use std::{collections::HashMap, result::Result, str::FromStr};

use sea_query::{
    BinOper, Func, IntoColumnRef, IntoIden, Query, SimpleExpr, extension::postgres::PgBinOper,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    Transpiler, functions::subquery, intermediate::ToSql, sql_extensions::SqlExtension,
    structure::Column, transpiler::ParseError, transpiler::alias,
};

pub trait TypeConversion {
    fn try_convert(
        &self,
        tp: &Transpiler,
        expr: TypedExpression,
    ) -> Result<TypedExpression, ConversionError>;

    /// Assume `UnsafeConversions` are succesfull
    fn force_convert(
        &self,
        tp: &Transpiler,
        expr: TypedExpression,
    ) -> Result<TypedExpression, ConversionError> {
        match self.try_convert(tp, expr) {
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
    pub fn assume_is(&self, ty: impl Into<Type>) -> Self {
        Self {
            ty: ty.into(),
            ..self.clone()
        }
    }

    #[must_use]
    pub fn sql_cast(&self, type_name: &str, ty: Type) -> Self {
        Self {
            ty,
            expr: self.expr.clone().cast_as(alias(type_name)),
        }
    }
}

impl ToSql for TypedExpression {
    fn to_sql(&self, _tp: &crate::Transpiler) -> crate::Result<TypedExpression> {
        Ok(self.clone())
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> Type {
        self.ty.clone()
    }
}

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Unsafe Conversion from {:?} to {:?}", .0, .1.ty)]
    UnsafeConversion(Type, TypedExpression),
    #[error("Can not convert from {:?} to {:?}", .0, .1)]
    CantConvert(Type, Type),
    #[error("Unimplemented Conversion from {:?} to {:?}", .0, .1)]
    UnimplementedConvertion(Type, Type),
    #[error(transparent)]
    ParseError(Box<ParseError>),
}

impl From<ParseError> for ConversionError {
    fn from(value: ParseError) -> Self {
        Self::ParseError(Box::new(value))
    }
}

impl From<RecordSet> for Type {
    fn from(value: RecordSet) -> Self {
        Self::RecordSet(value.into())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct RecordSet {
    pub colummns: HashMap<String, Column>,
    pub known_content: bool,
}

impl RecordSet {
    #[must_use]
    pub const fn with_cols(colummns: HashMap<String, Column>) -> Self {
        Self {
            colummns,
            known_content: true,
        }
    }

    #[must_use]
    pub fn anon_list() -> Self {
        Self::with_cols([("_".to_owned(), Column::new("_", SqlType::JSON))].into())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub enum Type {
    Json(JsonType),
    Sql(SqlType),
    /// Simulate the CEL time type
    Time,
    Array(Box<Type>),
    Schema,
    RecordSet(Box<RecordSet>),
    #[default]
    Unknown,
    Null,
}

impl Type {
    #[must_use]
    pub fn inner(&self) -> Self {
        match self {
            Self::Array(t) => *t.clone(),
            Self::Json(JsonType::List) => JsonType::Any.into(),
            _ => Self::Null,
        }
    }

    #[must_use]
    pub const fn is_json(&self) -> bool {
        matches!(self, Self::Json(_) | Self::Sql(SqlType::JSON))
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize, Copy)]
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
            "time" => Self::Time,
            "timestamp with time zone" => Self::Time,
            "timestamp" => Self::Time,
            "_" => Self::Inferred,
            _ => return Err(()),
        })
    }
}

impl IntoIden for SqlType {
    fn into_iden(self) -> sea_query::DynIden {
        #[allow(
            clippy::match_same_arms,
            reason = "String and Inferred both map to 'text' but for different reasons"
        )]
        alias(match self {
            Self::String => "text",
            Self::Boolean => "bool",
            Self::Integer => "int",
            Self::UInteger => "uint",
            Self::Float => "flaot",
            Self::Bytes => "bytes",
            Self::Number => "number",
            Self::Time => "timestamp with time zone",
            Self::Double => "double",
            Self::JSON => "jsonb",
            // This will always error
            Self::Null => "null",
            // Text is probably the closest to "universal"
            Self::Inferred => "text",
        })
    }
}

#[must_use]
pub fn simple_func(name: &'static str, arg: SimpleExpr) -> SimpleExpr {
    Func::cust(name).arg(arg).into()
}

impl TypeConversion for Type {
    /// Try to convert the given expr into one of the given type
    ///
    /// The returned [`TypedExpression`] may have a type that is more
    /// specific than the one that was requested (e.g `JsonType::List` instead of
    /// `JsonType::Any` or the original type instead of `Unknown`)
    fn try_convert(
        &self,
        tp: &Transpiler,
        TypedExpression { expr, ty }: TypedExpression,
    ) -> Result<TypedExpression, ConversionError> {
        Ok(match (ty, self) {
            (ty, a) if *a == ty => TypedExpression { expr, ty },
            (Self::RecordSet(b), Self::RecordSet(a)) if *a == b || !a.known_content => {
                expr.with_type(Self::RecordSet(b))
            }
            (ty, Self::Json(json_type)) => {
                json_type.try_convert(tp, TypedExpression { expr, ty })?
            }
            (ty, Self::Sql(sql_type)) => sql_type.try_convert(tp, TypedExpression { expr, ty })?,
            (Self::Json(JsonType::List) | Self::Sql(SqlType::JSON), Self::RecordSet(_)) => {
                simple_func("jsonb_array_elements", expr).with_type(self.clone())
            }
            (Self::Json(JsonType::Map), Self::RecordSet(_)) => {
                simple_func("jsonb_object_keys", expr).with_type(self.clone())
            }
            (ty @ Self::Sql(SqlType::JSON), _) => {
                JsonType::Any.try_convert(tp, TypedExpression { expr, ty })?
            }

            (ty, _) => return Err(ConversionError::UnimplementedConvertion(ty, self.clone())),
        })
    }
}

impl TypeConversion for JsonType {
    fn try_convert(
        &self,
        tp: &Transpiler,
        expr: TypedExpression,
    ) -> Result<TypedExpression, ConversionError> {
        Ok(match (expr.expr, expr.ty, self) {
            (
                SimpleExpr::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b),
                _,
                Self::Any,
            ) => SimpleExpr::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b)
                .with_type(Self::Any),
            (expr, ty @ Type::Json(_), Self::Any) => expr.with_type(ty),
            (expr, Type::Json(from), to) => match (from, to) {
                (from, to) if from == *to => expr.with_type(self),
                (_, Self::Any) => expr.with_type(self),
                (from @ Self::Any, _) => {
                    return Err(ConversionError::UnsafeConversion(
                        Type::Json(from),
                        expr.with_type(self),
                    ));
                }
                (from, _) => {
                    return Err(ConversionError::CantConvert(from.into(), to.clone().into()));
                }
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
            ) => simple_func("to_jsonb", expr).with_type(Self::Number),
            (expr, Type::Sql(SqlType::JSON), Self::Any) => expr.with_type(Self::Any),
            (expr, Type::Sql(SqlType::String), Self::Any | Self::String) => {
                simple_func("to_jsonb", expr.cast_as("text")).with_type(Self::String)
            }
            (expr, Type::Sql(SqlType::Boolean), Self::Any | Self::Boolean) => {
                simple_func("to_jsonb", expr).with_type(Self::Boolean)
            }
            (expr, Type::RecordSet(r), Self::List | Self::Any) => {
                if r.colummns.len() == 1 {
                    simple_func("to_jsonb", simple_func("array", expr)).with_type(Self::List)
                } else {
                    let a1 = tp.alias();
                    subquery(
                        Query::select()
                            .expr(simple_func(
                                "to_jsonb",
                                simple_func(
                                    "array",
                                    subquery(
                                        Query::select()
                                            .expr(simple_func(
                                                "row_to_json",
                                                a1.into_column_ref().into(),
                                            ))
                                            .from_subquery(
                                                expr.with_type(Type::RecordSet(r))
                                                    .to_record_set(tp)?,
                                                a1,
                                            )
                                            .take(),
                                    )
                                    .expr,
                                ),
                            ))
                            .take(),
                    )
                    .assume_is(Self::List)
                }
            }
            (expr, ty @ Type::RecordSet(_), Self::Map) => {
                let alias = tp.alias_key_value();
                subquery(
                    Query::select()
                        .expr(SimpleExpr::FunctionCall(
                            Func::cust("jsonb_object_agg")
                                .arg(alias.key())
                                .arg(alias.value()),
                        ))
                        .from_subquery(expr.with_type(ty).to_record_set(tp)?, alias)
                        .take(),
                )
            }
            .assume_is(Self::Map),
            (expr, Type::Unknown, Self::Any) => expr.cast_as("jsonb").with_type(Self::Any),
            (_, Type::Null, Self::Any | Self::Null) => {
                SimpleExpr::Constant(serde_json::Value::Null.into())
                    .cast_as("jsonb")
                    .with_type(Self::Null)
            }
            (_, a, b) => {
                return Err(ConversionError::UnimplementedConvertion(a, b.into()));
            }
        })
    }
}

impl TypeConversion for SqlType {
    fn try_convert(
        &self,
        tp: &Transpiler,
        TypedExpression { expr, ty }: TypedExpression,
    ) -> Result<TypedExpression, ConversionError> {
        let expr = expr.into_json_cast();
        #[allow(clippy::match_same_arms)]
        Ok(match (ty, self) {
            (ty, a) if Type::Sql(*a) == ty => TypedExpression { expr, ty },
            // JSON types
            (Type::Json(_), Self::JSON) => expr.with_type(Self::JSON),
            (Type::Json(JsonType::Any | JsonType::Number), ty @ Self::Integer) => {
                expr.sql_cast("int", ty)
            }
            (Type::Json(JsonType::Any | JsonType::Number), ty @ Self::Number) => {
                expr.sql_cast("float", ty)
            }
            (Type::Json(JsonType::Any | JsonType::Number), ty @ Self::Float) => {
                expr.sql_cast("float", ty)
            }
            (Type::Json(_), ty @ Self::String) => expr.sql_cast("text", ty),
            (Type::Json(JsonType::Any | JsonType::Boolean), ty @ Self::Boolean) => {
                expr.sql_cast("bool", ty)
            }
            // SQL types
            (Type::Sql(sql_type), s) => match (sql_type, s) {
                (_, ty @ Self::String) => expr.sql_cast("text", ty),
                (ty, Self::JSON) => JsonType::Any.try_convert(tp, expr.with_type(ty))?,
                (Self::Integer, ty @ Self::Number) => expr.with_type(ty),
                (Self::UInteger, Self::Integer) => expr.with_type(Self::UInteger),
                (Self::Number, ty @ Self::Integer) => expr.sql_cast("int", ty),
                (Self::Time, Self::Integer) => {
                    SimpleExpr::FunctionCall(Func::cust("date_part").arg("epoch").arg(expr))
                        .with_type(Self::Integer)
                }
                (Self::JSON, ty @ Self::Boolean) => expr.sql_cast("bool", ty),
                (Self::JSON, ty @ Self::Integer) => expr.sql_cast("int", ty),
                (Self::JSON, ty @ Self::UInteger) => expr.sql_cast("uint", ty),
                (Self::JSON, ty @ Self::Float) => expr.sql_cast("float", ty),
                (Self::JSON, Self::Number) => expr.sql_cast("float", Self::Float),
                (Self::JSON, ty @ Self::Double) => expr.sql_cast("double", ty),

                (a, b) => {
                    return Err(ConversionError::UnimplementedConvertion(
                        a.into(),
                        (*b).into(),
                    ));
                }
            },

            (Type::Unknown, ty @ Self::Boolean) => expr.sql_cast("bool", ty),
            (Type::Unknown, ty @ Self::Integer) => expr.sql_cast("int", ty),
            (Type::Unknown, ty @ Self::UInteger) => expr.sql_cast("uint", ty),
            (Type::Unknown, ty @ Self::Float) => expr.sql_cast("float", ty),
            (Type::Unknown, Self::Number) => expr.sql_cast("float", Self::Float),
            (Type::Unknown, ty @ Self::Double) => expr.sql_cast("double", ty),

            (a, b) => {
                return Err(ConversionError::UnimplementedConvertion(a, Type::Sql(*b)));
            }
        })
    }
}

impl From<JsonType> for Type {
    fn from(value: JsonType) -> Self {
        Self::Json(value)
    }
}
impl From<&JsonType> for Type {
    fn from(value: &JsonType) -> Self {
        Self::Json(value.clone())
    }
}

impl From<SqlType> for Type {
    fn from(value: SqlType) -> Self {
        Self::Sql(value)
    }
}

impl From<&SqlType> for Type {
    fn from(value: &SqlType) -> Self {
        Self::Sql(*value)
    }
}
