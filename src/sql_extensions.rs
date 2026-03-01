use sea_query::{BinOper, FunctionCall, SimpleExpr, extension::postgres::PgBinOper};

use crate::{
    transpiler::alias,
    types::{Type, TypedExpression},
};
pub trait SqlExtension {
    fn with_type(self, ty: impl Into<Type>) -> TypedExpression;
    #[must_use]
    fn into_json_cast(self) -> Self;
    #[must_use]
    fn into_json_get(self) -> Self;
    #[must_use]
    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression;
}

impl SqlExtension for SimpleExpr {
    fn into_json_cast(self) -> Self {
        match self {
            Self::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b) => {
                Self::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b)
            }
            x => x,
        }
    }

    fn into_json_get(self) -> Self {
        match self {
            Self::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b) => {
                Self::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b)
            }
            x => x,
        }
    }

    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            ty: ty.into(),
            expr: self.cast_as(alias(type_name)),
        }
    }

    fn with_type(self, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            expr: self,
            ty: ty.into(),
        }
    }
}

impl SqlExtension for FunctionCall {
    fn with_type(self, ty: impl Into<Type>) -> TypedExpression {
        SimpleExpr::FunctionCall(self).with_type(ty)
    }

    fn into_json_cast(self) -> Self {
        self
    }

    fn into_json_get(self) -> Self {
        self
    }

    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression {
        SimpleExpr::FunctionCall(self).sql_cast(type_name, ty)
    }
}
