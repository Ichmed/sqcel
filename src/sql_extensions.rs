use std::sync::Arc;

use sea_query::{
    BinOper, ExprTrait, Func, FunctionCall, IntoIden, Keyword, SelectStatement, SimpleExpr,
    SubQueryStatement, extension::postgres::PgBinOper,
};

use crate::{
    Error, Transpiler,
    transpiler::{alias::TableAlias, str_alias},
    types::{ColumnType, Type, TypedExpression},
};
pub trait SqlExtension: Sized {
    fn with_type(self, ty: impl Into<Type>) -> TypedExpression;
    #[must_use]
    fn into_json_cast(self) -> Self {
        self
    }
    #[must_use]
    fn into_json_get(self) -> Self {
        self
    }
    #[must_use]
    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression;

    fn as_select(&self) -> Result<&SelectStatement, Error> {
        Err(Error::NotASelectStatement)
    }

    fn extract(self, what: impl ToString) -> TypedExpression;
}

impl SqlExtension for TypedExpression {
    fn with_type(self, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            ty: ty.into(),
            ..self
        }
    }

    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            ty: ty.into(),
            expr: self.expr.cast_as(str_alias(type_name)),
        }
    }

    fn into_json_cast(self) -> Self {
        self.expr.into_json_cast().with_type(ColumnType::Text)
    }

    fn extract(self, what: impl ToString) -> Self {
        self.expr.extract(what).with_type(ColumnType::BigInteger)
    }
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
            Self::Binary(x, BinOper::Custom("#>>"), extr)
                if *extr == SimpleExpr::Constant("{}".into()) =>
            {
                *x
            }
            x => x,
        }
    }

    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            ty: ty.into(),
            expr: self.cast_as(str_alias(type_name)),
        }
    }

    fn with_type(self, ty: impl Into<Type>) -> TypedExpression {
        TypedExpression {
            expr: self,
            ty: ty.into(),
        }
    }

    fn as_select(&self) -> Result<&SelectStatement, Error> {
        match self {
            SimpleExpr::SubQuery(None, x) => match &**x {
                SubQueryStatement::SelectStatement(x) => Ok(&x),
                _ => Err(Error::NotASelectStatement),
            },
            _ => Err(Error::NotASelectStatement),
        }
    }

    fn extract(self, what: impl ToString) -> TypedExpression {
        Func::cust("EXTRACT")
            .arg(SimpleExpr::Binary(
                Box::new(SimpleExpr::Keyword(Keyword::Custom(str_alias(what)))),
                BinOper::Custom("from"),
                Box::new(self),
            ))
            .cast_as("int")
            .with_type(ColumnType::BigInteger)
    }
}

impl SqlExtension for FunctionCall {
    fn with_type(self, ty: impl Into<Type>) -> TypedExpression {
        SimpleExpr::FunctionCall(self).with_type(ty)
    }

    fn sql_cast(self, type_name: &str, ty: impl Into<Type>) -> TypedExpression {
        SimpleExpr::FunctionCall(self).sql_cast(type_name, ty)
    }

    fn extract(self, what: impl ToString) -> TypedExpression {
        SimpleExpr::FunctionCall(self).extract(what)
    }
}

pub trait IntoSqlExpression {
    fn into_expr(self) -> SimpleExpr;
}

impl IntoSqlExpression for SelectStatement {
    fn into_expr(self) -> SimpleExpr {
        SimpleExpr::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(self)))
    }
}

pub trait AliasWarpping {
    fn into_table_alias(self, tp: &Transpiler) -> TableAlias<1>;
}

impl<T: IntoIden> AliasWarpping for T {
    fn into_table_alias(self, tp: &Transpiler) -> TableAlias<1> {
        TableAlias(Arc::new((tp.alias().0, [self.into_iden()])))
    }
}
