use sea_query::{Query, SelectStatement, SimpleExpr, SubQueryStatement};

use crate::{
    Result, Transpiler,
    transpiler::alias,
    types::{Type, TypeConversion, TypedExpression},
};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression>;

    fn cast(&self, tp: &Transpiler, ty: Type) -> Result<SimpleExpr> {
        Ok(ty.try_convert(self.to_sql(tp)?)?.expr)
    }

    fn assume(&self, tp: &Transpiler, ty: Type) -> Result<TypedExpression> {
        Ok(TypedExpression {
            expr: self.to_sql(tp)?.expr,
            ty,
        })
    }

    fn to_record_set(&self, tp: &Transpiler, a: &str) -> Result<SelectStatement> {
        Ok(match self.cast(tp, Type::RecordSet)? {
            SimpleExpr::SubQuery(_, x) => match *x {
                SubQueryStatement::SelectStatement(select_statement) => select_statement,
                _ => todo!(),
            },
            x => Query::select().expr_as(x, alias(a)).take(),
        })
    }
}

impl ToSql for SimpleExpr {
    fn to_sql(&self, _: &Transpiler) -> Result<TypedExpression> {
        Ok(TypedExpression {
            expr: self.clone(),
            ty: Type::Unknown,
        })
    }
}

macro_rules! wrappers {
    ($($(#[$attr:meta])* $name:ident),* $(,)?) => {$(
        #[doc = "An SQL Expression guaranteeing that "]
        $(#[$attr])*
        #[derive(Clone, Debug)]
        pub struct $name(SimpleExpr);

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = SimpleExpr;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$name> for SimpleExpr {
            fn from(value: $name) -> SimpleExpr {
                value.0
            }
        }

    )*

    };
}

wrappers!(
    /// the return type is `jsonb`
    JsonExpression,
    /// the return type is `numeric`
    NumericExpression,
);
