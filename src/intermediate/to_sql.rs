use sea_query::{Func, SimpleExpr};

use crate::{Result, Transpiler, transpiler::alias};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<SimpleExpr>;

    fn to_json(&self, tp: &Transpiler) -> Result<JsonExpression> {
        Ok(JsonExpression(SimpleExpr::FunctionCall(
            Func::cust(alias("to_jsonb")).arg(self.to_sql(tp)?),
        )))
    }
}

impl ToSql for SimpleExpr {
    fn to_sql(&self, _: &Transpiler) -> Result<SimpleExpr> {
        Ok(self.clone())
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
);
