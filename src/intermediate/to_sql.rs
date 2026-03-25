use sea_query::{DynIden, Query, SimpleExpr, SubQueryStatement, TableRef};

use crate::{
    Error, Result, Transpiler,
    functions::iter::{IterKind, Iterable},
    structure::{Column, Table},
    types::{Type, TypedExpression},
};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression>;

    // fn cast(&self, tp: &Transpiler, ty: Type) -> Result<SimpleExpr> {
    //     Ok(ty.try_convert(tp, self.to_sql(tp)?)?.expr)
    // }

    fn returntype(&self, tp: &Transpiler) -> Type;

    fn try_iterate(&self, tp: &Transpiler, var: DynIden) -> Result<Iterable> {
        try_iterate_fallback(self, tp, &var)
    }
    // fn try_iterate(&self, tp: &Transpiler, _var: DynIden) -> Result<Iterable>;
}

pub fn try_iterate_fallback<T: ToSql + ?Sized>(
    me: &T,
    tp: &Transpiler,
    var: &DynIden,
) -> Result<Iterable> {
    Ok(match me.returntype(tp) {
        Type::Column(name, ty) => Iterable {
            expr: TableRef::SubQuery(
                Query::select().expr(me.to_sql(tp)?.expr).take(),
                var.clone(),
            ),
            kind: IterKind::Column(Column::new(name.unwrap_or_default(), ty)),
        },
        Type::NamedView(index_map) => match me.to_sql(tp)?.expr {
            SimpleExpr::SubQuery(None, sub) => {
                if let SubQueryStatement::SelectStatement(sub) = *sub {
                    Iterable {
                        expr: TableRef::SubQuery(sub, var.clone()),
                        kind: IterKind::Table(Table::new(var.to_string()).columns(index_map)),
                    }
                } else {
                    return Err(Error::CanNotIterateType(me.returntype(tp)));
                }
            }
            _ => return Err(Error::CanNotIterateType(me.returntype(tp))),
        },

        // Type::Cell(cell) => cell.try_iterate(),
        _ => return Err(Error::CanNotIterateType(me.returntype(tp))),
    })
}

// impl ToSql for SimpleExpr {
//     fn to_sql(&self, _: &Transpiler) -> Result<TypedExpression> {
//         Ok(TypedExpression {
//             expr: self.clone(),
//             ty: Type::Unknown,
//         })
//     }

//     fn returntype(&self, _tp: &Transpiler) -> Type {
//         Type::Unknown
//     }
// }

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
