use sea_query::{DynIden, IntoIden, Query, SimpleExpr, SubQueryStatement, TableRef};

use crate::{
    Error, Result, Transpiler,
    functions::iter::{IterKind, Iterable},
    structure::{Column, Table},
    types::{Type, TypedExpression},
};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression>;

    fn returntype(&self, tp: &Transpiler) -> Type;

    fn try_iterate(&self, tp: &Transpiler, var: impl IntoIden) -> Result<Iterable> {
        try_iterate_fallback(self, tp, &var.into_iden())
    }
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
        // Type::Row(_items) => todo!(),
        // Type::NamedRow(_items) => todo!(),
        Type::View(items) => match me.to_sql(tp)?.expr {
            SimpleExpr::SubQuery(None, sub) => {
                if let SubQueryStatement::SelectStatement(sub) = *sub {
                    Iterable {
                        expr: TableRef::SubQuery(sub, var.clone()),
                        kind: IterKind::Table(
                            items
                                .map(|items| {
                                    Table::new(var.to_string()).columns(
                                        items.into_iter().filter_map(|(k, v)| k.map(|k| (k, v))),
                                    )
                                })
                                .unwrap_or_default(),
                        ),
                    }
                } else {
                    return Err(Error::CanNotIterateType(me.returntype(tp)));
                }
            }
            _ => return Err(Error::CanNotIterateType(me.returntype(tp))),
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
