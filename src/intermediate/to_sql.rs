use sea_query::{DynIden, IntoIden, Query, SimpleExpr, SubQueryStatement, TableRef};

use crate::{
    Error, Result, Transpiler,
    functions::{
        aggregate::buckets::Buckets,
        iter::{IterKind, Iterable},
    },
    sql_extensions::SqlExtension,
    structure::{Column, Table},
    types::{Type, TypedExpression},
};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression>;

    fn returntype(&self, tp: &Transpiler) -> Type;

    fn try_iterate(&self, tp: &Transpiler, var: impl IntoIden) -> Result<Iterable> {
        try_iterate_fallback(self, tp, &var.into_iden())
    }

    fn try_as_buckets(&self, tp: &Transpiler, var: impl IntoIden) -> Result<Buckets> {
        let te = self.to_sql(tp)?;
        let var = var.into_iden();
        if let Type::NamedView { fields, groups } = te.ty
            && !groups.is_empty()
        {
            Ok(Buckets {
                select: self.to_sql(tp)?.as_select()?.clone(),
                layout: Table::new(var.to_string()).columns(fields),
                groups,
            })
        } else {
            let x = self.try_iterate(tp, var)?;
            let IterKind::Table(layout) = x.kind else {
                return Err(Error::CanOnlyUseViewsAsBuckets);
            };
            Ok(Buckets {
                select: Query::select().from(x.expr).take(),
                layout,
                groups: Default::default(),
            })
        }
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
                    return Err(Error::CanNotIterateType(Box::new(me.returntype(tp))));
                }
            }
            x => {
                dbg!(x);
                return Err(Error::CanNotIterateType(Box::new(me.returntype(tp))));
            }
        },
        Type::NamedView {
            fields: index_map, ..
        } => match me.to_sql(tp)?.expr {
            SimpleExpr::SubQuery(None, sub) => {
                if let SubQueryStatement::SelectStatement(sub) = *sub {
                    Iterable {
                        expr: TableRef::SubQuery(sub, var.clone()),
                        kind: IterKind::Table(Table::new(var.to_string()).columns(index_map)),
                    }
                } else {
                    return Err(Error::CanNotIterateType(Box::new(me.returntype(tp))));
                }
            }
            x => {
                dbg!(x);
                return Err(Error::CanNotIterateType(Box::new(me.returntype(tp))));
            }
        },

        // Type::Cell(cell) => cell.try_iterate(),
        x => {
            dbg!(x);
            return Err(Error::CanNotIterateType(Box::new(me.returntype(tp))));
        }
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
