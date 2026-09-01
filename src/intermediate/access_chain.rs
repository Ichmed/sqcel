use std::{borrow::Cow, iter::once};

use sea_query::{
    ColumnRef, DynIden, Func, IntoIden, SimpleExpr, TableRef, extension::postgres::PgExpr,
};

use crate::{
    Error, Result, Transpiler,
    functions::iter::{IterKind, Iterable},
    intermediate::{Expression, Ident, ToSql},
    sql_extensions::{AliasWarpping, SqlExtension},
    structure::{Column, Table},
    types::{Cell, ColumnType, JsonType, Type, TypedExpression},
};

#[derive(Clone, Debug, PartialEq)]
pub struct AccessChain {
    pub(crate) head: Option<Box<Expression>>,
    pub(crate) idents: Vec<Ident>,
}

impl ToSql for AccessChain {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        let (head, rest) = match (&self.head, self.idents.as_slice()) {
            (None, [schema, table, column, rest @ ..])
                if let Some((r, t)) = tp.layout.column([schema, table, column]) =>
            {
                (SimpleExpr::Column(r).with_type(t), rest)
            }
            (None, [table, column, rest @ ..])
                if let Some((r, t)) = tp.layout.column([table, column]) =>
            {
                (SimpleExpr::Column(r).with_type(t), rest)
            }
            (None, [column, rest @ ..]) if let Some((r, t)) = tp.layout.column([column]) => {
                (SimpleExpr::Column(r).with_type(t), rest)
            }
            (None, [table, rest @ ..]) if let Some(r) = tp.layout.table_asterisk([table]) => {
                let types = tp.layout.table_columns([table]).unwrap().clone();
                (
                    SimpleExpr::Column(r).with_type(Type::NamedView(
                        types.into_iter().map(|c| (c.0, c.1.ty)).collect(),
                    )),
                    rest,
                )
            }
            (None, [var, rest @ ..]) if let Some(v) = tp.variables.get(var.as_str()) => {
                (v.to_sql(tp)?, rest)
            }
            (Some(head), rest) => (head.to_sql(tp)?, rest),
            (None, rest) => {
                return Err(Error::UnknownField(
                    rest.iter()
                        .map(|x| x.as_str().to_owned())
                        .collect::<Vec<_>>()
                        .join("."),
                    Default::default(),
                ));
            }
        };

        let (rest, tail) = match rest {
            [rest @ .., tail] => (rest, Some(SimpleExpr::Constant(tail.to_sql_string()))),
            rest => (rest, None),
        };

        Self::chain_to_json_access(
            once(head).chain(rest.iter().map(|s| TypedExpression {
                ty: Type::Unknown,
                expr: SimpleExpr::Constant(s.to_sql_string()),
            })),
            tail,
        )
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        match (&self.head, self.idents.as_slice()) {
            (None, [column, ..]) if let Some((_, ty)) = tp.layout.column([column]) => {
                Type::Column(Some(column.as_str().to_owned()), ty)
            }
            (None, [table, column, ..])
                if let Some(col) = tp
                    .layout
                    .table_columns([table])
                    .and_then(|t| t.get(column.as_str())) =>
            {
                Type::Column(Some(column.as_str().to_owned()), col.ty.clone())
            }
            (None, [table, ..]) if let Some(tab) = tp.layout.table_columns([table]) => {
                tab.clone().into()
            }
            (None, [table, ..]) if let Some(var) = tp.variables.get(table.as_str()) => {
                var.returntype(tp)
            }
            _ => Type::Unknown,
        }
    }

    fn try_iterate(&self, tp: &Transpiler, var: impl IntoIden) -> Result<Iterable> {
        let var = var.into_iden();
        Ok(match (&self.head, self.idents.as_slice()) {
            (None, p @ [schema, table, column]) if let Some((_, ty)) = tp.layout.column(p) => {
                json_to_iterable(
                    tp,
                    var,
                    ColumnRef::SchemaTableColumn(
                        schema.into_iden(),
                        table.into_iden(),
                        column.into_iden(),
                    ),
                    Some(column.to_string()),
                    ty,
                )?
            }
            (None, p @ [table, column]) if let Some((_, ty)) = tp.layout.column(p) => {
                json_to_iterable(
                    tp,
                    var,
                    ColumnRef::TableColumn(table.into_iden(), column.into_iden()),
                    Some(column.to_string()),
                    ty,
                )?
            }
            (None, p @ [column]) if let Some((_, ty)) = tp.layout.column(p) => json_to_iterable(
                tp,
                var,
                ColumnRef::Column(column.into_iden()),
                Some(column.to_string()),
                ty,
            )?,
            (None, [column, rest @ ..]) if let Some((r, t)) = tp.layout.column([column]) => {
                let (rest, tail) = match rest {
                    [rest @ .., tail] => (rest, Some(SimpleExpr::Constant(tail.to_sql_string()))),
                    rest => (rest, None),
                };
                json_to_iterable(
                    tp,
                    var,
                    Self::chain_to_json_access(
                        once(SimpleExpr::Column(r).with_type(t.clone())).chain(rest.iter().map(
                            |s| TypedExpression {
                                ty: Type::Unknown,
                                expr: SimpleExpr::Constant(s.to_sql_string()),
                            },
                        )),
                        tail,
                    )?
                    .expr,
                    Some(column.to_string()),
                    t,
                )?
            }
            (None, p @ [table]) if let Some(tab) = tp.layout.table(p) => Iterable {
                expr: match tab {
                    TableRef::Table(t) | TableRef::TableAlias(t, _) => {
                        TableRef::TableAlias(t, var.clone())
                    }
                    TableRef::SchemaTable(s, t) | TableRef::SchemaTableAlias(s, t, _) => {
                        TableRef::SchemaTableAlias(s, t, var.clone())
                    }
                    _ => return Err(Error::CanNotIterateType(self.returntype(tp))),
                },
                kind: IterKind::Table(
                    Table::new(var.to_string()).columns(
                        tp.layout
                            .schema
                            .as_ref()
                            .unwrap()
                            .0
                            .get(&table.to_string())
                            .unwrap()
                            .clone()
                            .layout,
                    ),
                ),
            },
            // (None, [variable]) if tp.variables.contains_key(variable.as_str()) => tp
            //     .variables
            //     .get(variable.as_str())
            //     .unwrap()
            //     .try_iterate(tp, var)?,
            (None, [variable]) if let Some(v) = tp.variables.get(variable.as_str()) => {
                v.try_iterate(tp, var)?
            }
            x => {
                return Err(Error::CanNotIterateType(
                    x.0.as_ref().map(|x| x.returntype(tp)).unwrap_or_default(),
                ));
            }
        })
    }
}

pub fn json_to_iterable(
    tp: &Transpiler,
    var: DynIden,
    col_ref: impl Into<SimpleExpr>,
    col_name: Option<String>,
    ty: ColumnType,
) -> Result<Iterable> {
    let ColumnType::Json(ty, _) = ty else {
        return Err(Error::CanNotIterateType(Type::Column(col_name, ty)));
    };
    let kind = IterKind::Column(Column::new(var.to_string(), ty));
    let expr = TableRef::FunctionCall(
        Func::cust("jsonb_array_elements").arg(col_ref),
        var.into_table_alias(tp).into_iden(),
    );
    Ok(Iterable { expr, kind })
}

impl AccessChain {
    #[must_use]
    pub const fn new(chain: Vec<Ident>) -> Self {
        Self {
            head: None,
            idents: chain,
        }
    }

    pub fn to_path(&self) -> Option<Cow<'_, str>> {
        match (self.head.as_ref(), self.idents.as_slice()) {
            (Some(_), _) | (_, []) => None,
            (None, [single]) => Some(Cow::Borrowed(single.as_str())),
            (None, more) => Some(Cow::Owned(
                more.iter().map(Ident::as_str).collect::<Vec<_>>().join("."),
            )),
        }
    }

    fn chain_to_json_access(
        chain: impl IntoIterator<Item = TypedExpression>,
        tail: Option<SimpleExpr>,
    ) -> Result<TypedExpression> {
        let outer = chain
            .into_iter()
            .reduce(|a, b| TypedExpression {
                ty: Cell::Value(JsonType::Any.into()).into(),
                expr: a.expr.get_json_field(b.expr),
            })
            .ok_or(Error::Todo("tried to create empty access chain"))?;

        Ok(match tail {
            Some(tail) => TypedExpression {
                ty: Cell::Value(JsonType::Any.into()).into(),
                expr: outer.expr.get_json_field(tail),
            },
            None => outer,
        })
    }

    pub const fn as_single_ident(&self) -> Result<&Ident> {
        match (self.head.as_ref(), self.idents.as_slice()) {
            (None, [single]) => Ok(single),
            _ => Err(Error::Todo("Invalid var name")),
        }
    }

    pub fn to_table_access(&self, tp: &Transpiler) -> Result<TableRef> {
        tp.layout
            .table(&self.idents)
            .ok_or(Error::Todo("Unknown table"))
    }
}
