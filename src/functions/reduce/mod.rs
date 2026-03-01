use std::sync::Arc;

use super::{Function, subquery};
use crate::{
    Transpiler,
    intermediate::{Expression, ExpressionInner, Ident, Rc, ToSql},
    sql_extensions::SqlExtension,
    structure::{Column, Table},
    transpiler::Result,
    types::{RecordSet, SqlType, Type, TypedExpression},
    variables::Variable,
};
use sea_query::{ColumnRef, ExprTrait, Func, Iden, IntoIden, Query, SimpleExpr};

enum Reduceable {
    /// This is a fixed size list, its length is fix
    Fix(usize),
    // Subquery(Box<SelectStatement>, Vec<Expression>),
    // Access(AccessChain, Vec<Expression>),
    Expr(Expression, Vec<Expression>),
}

#[non_exhaustive]
pub enum Reducer {
    Count,
    Sum,
    Mean,
    Max,
    Min,
}

impl Reducer {}

pub struct Reduce {
    var: Option<Ident>,
    reducer: Reducer,
    reduceable: Reduceable,
}

impl Reduce {
    pub fn new(
        reducer: Reducer,
        arg: &Expression,
        var: Option<&Expression>,
        aggregate: &[Expression],
    ) -> Result<Rc<Self>> {
        let aggregate: Vec<_> = aggregate.to_vec();
        let reduceable = match (&reducer, &**arg) {
            (reducer, ExpressionInner::Variable(v)) => match (reducer, v) {
                (Reducer::Count, Variable::List(expressions)) => Reduceable::Fix(expressions.len()),
                (_, _) => Reduceable::Expr(arg.clone(), aggregate),
            },
            (_, _) => Reduceable::Expr(arg.clone(), aggregate),
        };
        Ok(Rc::new(Self {
            var: var.map(|x| x.as_single_ident()).transpose()?.cloned(),
            reducer,
            reduceable,
        }))
    }
}

impl Function for Reduce {}

impl ToSql for Reduce {
    fn returntype(&self, tp: &Transpiler) -> Type {
        match &self.reduceable {
            Reduceable::Expr(_, x) if !x.is_empty() => RecordSet::with_cols(
                x.iter()
                    .enumerate()
                    .map(|(i, x)| {
                        let name = format!("_{i}");
                        (
                            name.clone(),
                            Column::new(
                                name,
                                if let Type::Sql(ty) = x.returntype(tp) {
                                    ty
                                } else {
                                    SqlType::Inferred
                                },
                            ),
                        )
                    })
                    .chain([("count".to_owned(), Column::new("count", SqlType::Integer))])
                    .collect(),
            )
            .into(),
            _ => Type::Sql(SqlType::UInteger),
        }
    }

    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        let alias = self
            .var
            .clone()
            .unwrap_or_else(|| Ident(Arc::new(tp.alias().to_string())));

        match &self.reduceable {
            Reduceable::Fix(fix) => {
                Ok(SimpleExpr::Constant((*fix as u64).into()).with_type(SqlType::UInteger))
            }

            Reduceable::Expr(expression_inner, aggregate) => {
                let tp_inner = tp.clone().enter_anonymous_schema(
                    [(
                        alias.to_string(),
                        Table::new(alias.to_string()).columns(expression_inner.columns(tp)),
                    )]
                    .into(),
                );
                let aggregate = aggregate
                    .iter()
                    .map(|x| Ok(x.to_sql(&tp_inner)?.expr))
                    .collect::<Result<Vec<_>>>()?;
                let mut prepared = Query::select()
                    .from_subquery(expression_inner.to_record_set(tp)?, &alias)
                    .add_group_by(aggregate.iter().cloned())
                    .exprs(aggregate.iter().cloned())
                    .take();

                Ok(subquery(
                    match &self.reducer {
                        Reducer::Min => {
                            prepared.expr(Func::min(ColumnRef::Column(alias.into_iden())))
                        }
                        Reducer::Max => {
                            prepared.expr(Func::max(ColumnRef::Column(alias.into_iden())))
                        }
                        Reducer::Count => {
                            prepared.expr(Func::count(ColumnRef::Column(alias.into_iden())))
                        }
                        Reducer::Sum => prepared.expr(Func::sum(
                            ColumnRef::Column(alias.into_iden()).cast_as("float"),
                        )),
                        Reducer::Mean => prepared.expr(Func::avg(
                            ColumnRef::Column(alias.into_iden()).cast_as("float"),
                        )),
                    }
                    .take(),
                )
                .assume_is(self.returntype(tp)))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use sea_query::PostgresQueryBuilder;

    use crate::{
        Transpiler,
        hacks::{self, get_plaintext_expression, postgres},
        structure::{Database, Schema, SqlLayout, Table},
        types::SqlType,
    };

    #[test]
    fn count_list() {
        let sql = postgres("[1, 2, 3].count()").unwrap();
        assert_eq!(sql, "3");
    }

    #[test]
    fn count_func() {
        let sql = postgres("[1, 2, 3].map(x, x).count()").unwrap();
        assert_eq!(sql, "3");

        let sql = postgres("[1, 2, 3].filter(x, x != 2).count()").unwrap();
        assert_eq!(sql, "2");

        let tp = Transpiler::quick(
            vec![("sch", vec![("foo", vec![("number", SqlType::Integer)])])].into(),
        )
        .to_builder()
        .build()
        .unwrap();

        let sql = hacks::get_plaintext_expression(
            "foo.filter(x, x.number == 2)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT * FROM (SELECT * FROM "foo") AS "x" WHERE "x"."number" = 2)"#
        );

        let sql = hacks::get_plaintext_expression(
            "foo.filter(x, x.number == 2).count(x, x.number)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number", COUNT("x") FROM (SELECT * FROM (SELECT * FROM "foo") AS "x" WHERE "x"."number" = 2) AS "x" GROUP BY "x"."number")"#
        );

        let sql =
            hacks::get_plaintext_expression("foo.count(x, x.number)", &tp, PostgresQueryBuilder)
                .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number", COUNT("x") FROM (SELECT * FROM "foo") AS "x" GROUP BY "x"."number")"#
        );
    }

    #[test]
    fn count_agg_single() {
        let tp = Transpiler::quick(
            vec![("postgres", vec![("foo", vec![("bar", SqlType::UInteger)])])].into(),
        );

        let sql =
            get_plaintext_expression("foo.count(x, x.bar)", &tp, PostgresQueryBuilder).unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar", COUNT("x") FROM (SELECT * FROM "foo") AS "x" GROUP BY "x"."bar")"#
        );
    }

    #[test]
    fn count_agg_multi() {
        let tp = Transpiler::new()
            .layout(SqlLayout::new(Database::new().schema(
                Schema::new("postgres").table(Table::new("foo").columns([
                    ("bar_1", SqlType::UInteger),
                    ("bar_2", SqlType::UInteger),
                    ("bar_3", SqlType::UInteger),
                ])),
            )))
            .enter_schema("postgres")
            .build()
            .unwrap();

        let sql = get_plaintext_expression(
            "foo.count(x, x.bar_1, x.bar_2, x.bar_3)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar_1", "x"."bar_2", "x"."bar_3", COUNT("x") FROM (SELECT * FROM "foo") AS "x" GROUP BY "x"."bar_1", "x"."bar_2", "x"."bar_3")"#
        );
    }
}
