use sea_query::{ColumnRef, ExprTrait, Func, IntoIden, Query, SelectExpr};

use super::Function;
use crate::{
    Transpiler,
    intermediate::{Expression, Ident, Rc, ToSql},
    magic::x,
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::{Result, str_alias},
    types::{SqlType, Type, TypedExpression},
};

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
    var: Ident,
    rec: Expression,
    aggs: Vec<Expression>,
    reducer: Reducer,
}

impl Reduce {
    pub fn new(
        reducer: Reducer,
        rec: &Expression,
        var: Option<&Expression>,
        aggregate: &[Expression],
    ) -> Result<Rc<Self>> {
        let aggs: Vec<_> = aggregate.to_vec();
        Ok(Rc::new(Self {
            var: var.cloned().unwrap_or_else(x).as_single_ident()?.clone(),
            rec: rec.clone(),
            aggs,
            reducer,
        }))
    }
}

impl Function for Reduce {}

impl ToSql for Reduce {
    fn returntype(&self, tp: &Transpiler) -> Type {
        if self.aggs.is_empty() {
            SqlType::Unsigned.into()
        } else {
            let Ok(source) = self.rec.try_iterate(tp, self.var.clone().into_iden()) else {
                return Type::default();
            };

            let tp_inner = tp.iterate(&source);
            {
                Type::NamedView(
                    self.aggs
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            let name = format!("c_{i}");
                            (name, x.returntype(&tp_inner).col_type().unwrap().clone())
                        })
                        .chain([("count".to_owned(), SqlType::Integer)])
                        .collect(),
                )
            }
        }
    }

    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        let var = self.var.clone().into_iden();
        let source = self.rec.try_iterate(tp, var.clone())?;

        let tp_inner = tp.iterate(&source);

        let aggs = self
            .aggs
            .iter()
            .map(|x| Ok(x.to_sql(&tp_inner)?.expr))
            .collect::<Result<Vec<_>>>()?;

        Ok(Query::select()
            .exprs(
                aggs.iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, expr)| SelectExpr {
                        expr,
                        alias: Some(str_alias(format!("c_{index}"))),
                        window: None,
                    }),
            )
            .add_group_by(aggs.iter().cloned())
            .expr(match &self.reducer {
                Reducer::Min => Func::min(ColumnRef::Column(var)),
                Reducer::Max => Func::max(ColumnRef::Column(var)),
                Reducer::Count => Func::count(ColumnRef::Column(var)),
                Reducer::Sum => Func::sum(ColumnRef::Column(var).cast_as("float")),
                Reducer::Mean => Func::avg(ColumnRef::Column(var).cast_as("float")),
            })
            .from(source)
            .take()
            .into_expr()
            .with_type(self.returntype(tp)))
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
        let sql = postgres("[1, 2, 3].size()").unwrap();
        assert_eq!(sql, "3");
    }

    #[test]
    fn count_func() {
        let sql = postgres("[1, 2, 3].map(x, x).size()").unwrap();
        assert_eq!(sql, "3");

        let sql = postgres("[1, 2, 3].filter(x, x != 2).size()").unwrap();
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
            r#"(SELECT "x".* FROM "foo" AS "x" WHERE "x"."number" = 2)"#
        );

        let sql = hacks::get_plaintext_expression(
            "foo.filter(x, x.number == 2).count(x, x.number)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number" AS "c_0", COUNT("x") FROM (SELECT "x".* FROM "foo" AS "x" WHERE "x"."number" = 2) AS "x" GROUP BY "x"."number")"#
        );

        let sql =
            hacks::get_plaintext_expression("foo.count(x, x.number)", &tp, PostgresQueryBuilder)
                .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number" AS "c_0", COUNT("x") FROM "foo" AS "x" GROUP BY "x"."number")"#
        );
    }

    #[test]
    fn count_agg_single() {
        let tp = Transpiler::quick(
            vec![("postgres", vec![("foo", vec![("bar", SqlType::Unsigned)])])].into(),
        );

        let sql =
            get_plaintext_expression("foo.count(x, x.bar)", &tp, PostgresQueryBuilder).unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar" AS "c_0", COUNT("x") FROM "foo" AS "x" GROUP BY "x"."bar")"#
        );
    }

    #[test]
    fn count_agg_multi() {
        let tp = Transpiler::new()
            .layout(SqlLayout::new(Database::new().schema(
                Schema::new("postgres").table(Table::new("foo").columns([
                    ("bar_1", SqlType::Unsigned),
                    ("bar_2", SqlType::Unsigned),
                    ("bar_3", SqlType::Unsigned),
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
            r#"(SELECT "x"."bar_1" AS "c_0", "x"."bar_2" AS "c_1", "x"."bar_3" AS "c_2", COUNT("x") FROM "foo" AS "x" GROUP BY "x"."bar_1", "x"."bar_2", "x"."bar_3")"#
        );
    }
}
