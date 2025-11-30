use super::{Function, subquery};
use crate::{
    Transpiler,
    intermediate::{AccessChain, Expression, ExpressionInner, Rc, ToSql},
    transpiler::{ParseError, Result, alias},
    types::{RecordSet, SqlType, Type, TypedExpression},
    variables::Variable,
};
use sea_query::{ColumnRef, Func, Query, SelectStatement, SimpleExpr, Value};

enum Countable {
    /// This is a fixed size list, its length is fix
    Fix(usize),
    Subquery(Box<SelectStatement>, Vec<Expression>),
    Access(AccessChain, Vec<Expression>),
}

pub struct Count(Countable);

impl Count {
    pub fn new(arg: &Expression, aggregate: &[Expression]) -> Result<Rc<Self>> {
        let aggregate: Vec<_> = aggregate.to_vec();
        Ok(Rc::new(Count(match &**arg {
            ExpressionInner::Variable(v) => match v {
                Variable::List(_) | Variable::SqlSubQueryAtom(_, _) if !aggregate.is_empty() => {
                    return Err(ParseError::Todo("Can't use agg in static count"));
                }
                Variable::List(expressions) => Countable::Fix(expressions.len()),
                Variable::SqlSubQueryAtom(_, _) => Countable::Fix(1),
                Variable::SqlSubQuery(select_statement, _) => {
                    Countable::Subquery(select_statement.clone(), aggregate)
                }
                Variable::SqlAny(simple_expr) => Countable::Subquery(
                    Box::new(Query::select().expr(*simple_expr.clone()).take()),
                    aggregate,
                ),
                _ => return Err(ParseError::Todo("Uncountable variable")),
            },
            ExpressionInner::Access(x) => Countable::Access(x.clone(), aggregate),
            _ => return Err(ParseError::Todo("Uncountable expression")),
        })))
    }
}

impl Function for Count {}

impl ToSql for Count {
    fn returntype(&self, _tp: &Transpiler) -> Type {
        match &self.0 {
            Countable::Subquery(_, x) if !x.is_empty() => {
                RecordSet(Default::default(), true).into()
            }
            Countable::Access(_, x) if !x.is_empty() => RecordSet(Default::default(), true).into(),
            Countable::Fix(_) | Countable::Subquery(_, _) | Countable::Access(_, _) => {
                Type::Sql(SqlType::UInteger)
            }
        }
    }

    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match &self.0 {
            Countable::Fix(fix) => TypedExpression {
                ty: Type::Sql(SqlType::UInteger),
                expr: SimpleExpr::Constant(Value::BigInt(Some((*fix).try_into()?))),
            },
            Countable::Subquery(select_statement, aggregate) => subquery({
                let aggregate = aggregate
                    .iter()
                    .map(|x| Ok(x.to_sql(tp)?.expr))
                    .collect::<Result<Vec<_>>>()?;
                Query::select()
                    .from_subquery(*select_statement.clone(), alias("_"))
                    .add_group_by(aggregate.iter().cloned())
                    .exprs(aggregate.iter().cloned())
                    .expr(Func::count(ColumnRef::Asterisk))
                    .take()
            }),
            Countable::Access(chain, aggregate) => subquery({
                let aggregate = aggregate
                    .iter()
                    .map(|x| {
                        Ok(x.to_sql(
                            &tp.clone()
                                .enter_anonymous_table(chain.columns(tp).into_iter().collect()),
                        )?
                        .expr)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Query::select()
                    .from_subquery(
                        chain.to_record_set(tp, alias("_"))?,
                        chain.as_single_ident()?,
                    )
                    .add_group_by(aggregate.iter().cloned())
                    .exprs(aggregate.iter().cloned())
                    .expr(Func::count(ColumnRef::Asterisk))
                    .take()
            }),
        })
    }
}

#[cfg(test)]
mod test {
    use sea_query::PostgresQueryBuilder;

    use crate::{
        Transpiler,
        hacks::{get_plaintext_expression, postgres},
        structure::{Database, Schema, SqlLayout, Table},
        types::SqlType,
    };

    #[test]
    fn count_list() {
        let sql = postgres("[1, 2, 3].count()").unwrap();
        assert_eq!(sql, "3");
    }

    #[test]
    fn count_agg_list() {
        let error = postgres("[1, 2, 3].count(something)").unwrap_err();
        assert_eq!(error.to_string(), "TODO: Can't use agg in static count");
    }

    #[test]
    fn count_func() {
        let sql = postgres("[1, 2, 3].map(x, x).count()").unwrap();
        assert_eq!(sql, "3");

        let sql = postgres("[1, 2, 3].filter(x, x != 2).count()").unwrap();
        assert_eq!(sql, "2");
    }

    #[test]
    fn count_agg_single() {
        let tp = Transpiler::new()
            .layout(SqlLayout::new(Database::new().schema(
                Schema::new("postgres").table(Table::new("foo").column(("bar", SqlType::UInteger))),
            )))
            .enter_schema("postgres")
            .build()
            .unwrap();

        let sql = get_plaintext_expression("foo.count(bar)", &tp, PostgresQueryBuilder).unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "bar", COUNT(*) FROM (SELECT * FROM "foo") AS "foo" GROUP BY "bar")"#
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

        let sql =
            get_plaintext_expression("foo.count(bar_1, bar_2, bar_3)", &tp, PostgresQueryBuilder)
                .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "bar_1", "bar_2", "bar_3", COUNT(*) FROM (SELECT * FROM "foo") AS "foo" GROUP BY "bar_1", "bar_2", "bar_3")"#
        );
    }
}
