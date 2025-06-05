use super::{Function, subquery};
use crate::{
    Transpiler,
    intermediate::{Expression, ExpressionInner, Rc, ToSql},
    transpiler::{ParseError, Result, alias},
    types::{SqlType, Type, TypedExpression},
};
use sea_query::{ColumnRef, Func, Query, SelectStatement, SimpleExpr, Value};

enum Countable {
    /// This is a fixed size list, its length is fix
    Fix(usize),
    Subquery(Box<SelectStatement>),
}

pub struct Count(Countable);

impl Count {
    pub fn new(arg: &Expression) -> Result<Rc<Self>> {
        Ok(Rc::new(Count(match &**arg {
            ExpressionInner::Variable(v) => match v {
                crate::variables::Variable::List(expressions) => Countable::Fix(expressions.len()),
                crate::variables::Variable::SqlSubQueryAtom(_) => Countable::Fix(1),
                crate::variables::Variable::SqlSubQuery(select_statement) => {
                    Countable::Subquery(select_statement.clone())
                }
                crate::variables::Variable::SqlAny(simple_expr) => {
                    Countable::Subquery(Box::new(Query::select().expr(*simple_expr.clone()).take()))
                }
                _ => return Err(ParseError::Todo("Uncountable variable")),
            },
            // TODO: Count subquery here?
            _ => return Err(ParseError::Todo("Uncountable expression")),
        })))
    }
}

impl Function for Count {
    fn returntype(&self) -> Type {
        Type::Sql(SqlType::UInteger)
    }
}

impl ToSql for Count {
    fn to_sql(&self, _tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match &self.0 {
            Countable::Fix(fix) => TypedExpression {
                ty: Type::Sql(SqlType::UInteger),
                expr: SimpleExpr::Constant(Value::BigInt(Some(*fix as i64))),
            },
            Countable::Subquery(select_statement) => subquery(
                Query::select()
                    .expr(Func::count(ColumnRef::Asterisk))
                    .from_subquery(*select_statement.clone(), alias("_"))
                    .take(),
            ),
        })
    }
}
