mod count;
mod last;
mod map;

use crate::{
    Transpiler,
    intermediate::{Expression, Rc, ToSql},
    transpiler::{ParseError, Result, alias},
};
use count::Count;
use last::Last;
use map::Map;
use sea_query::{Query, SelectStatement, SimpleExpr as SqlExpression, SubQueryStatement};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum FunctionReturn {
    Object,
    Array,
    Atom,
    Json,
    SubqueryList,
    SubqueryAtom,
    Unknown,
}

pub trait Function: ToSql {
    fn returntype(&self) -> FunctionReturn;
}

pub fn get(
    tp: &Transpiler,
    name: Expression,
    rec: Option<Expression>,
    args: Vec<Expression>,
) -> Result<Rc<dyn Function>> {
    if let Expression::Access(name) = name {
        Ok(
            match (
                name.to_path()
                    .ok_or(ParseError::Todo("Function paths can't be empty"))?
                    .as_ref(),
                rec.as_ref(),
                args.as_slice(),
            ) {
                ("int", None, [x]) => json_cast("integer", x, tp)?,
                ("string", None, [x]) => json_cast("text", x, tp)?,
                ("bool", None, [x]) => json_cast("boolean", x, tp)?,
                ("double", None, [x]) => json_cast("double precision", x, tp)?,

                ("count", None, [x]) => Count::new(x)?,
                ("count", Some(x), []) => Count::new(x)?,

                ("map", Some(rec), [var, func]) => Map::new(rec, var, None, func)?,
                ("map", Some(rec), [var, filter, func]) => Map::new(rec, var, Some(filter), func)?,
                // Implement .filter(x, pred) as .map(x, pred, x)
                ("filter", Some(rec), [var, filter]) => Map::new(rec, var, Some(filter), var)?,

                ("last", None, [amount]) => Last::new(Some(amount), None)?,
                ("last", None, [amount, predicate]) => Last::new(Some(amount), Some(predicate))?,
                _ => return Err(ParseError::Todo("Unknown function or arg configuration")),
            },
        )
    } else {
        Err(ParseError::Todo("Only idents can be function names"))
    }
}

struct Cast(Expression, &'static str);
impl Function for Cast {
    fn returntype(&self) -> FunctionReturn {
        FunctionReturn::Atom
    }
}

impl ToSql for Cast {
    fn to_sql(&self, tp: &Transpiler) -> Result<SqlExpression> {
        Ok(self.0.to_sql(tp)?.cast_as(alias(self.1)))
    }
}

#[inline]
fn json_cast(ty: &'static str, exp: &Expression, _: &Transpiler) -> Result<Rc<dyn Function>> {
    Ok(Rc::new(Cast(exp.clone(), ty)))
}

pub fn to_select(expr: SqlExpression) -> SelectStatement {
    match expr {
        SqlExpression::SubQuery(_, boxed) => match *boxed {
            SubQueryStatement::SelectStatement(select_statement) => select_statement,
            _ => todo!(),
        },
        x => Query::select().expr(x).take(),
    }
}

pub fn subquery(s: SelectStatement) -> SqlExpression {
    SqlExpression::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(s)))
}
