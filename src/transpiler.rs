use cel_parser::{ArithmeticOp, Atom, Expression as CelExpr, Member, RelationOp, UnaryOp};
use sea_query::{Alias, BinOper, CaseStatement, Func, SimpleExpr as SqlExpr, Value};
use std::sync::Arc;
use thiserror::Error;

pub struct Transpiler;

impl Transpiler {
    pub fn new() -> Self {
        Self
    }

    pub fn transpile<T>(&self, src: &str) -> Result<SqlExpr> {
        Ok(cel_parser::parse(src)?.to_sql(self)?)
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error(transparent)]
    CelError(#[from] cel_parser::error::ParseError),
    #[error("A non ident function name was supplied")]
    NonIdentFunctionName,
}

pub type Result<T> = std::result::Result<T, ParseError>;

trait ToSql<E> {
    fn to_sql(self, tp: &Transpiler) -> Result<E>;
}

impl ToSql<SqlExpr> for CelExpr {
    fn to_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            CelExpr::Arithmetic(lhs, op, rhs) => {
                lhs.to_sql(tp)?.binary(op.to_sql(tp)?, rhs.to_sql(tp)?)
            }
            CelExpr::Relation(lhs, op, rhs) => {
                lhs.to_sql(tp)?.binary(op.to_sql(tp)?, rhs.to_sql(tp)?)
            }
            CelExpr::Ternary(r#if, then, r#else) => SqlExpr::Case(Box::new(
                CaseStatement::new()
                    .case(r#if.to_sql(tp)?, then.to_sql(tp)?)
                    .finally(r#else.to_sql(tp)?),
            )),
            CelExpr::Or(a, b) => a.to_sql(tp)?.or(b.to_sql(tp)?),
            CelExpr::And(a, b) => a.to_sql(tp)?.add(b.to_sql(tp)?),
            CelExpr::Unary(unary_op, expression) => (unary_op, *expression).to_sql(tp)?,
            CelExpr::Member(expression, member) => (*expression, *member).to_sql(tp)?,
            CelExpr::FunctionCall(ident, receiver, args) => {
                function_call(ident, receiver, args, tp)?
            }
            CelExpr::List(items) => list(items, tp)?,
            CelExpr::Map(items) => map(items, tp)?,
            CelExpr::Atom(atom) => atom.to_sql(tp)?,
            CelExpr::Ident(s) => {
                SqlExpr::Value(sea_query::Value::String(Some(Box::new((*s).clone()))))
            }
        })
    }
}

fn list(items: Vec<CelExpr>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_list")).args(
            items
                .into_iter()
                .map(|x| x.to_sql(tp))
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

fn map(items: Vec<(CelExpr, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_object")).args(
            items
                .into_iter()
                .flat_map(|(a, b)| [a.to_sql(tp), b.to_sql(tp)])
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

fn obj(items: Vec<(Arc<String>, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_object")).args(
            items
                .into_iter()
                .flat_map(|(a, b)| {
                    [
                        Ok(SqlExpr::Constant(Value::String(Some(Box::new(
                            (*a).clone(),
                        ))))),
                        b.to_sql(tp),
                    ]
                })
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

fn function_call(
    ident: Box<CelExpr>,
    receiver: Option<Box<CelExpr>>,
    args: Vec<CelExpr>,
    tp: &Transpiler,
) -> Result<SqlExpr> {
    match *ident {
        // Clone makes me sad ;(
        CelExpr::Ident(i) => match (i.as_str(), receiver, args.as_slice()) {
            ("int", _, [exp, ..]) => json_cast("integer", exp.clone(), tp),
            ("str", _, [exp, ..]) => json_cast("text", exp.clone(), tp),
            ("bool", _, [exp, ..]) => json_cast("boolean", exp.clone(), tp),
            ("float", _, [exp, ..]) => json_cast("double", exp.clone(), tp),
            (name, receiver, _) => Ok(SqlExpr::FunctionCall(
                Func::cust(Alias::new(name)).args(
                    receiver
                        .map(|x| *x)
                        .into_iter()
                        .chain(args)
                        .map(|x| x.to_sql(tp))
                        .collect::<Result<Vec<_>>>()?,
                ),
            )),
        },
        _ => Err(ParseError::NonIdentFunctionName),
    }
}

fn json_cast(ty: &str, exp: CelExpr, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(
        SqlExpr::FunctionCall(Func::cust(Alias::new("nullif")).arg(exp.to_sql(tp)?).arg(
            SqlExpr::Constant(Value::String(Some(Box::new("null".to_owned())))),
        ))
        .cast_as(Alias::new(ty)),
    )
}

impl ToSql<BinOper> for ArithmeticOp {
    fn to_sql(self, tp: &Transpiler) -> Result<BinOper> {
        Ok(match self {
            ArithmeticOp::Add => BinOper::Add,
            ArithmeticOp::Subtract => BinOper::Sub,
            ArithmeticOp::Divide => BinOper::Div,
            ArithmeticOp::Multiply => BinOper::Mul,
            ArithmeticOp::Modulus => BinOper::Mod,
        })
    }
}

impl ToSql<BinOper> for RelationOp {
    fn to_sql(self, tp: &Transpiler) -> Result<BinOper> {
        Ok(match self {
            RelationOp::LessThan => BinOper::SmallerThan,
            RelationOp::LessThanEq => BinOper::SmallerThanOrEqual,
            RelationOp::GreaterThan => BinOper::GreaterThan,
            RelationOp::GreaterThanEq => BinOper::GreaterThanOrEqual,
            RelationOp::Equals => BinOper::Equal,
            RelationOp::NotEquals => BinOper::NotEqual,
            RelationOp::In => BinOper::In,
        })
    }
}

impl ToSql<SqlExpr> for Atom {
    fn to_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(SqlExpr::Constant(match self {
            Atom::Int(num) => Value::BigInt(Some(num)),
            Atom::UInt(num) => Value::BigUnsigned(Some(num)),
            Atom::Float(f) => Value::Double(Some(f)),
            Atom::String(s) => Value::String(Some(Box::new((*s).clone()))),
            Atom::Bytes(items) => Value::Bytes(Some(Box::new((*items).clone()))),
            Atom::Bool(b) => Value::Bool(Some(b)),
            Atom::Null => Value::Int(None),
        }))
    }
}

impl ToSql<SqlExpr> for (UnaryOp, CelExpr) {
    fn to_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            (UnaryOp::DoubleMinus | UnaryOp::DoubleNot, x) => x.to_sql(tp)?,
            (UnaryOp::Not, x) => x.to_sql(tp)?.not(),
            (UnaryOp::Minus, x) => SqlExpr::Constant(Value::BigInt(Some(0))).sub(x.to_sql(tp)?),
        })
    }
}

impl ToSql<SqlExpr> for (CelExpr, Member) {
    fn to_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            (CelExpr::Ident(_), Member::Fields(items)) => obj(items, tp)?,
            (receiver, field) => SqlExpr::Binary(
                Box::new(receiver.to_sql(tp)?),
                BinOper::Custom("->"),
                Box::new(field.to_sql(tp)?),
            ),
        })
    }
}

impl ToSql<SqlExpr> for Member {
    fn to_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            Member::Attribute(iden) => {
                SqlExpr::Constant(Value::String(Some(Box::new((*iden).to_owned()))))
            }
            Member::Index(expression) => expression.to_sql(tp)?,
            Member::Fields(items) => obj(items, tp)?,
        })
    }
}

#[cfg(test)]
mod test {
    use sea_query::{PostgresQueryBuilder, Query};

    use crate::transpiler::Transpiler;

    use super::ToSql;
    #[test]
    fn test() {
        let x = "int({bar: 5}.bar) + 1 == -3 ? 10 : 5";
        println!("{x}");
        let x = cel_parser::parse(x).unwrap().to_sql(&Transpiler).unwrap();
        let x = Query::select().expr(x).build(PostgresQueryBuilder);
        println!("{}", x.0);
    }

    #[test]
    fn funtest() {
        dbg!(cel_parser::parse(r#"M{}.value"#).unwrap());
    }
}
