pub mod cel;
pub mod to_sql;

use crate::{
    Transpiler,
    functions::{Function, subquery},
    transpiler::{ParseError, alias},
    variables::{Atom, Object, Variable},
};
use cel_interpreter::{
    Value as CelValue,
    objects::{Key, Map},
};
use cel_parser::{ArithmeticOp, RelationOp, UnaryOp};
use sea_query::{
    BinOper, CaseStatement, ColumnRef, Query, SelectStatement, SimpleExpr, SubQueryStatement,
    Value,
    extension::postgres::{PgBinOper, PgExpr},
};
use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    sync::Arc,
};
pub use to_sql::ToSql;

#[cfg(not(feature = "thread-safe"))]
pub type Rc<T> = std::rc::Rc<T>;
#[cfg(feature = "thread-safe")]
pub type Rc<T> = std::arc::Arc<T>;

#[derive(Clone)]
pub enum Expression {
    Access(AccessChain),
    Index(Box<Expression>, i64),
    Variable(Variable),

    Arithmetic(Box<Expression>, ArithmeticOp, Box<Expression>),
    Relation(Box<Expression>, RelationOp, Box<Expression>),
    Ternary(Box<Expression>, Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Unary(UnaryOp, Box<Expression>),
    FunctionCall(Rc<dyn Function + 'static>),
}

#[derive(Clone, Debug)]
pub struct Ident(pub(crate) Arc<String>);

impl Ident {
    fn to_sql_string(&self) -> Value {
        Value::String(Some(Box::new((*self.0).clone())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

type Error = ParseError;

type Result<T> = std::result::Result<T, Error>;

impl Atom {
    pub fn to_sql(&self, _: &Transpiler) -> Result<SimpleExpr> {
        Ok(SimpleExpr::Constant(match self {
            Atom::Null => Value::Json(None),
            Atom::Bool(b) => Value::Bool(Some(*b)),
            Atom::String(s) => Value::String(Some(Box::new(s.to_string()))),
            Atom::Bytes(b) => Value::Bytes(Some(Box::new(b.as_ref().clone()))),
            Atom::Int(i) => Value::BigInt(Some(*i)),
            Atom::UInt(u) => Value::BigUnsigned(Some(*u)),
            Atom::Float(f) => Value::Double(Some(*f)),
        }))
    }
}

impl From<serde_json::Value> for Variable {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Variable::Atom(Atom::Null),
            serde_json::Value::Bool(bool) => Variable::Atom(Atom::Bool(bool)),
            serde_json::Value::Number(number) => {
                if let Some(i) = number.as_i64() {
                    Variable::Atom(Atom::Int(i))
                } else if let Some(f) = number.as_f64() {
                    Variable::Atom(Atom::Float(f))
                } else {
                    unimplemented!("What are you doing here")
                }
            }
            serde_json::Value::String(s) => Variable::Atom(Atom::String(s.into())),
            serde_json::Value::Array(values) => Variable::List(
                values
                    .into_iter()
                    .map(Into::into)
                    .map(Expression::Variable)
                    .collect(),
            ),
            serde_json::Value::Object(map) => Variable::Object(Object {
                data: map
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            Expression::Variable(Variable::Atom(Atom::String(k.into()))),
                            Expression::Variable(v.into()),
                        )
                    })
                    .collect(),
                schema: None,
            }),
        }
    }
}

impl ToIntermediate for serde_json::Value {
    fn to_sqcel(&self, _tp: &Transpiler) -> Result<Expression> {
        Ok(Expression::Variable(self.clone().into()))
    }
}

impl TryFrom<Variable> for Key {
    type Error = CantBeAValue;

    fn try_from(value: Variable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Variable::Atom(atom) => match atom {
                Atom::Bool(b) => Key::Bool(b),
                Atom::String(s) => Key::String(s),
                Atom::Int(i) => Key::Int(i),
                _ => return Err(CantBeAValue),
            },
            _ => return Err(CantBeAValue),
        })
    }
}

impl TryFrom<Expression> for Key {
    type Error = CantBeAValue;

    fn try_from(value: Expression) -> std::result::Result<Self, Self::Error> {
        match value {
            Expression::Variable(v) => v.try_into(),
            _ => Err(CantBeAValue),
        }
    }
}

pub struct CantBeAValue;

impl TryFrom<Variable> for CelValue {
    type Error = CantBeAValue;

    fn try_from(value: Variable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Variable::Object(Object { data, .. }) => CelValue::Map(Map {
                map: Arc::new(
                    data.into_iter()
                        .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                        .collect::<std::result::Result<_, _>>()?,
                ),
            }),
            Variable::List(expresions) => CelValue::List(Arc::new(
                expresions
                    .into_iter()
                    .map(|v| match v {
                        Expression::Variable(x) => x.try_into(),
                        _ => Err(CantBeAValue),
                    })
                    .collect::<std::result::Result<_, _>>()?,
            )),
            Variable::Atom(atom) => match atom {
                Atom::Null => CelValue::Null,
                Atom::Bool(b) => CelValue::Bool(b),
                Atom::String(s) => CelValue::String(s),
                Atom::Bytes(items) => CelValue::Bytes(items.clone()),
                Atom::Int(i) => CelValue::Int(i),
                Atom::UInt(u) => CelValue::UInt(u),
                Atom::Float(f) => CelValue::Float(f),
            },
            _ => return Err(CantBeAValue),
        })
    }
}

impl TryFrom<Expression> for CelValue {
    type Error = CantBeAValue;

    fn try_from(value: Expression) -> std::result::Result<Self, Self::Error> {
        match value {
            Expression::Variable(var) => var.try_into(),
            _ => Err(CantBeAValue),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccessChain {
    pub(crate) head: Option<Box<Expression>>,
    pub(crate) idents: Vec<Ident>,
}

impl ToSql for AccessChain {
    fn to_sql(&self, tp: &Transpiler) -> Result<SimpleExpr> {
        let (head, rest) = match (&self.head, self.idents.as_slice()) {
            (None, [schema, table, column, rest @ ..])
                if tp.schemas.contains_key(schema.as_str()) =>
            {
                (
                    tp.schemas.get(schema.as_str()).map(|schema_name| {
                        SimpleExpr::Column(ColumnRef::SchemaTableColumn(
                            alias(schema_name),
                            alias(table.as_str()),
                            alias(column.as_str()),
                        ))
                    }),
                    rest,
                )
            }
            (None, [table, column, rest @ ..]) if tp.tables.contains_key(table.as_str()) => (
                tp.tables.get(table.as_str()).map(|table_name| {
                    SimpleExpr::Column(ColumnRef::TableColumn(
                        alias(table_name),
                        alias(column.as_str()),
                    ))
                }),
                rest,
            ),
            (None, [column, rest @ ..]) if tp.columns.contains_key(column.as_str()) => (
                tp.columns
                    .get(column.as_str())
                    .map(|column_name| SimpleExpr::Column(ColumnRef::Column(alias(column_name)))),
                rest,
            ),
            (None, [var, rest @ ..]) if tp.variables.contains_key(var.as_str()) => (
                tp.variables
                    .get(var.as_str())
                    .map(|v| v.to_sql(tp))
                    .transpose()?,
                rest,
            ),
            (Some(head), rest) => (Some(head.to_sql(tp)?), rest),
            (None, _) => {
                return Err(Error::UnknownField(
                    "?".to_owned(),
                    self.to_path()
                        .map(Cow::into_owned)
                        .unwrap_or("?".to_owned()),
                ));
            }
        };

        let (rest, tail) = match rest {
            [rest @ .., tail] => (rest, Some(SimpleExpr::Constant(tail.to_sql_string()))),
            rest => (rest, None),
        };

        Self::chain_to_json_access(
            head.into_iter()
                .chain(rest.iter().map(|s| SimpleExpr::Constant(s.to_sql_string()))),
            tail,
        )
    }
}

impl AccessChain {
    fn new(chain: Vec<Ident>) -> Self {
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
        chain: impl IntoIterator<Item = SimpleExpr>,
        tail: Option<SimpleExpr>,
    ) -> Result<SimpleExpr> {
        let outer = chain
            .into_iter()
            .reduce(|a, b| a.get_json_field(b))
            .ok_or(Error::Todo("tried to create empty access chain"))?;

        Ok(match tail {
            Some(tail) => outer.cast_json_field(tail),
            None => outer,
        })
    }

    pub fn as_single_ident(&self) -> Result<&Ident> {
        match (self.head.as_ref(), self.idents.as_slice()) {
            (None, [single]) => Ok(single),
            _ => Err(Error::Todo("Invalid var name")),
        }
    }
}

impl ToSql for Expression {
    fn to_sql(&self, tp: &Transpiler) -> Result<SimpleExpr> {
        Ok(match self {
            Expression::Access(chain) => chain.to_sql(tp)?,
            Expression::Variable(variable) => variable.to_sql(tp)?,
            Expression::Arithmetic(a, op, b) => SimpleExpr::Binary(
                Box::new(a.to_sql(tp)?),
                match op {
                    ArithmeticOp::Add => BinOper::Add,
                    ArithmeticOp::Subtract => BinOper::Sub,
                    ArithmeticOp::Divide => BinOper::Div,
                    ArithmeticOp::Multiply => BinOper::Mul,
                    ArithmeticOp::Modulus => BinOper::Mod,
                },
                Box::new(b.to_sql(tp)?),
            ),
            Expression::Relation(a, op, b) => SimpleExpr::Binary(
                Box::new(match op {
                    RelationOp::Equals | RelationOp::NotEquals => a.to_json(tp)?.into(),
                    _ => a.to_sql(tp)?,
                }),
                match op {
                    RelationOp::LessThan => BinOper::SmallerThan,
                    RelationOp::LessThanEq => BinOper::SmallerThanOrEqual,
                    RelationOp::GreaterThan => BinOper::GreaterThan,
                    RelationOp::GreaterThanEq => BinOper::GreaterThanOrEqual,
                    RelationOp::Equals => BinOper::Equal,
                    RelationOp::NotEquals => BinOper::NotEqual,
                    RelationOp::In => {
                        return Err(ParseError::Todo("IN operator for lists, subqueries etc"));
                    }
                },
                Box::new(match op {
                    RelationOp::Equals | RelationOp::NotEquals => b.to_json(tp)?.into(),
                    _ => b.to_sql(tp)?,
                }),
            ),
            Expression::Ternary(r#if, then, r#else) => SimpleExpr::Case(Box::new(
                CaseStatement::new()
                    .case(r#if.to_sql(tp)?, then.to_sql(tp)?)
                    .finally(r#else.to_sql(tp)?),
            )),
            Expression::Or(a, b) => a.to_sql(tp)?.or(b.to_sql(tp)?),
            Expression::And(a, b) => a.to_sql(tp)?.and(b.to_sql(tp)?),
            Expression::Unary(unary_op, expresion) => match unary_op {
                UnaryOp::Not => expresion.to_sql(tp)?.not(),
                UnaryOp::DoubleNot => expresion.to_sql(tp)?,
                UnaryOp::Minus => SimpleExpr::Constant(match expresion.as_ref() {
                    Expression::Variable(Variable::Atom(a)) => match a {
                        Atom::Int(i) => Value::BigInt(Some(-i)),
                        Atom::UInt(u) => Value::BigInt(Some(
                            -((*u).try_into().or(Err(Error::Todo("UInt too large")))?),
                        )),
                        Atom::Float(f) => Value::Double(Some(-f)),
                        _ => return Err(Error::Todo("Can't be negative")),
                    },
                    _ => return Err(Error::Todo("Can't be negative")),
                }),
                UnaryOp::DoubleMinus => expresion.to_sql(tp)?,
            },
            Expression::FunctionCall(x) => x.to_sql(tp)?,
            Expression::Index(expression, index) => Self::index(tp, expression.as_ref(), *index)?,
        })
    }
}

impl Expression {
    fn index(tp: &Transpiler, ex: &Expression, index: i64) -> Result<SimpleExpr> {
        Ok(match ex {
            Expression::Variable(variable) => match variable {
                Variable::List(_) => ex
                    .to_sql(tp)?
                    .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(index)))),
                Variable::SqlSubQuery(select_statement) => subquery(
                    select_statement
                        .clone()
                        .offset(index as u64)
                        .limit(1)
                        .take(),
                ),

                Variable::SqlSubQueryAtom(select_statement) if index == 0 => {
                    subquery(*select_statement.clone())
                }
                Variable::SqlAny(simple_expr) => SimpleExpr::SubQuery(
                    None,
                    Box::new(SubQueryStatement::SelectStatement(
                        Query::select()
                            .expr(*simple_expr.clone())
                            .offset(index as u64)
                            .limit(1)
                            .take(),
                    )),
                ),
                _ => return Err(Error::Todo("Can not be accessed")),
            },
            Expression::Index(inner, index) => {
                let inner = Self::index(tp, inner, *index)?;
                match inner {
                    SimpleExpr::Binary(a, op, b) if op == PgBinOper::CastJsonField.into() => {
                        a.get_json_field(*b)
                    }
                    _ => inner,
                }
                .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(*index))))
            }
            _ => return Err(Error::Todo("Can't index this")),
        })
    }

    pub fn is_object(&self) -> bool {
        match self {
            Self::Variable(var) => var.is_object(),
            _ => false,
        }
    }

    pub(crate) fn as_postive_integer(&self) -> Result<u64> {
        match self {
            Expression::Variable(variable) => variable.as_postive_integer(),
            _ => Err(ParseError::Todo("Must be an integer")),
        }
    }

    pub fn as_single_ident(&self) -> Result<&Ident> {
        match self {
            Self::Access(a) => a.as_single_ident(),
            _ => Err(Error::Todo("Not an ident")),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match self {
            Self::Variable(Variable::Atom(Atom::String(s))) => Ok(s),
            _ => Err(Error::Todo("Not a str")),
        }
    }

    pub(crate) fn to_record_set(&self, tp: &Transpiler, alias: &str) -> Result<SelectStatement> {
        match self {
            Self::Variable(var) => var.to_record_set(tp, alias),
            _ => Err(Error::Todo("Can't be a record set")),
        }
    }
}

impl Debug for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Access(arg0) => f.debug_tuple("Access").field(arg0).finish(),
            Self::Index(arg0, arg1) => f.debug_tuple("Index").field(arg0).field(arg1).finish(),
            Self::Variable(arg0) => f.debug_tuple("Variable").field(arg0).finish(),
            Self::Arithmetic(arg0, arg1, arg2) => f
                .debug_tuple("Arithmetic")
                .field(arg0)
                .field(arg1)
                .field(arg2)
                .finish(),
            Self::Relation(arg0, arg1, arg2) => f
                .debug_tuple("Relation")
                .field(arg0)
                .field(arg1)
                .field(arg2)
                .finish(),
            Self::Ternary(arg0, arg1, arg2) => f
                .debug_tuple("Ternary")
                .field(arg0)
                .field(arg1)
                .field(arg2)
                .finish(),
            Self::Or(arg0, arg1) => f.debug_tuple("Or").field(arg0).field(arg1).finish(),
            Self::And(arg0, arg1) => f.debug_tuple("And").field(arg0).field(arg1).finish(),
            Self::Unary(arg0, arg1) => f.debug_tuple("Unary").field(arg0).field(arg1).finish(),
            Self::FunctionCall(v) => f
                .debug_tuple("FunctionCall")
                .field(&v.returntype())
                .finish_non_exhaustive(),
        }
    }
}

pub trait ToIntermediate {
    fn to_sqcel(&self, tp: &Transpiler) -> Result<Expression>;
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{
        Transpiler,
        intermediate::ToSql,
        transpiler::{TranspilerBuilder, alias},
    };
    use sea_query::{PostgresQueryBuilder, Query, SimpleExpr};
    use serde_json::json;

    use super::{AccessChain, Ident};

    fn i(i: &str) -> Ident {
        Ident(Arc::new(i.to_owned()))
    }

    #[test]
    fn chain_single_column() {
        let chain = AccessChain::new(vec![i("a")])
            .to_sql(&TranspilerBuilder::default().column("a").build())
            .unwrap();

        assert_eq!(
            chain,
            SimpleExpr::Column(sea_query::ColumnRef::Column(alias("a")))
        )
    }

    #[test]
    fn show() {
        // let x = AccessChain::new(vec![i("a"), i("b"), i("c"), i("d")])
        //     .to_sql(&TranspilerBuilder::default().column("a").build())
        //     .unwrap();

        let x = Transpiler::new()
            .var("a", json!({"test": 7}))
            .build()
            .transpile(r#"{"bar": a}"#)
            .unwrap();

        println!(
            "{}",
            Query::select().expr(x).to_string(PostgresQueryBuilder)
        )
    }
}
