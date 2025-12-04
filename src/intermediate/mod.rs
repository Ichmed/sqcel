pub mod cel;
pub mod to_sql;

use crate::{
    Result, Transpiler,
    functions::{Function, subquery},
    transpiler::{ParseError, alias},
    types::{JsonType, RecordSet, SqlType, Type, TypedExpression},
    variables::{Atom, Object, Variable},
};
use cel_interpreter::{
    Value as CelValue,
    objects::{Key, Map},
};
use cel_parser::{ArithmeticOp, RelationOp, UnaryOp};
use sea_query::{
    Asterisk, BinOper, CaseStatement, IntoIden, Query, SelectStatement, SimpleExpr,
    SubQueryStatement, TableRef, Value,
    extension::postgres::{PgBinOper, PgExpr},
};
use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    iter::once,
    sync::Arc,
};
pub use to_sql::ToSql;

#[cfg(not(feature = "thread-safe"))]
pub type Rc<T> = std::rc::Rc<T>;
#[cfg(feature = "thread-safe")]
pub type Rc<T> = std::sync::Arc<T>;

#[derive(Clone, Debug)]
pub struct Expression {
    inner: Rc<ExpressionInner>,
}

impl std::ops::Deref for Expression {
    type Target = ExpressionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub enum ExpressionInner {
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

impl ExpressionInner {
    #[must_use]
    pub fn into_anonymous(self) -> Expression {
        Expression {
            inner: Rc::new(self),
        }
    }
}

impl Expression {
    #[must_use]
    #[allow(clippy::match_same_arms, reason = "Nicer formatting")]
    pub fn returntype(&self, tp: &Transpiler) -> Type {
        match &*self.inner {
            ExpressionInner::Access(access_chain) => access_chain.returntype(tp),
            ExpressionInner::Index(expression, _) => expression.returntype(tp).inner(),
            ExpressionInner::Variable(variable) => variable.returntype(tp),
            ExpressionInner::Arithmetic(_, _, _) => SqlType::Number.into(),
            ExpressionInner::Relation(_, _, _) => SqlType::Boolean.into(),
            ExpressionInner::Ternary(_, expression1, _) => expression1.returntype(tp),
            ExpressionInner::Or(_, _) => SqlType::Boolean.into(),
            ExpressionInner::And(_, _) => SqlType::Boolean.into(),
            ExpressionInner::Unary(_, expression) => expression.returntype(tp),
            ExpressionInner::FunctionCall(function) => function.returntype(tp),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ident(pub(crate) Arc<String>);

impl Ident {
    fn to_sql_string(&self) -> Value {
        (*self.0).clone().into()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl IntoIden for Ident {
    fn into_iden(self) -> sea_query::DynIden {
        alias(self.0)
    }
}

impl IntoIden for &Ident {
    fn into_iden(self) -> sea_query::DynIden {
        alias(self.0.to_string())
    }
}

impl Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

type Error = ParseError;

impl ToSql for Atom {
    fn to_sql(&self, _: &Transpiler) -> Result<TypedExpression> {
        Ok(match self {
            Self::Null => TypedExpression {
                expr: SimpleExpr::Constant(Value::Json(None)),
                ty: Type::Null,
            },
            Self::Bool(b) => TypedExpression {
                ty: Type::Sql(SqlType::Boolean),
                expr: SimpleExpr::Constant(Value::Bool(Some(*b))),
            },
            Self::String(s) => TypedExpression {
                ty: Type::Sql(SqlType::String),
                expr: SimpleExpr::Constant(Value::String(Some(Box::new(s.to_string())))),
            },
            Self::Bytes(b) => TypedExpression {
                ty: Type::Sql(SqlType::Bytes),
                expr: SimpleExpr::Constant(Value::Bytes(Some(Box::new(b.as_ref().clone())))),
            },
            Self::Int(i) => TypedExpression {
                ty: Type::Sql(SqlType::Integer),
                expr: SimpleExpr::Constant(Value::BigInt(Some(*i))),
            },
            Self::UInt(u) => TypedExpression {
                ty: Type::Sql(SqlType::UInteger),
                expr: SimpleExpr::Constant(Value::BigUnsigned(Some(*u))),
            },
            Self::Float(f) => TypedExpression {
                ty: Type::Sql(SqlType::Float),
                expr: SimpleExpr::Constant(Value::Double(Some(*f))),
            },
        })
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        match self {
            Self::Null => Type::Null,
            Self::Bool(_) => SqlType::Boolean.into(),
            Self::String(_) => SqlType::String.into(),
            Self::Bytes(_) => SqlType::Bytes.into(),
            Self::Int(_) => SqlType::Integer.into(),
            Self::UInt(_) => SqlType::UInteger.into(),
            Self::Float(_) => SqlType::Float.into(),
        }
    }
}

impl From<serde_json::Value> for Variable {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Atom(Atom::Null),
            serde_json::Value::Bool(bool) => Self::Atom(Atom::Bool(bool)),
            serde_json::Value::Number(number) => {
                if let Some(i) = number.as_i64() {
                    Self::Atom(Atom::Int(i))
                } else if let Some(f) = number.as_f64() {
                    Self::Atom(Atom::Float(f))
                } else {
                    unimplemented!("What are you doing here")
                }
            }
            serde_json::Value::String(s) => Self::Atom(Atom::String(s.into())),
            serde_json::Value::Array(values) => Self::List(
                values
                    .into_iter()
                    .map(Into::into)
                    .map(ExpressionInner::Variable)
                    .map(ExpressionInner::into_anonymous)
                    .collect(),
            ),
            serde_json::Value::Object(map) => Self::Object(Object {
                data: map
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            ExpressionInner::Variable(Self::Atom(Atom::String(k.into())))
                                .into_anonymous(),
                            ExpressionInner::Variable(v.into()).into_anonymous(),
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
        Ok(ExpressionInner::Variable(self.clone().into()).into_anonymous())
    }
}

impl TryFrom<Variable> for Key {
    type Error = CantBeAValue;

    fn try_from(value: Variable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Variable::Atom(atom) => match atom {
                Atom::Bool(b) => Self::Bool(b),
                Atom::String(s) => Self::String(s),
                Atom::Int(i) => Self::Int(i),
                _ => return Err(CantBeAValue),
            },
            _ => return Err(CantBeAValue),
        })
    }
}

impl TryFrom<Expression> for Key {
    type Error = CantBeAValue;

    fn try_from(value: Expression) -> std::result::Result<Self, Self::Error> {
        match &*value.inner {
            ExpressionInner::Variable(v) => v.clone().try_into(),
            _ => Err(CantBeAValue),
        }
    }
}

pub struct CantBeAValue;

impl TryFrom<Variable> for CelValue {
    type Error = CantBeAValue;

    fn try_from(value: Variable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Variable::Object(Object { data, .. }) => Self::Map(Map {
                map: Arc::new(
                    data.into_iter()
                        .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                        .collect::<std::result::Result<_, _>>()?,
                ),
            }),
            Variable::List(expresions) => Self::List(Arc::new(
                expresions
                    .into_iter()
                    .map(|v| match &*v.inner {
                        ExpressionInner::Variable(x) => x.clone().try_into(),
                        _ => Err(CantBeAValue),
                    })
                    .collect::<std::result::Result<_, _>>()?,
            )),
            Variable::Atom(atom) => match atom {
                Atom::Null => Self::Null,
                Atom::Bool(b) => Self::Bool(b),
                Atom::String(s) => Self::String(s),
                Atom::Bytes(items) => Self::Bytes(items),
                Atom::Int(i) => Self::Int(i),
                Atom::UInt(u) => Self::UInt(u),
                Atom::Float(f) => Self::Float(f),
            },
            _ => return Err(CantBeAValue),
        })
    }
}

impl TryFrom<Expression> for CelValue {
    type Error = CantBeAValue;

    fn try_from(value: Expression) -> std::result::Result<Self, Self::Error> {
        match &*value.inner {
            ExpressionInner::Variable(var) => var.clone().try_into(),
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
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        let (head, rest) = match (&self.head, self.idents.as_slice()) {
            (None, [schema, table, column, rest @ ..])
                if tp.layout.column([schema, table, column]).is_some() =>
            {
                let (r, t) = tp.layout.column([schema, table, column]).unwrap();
                (
                    SimpleExpr::Column(r).assume(tp, t.unwrap_or_default())?,
                    rest,
                )
            }
            (None, [table, column, rest @ ..]) if tp.layout.column([table, column]).is_some() => {
                let (r, t) = tp.layout.column([table, column]).unwrap();
                (
                    SimpleExpr::Column(r).assume(tp, t.unwrap_or_default())?,
                    rest,
                )
            }
            (None, [column, rest @ ..]) if tp.layout.column([column]).is_some() => {
                let (r, t) = tp.layout.column([column]).unwrap();
                (
                    SimpleExpr::Column(r).assume(tp, t.unwrap_or_default())?,
                    rest,
                )
            }
            (None, [table, rest @ ..]) if tp.layout.table_asterisk([table]).is_some() => {
                let r = tp.layout.table_asterisk([table]).unwrap();
                let types = tp.layout.table_columns([table]).unwrap();
                (
                    SimpleExpr::Column(r)
                        .assume(tp, Type::RecordSet(Box::new(RecordSet(types, true))))?,
                    rest,
                )
            }
            (None, [var, rest @ ..]) if tp.variables.contains_key(var.as_str()) => {
                (tp.variables.get(var.as_str()).unwrap().to_sql(tp)?, rest)
            }
            (Some(head), rest) => (head.to_sql(tp)?, rest),
            (None, rest) => {
                return Err(Error::UnknownField(
                    rest.iter()
                        .map(|x| x.as_str().to_owned())
                        .collect::<Vec<_>>()
                        .join("."),
                    self.to_path()
                        .map_or_else(|| "?".to_owned(), Cow::into_owned),
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

    fn to_record_set(&self, tp: &Transpiler, alias: sea_query::DynIden) -> Result<SelectStatement> {
        match (&self.head, self.idents.as_slice()) {
            (None, [schema, table, column])
                if tp.layout.column([schema, table, column]).is_some() =>
            {
                let (r, _) = tp.layout.column([schema, table, column]).unwrap();

                Ok(Query::select().column(r).take())
            }
            (None, [table, column]) if tp.layout.column([table, column]).is_some() => {
                let (r, _) = tp.layout.column([table, column]).unwrap();
                Ok(Query::select().column(r).take())
            }
            (None, [column]) if tp.layout.column([column]).is_some() => {
                let (r, _) = tp.layout.column([column]).unwrap();
                Ok(Query::select().column(r).take())
            }
            (None, [table]) if tp.layout.table([table]).is_some() => {
                let r = tp.layout.table([table]).unwrap();

                Ok(Query::select().column(Asterisk).from(r).take())
            }
            (None, [var]) if tp.variables.contains_key(var.as_str()) => tp
                .variables
                .get(var.as_str())
                .unwrap()
                .to_record_set(tp, &alias.to_string()),
            (Some(_), _) => Ok(Query::select().expr_as(self.to_sql(tp)?.expr, alias).take()),
            (None, rest) => Err(Error::UnknownField(
                rest.iter()
                    .map(|x| x.as_str().to_owned())
                    .collect::<Vec<_>>()
                    .join("."),
                self.to_path()
                    .map_or_else(|| "?".to_owned(), Cow::into_owned),
            )),
        }
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        match (&self.head, self.idents.as_slice()) {
            (None, [column, ..]) if tp.layout.column([column]).is_some() => {
                tp.layout.column([column]).unwrap().1.unwrap_or_default()
            }
            (None, [table, ..]) if tp.layout.table_asterisk([table]).is_some() => {
                let types = tp.layout.table_columns([table]);
                Type::RecordSet(Box::new(RecordSet(types.unwrap(), true)))
            }
            (None, [table, ..]) if tp.variables.contains_key(table.as_str()) => {
                match tp.variables.get(table.as_str()).unwrap() {
                    Variable::SqlSubQuery(_, hash_map) | Variable::SqlSubQueryAtom(_, hash_map) => {
                        Type::RecordSet(Box::new(RecordSet(hash_map.clone(), true)))
                    }
                    Variable::Object(_) => JsonType::Map.into(),
                    Variable::List(_) => JsonType::List.into(),
                    Variable::Atom(atom) => atom.returntype(tp),
                    Variable::SqlAny(_) => Type::Unknown,
                }
            }
            _ => Type::Unknown,
        }
    }
}

impl AccessChain {
    const fn new(chain: Vec<Ident>) -> Self {
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
                ty: JsonType::Any.into(),
                expr: a.expr.get_json_field(b.expr),
            })
            .ok_or(Error::Todo("tried to create empty access chain"))?;

        Ok(match tail {
            Some(tail) => TypedExpression {
                ty: SqlType::JSON.into(),
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
            .ok_or(ParseError::Todo("Unknown table"))
    }
}

impl ToSql for Expression {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match &*self.inner {
            ExpressionInner::Access(chain) => chain.to_sql(tp)?,
            ExpressionInner::Variable(variable) => variable.to_sql(tp)?,
            ExpressionInner::Arithmetic(a, op, b) => TypedExpression {
                ty: Type::Sql(SqlType::Number),
                expr: SimpleExpr::Binary(
                    Box::new(a.to_sql(tp)?.expr),
                    match op {
                        ArithmeticOp::Add => BinOper::Add,
                        ArithmeticOp::Subtract => BinOper::Sub,
                        ArithmeticOp::Divide => BinOper::Div,
                        ArithmeticOp::Multiply => BinOper::Mul,
                        ArithmeticOp::Modulus => BinOper::Mod,
                    },
                    Box::new(b.to_sql(tp)?.expr),
                ),
            },
            ExpressionInner::Relation(a, op, b) => TypedExpression {
                ty: Type::Sql(SqlType::Boolean),
                expr: SimpleExpr::Binary(
                    Box::new(a.to_sql(tp)?.expr),
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
                    Box::new(b.to_sql(tp)?.expr),
                ),
            },
            ExpressionInner::Ternary(r#if, then, r#else) => TypedExpression {
                ty: Type::Sql(SqlType::Boolean),
                expr: SimpleExpr::Case(Box::new(
                    CaseStatement::new()
                        .case(r#if.to_sql(tp)?.expr, then.to_sql(tp)?.expr)
                        .finally(r#else.to_sql(tp)?.expr),
                )),
            },
            ExpressionInner::Or(a, b) => TypedExpression {
                ty: Type::Sql(SqlType::Boolean),
                expr: a.to_sql(tp)?.expr.or(b.to_sql(tp)?.expr),
            },
            ExpressionInner::And(a, b) => TypedExpression {
                ty: Type::Sql(SqlType::Boolean),
                expr: a.to_sql(tp)?.expr.and(b.to_sql(tp)?.expr),
            },
            ExpressionInner::Unary(unary_op, expresion) => TypedExpression {
                ty: Type::Unknown,
                expr: match unary_op {
                    UnaryOp::Not => expresion.to_sql(tp)?.expr.not(),
                    UnaryOp::Minus => SimpleExpr::Constant(match &*expresion.as_ref().inner {
                        ExpressionInner::Variable(Variable::Atom(a)) => match a {
                            Atom::Int(i) => Value::BigInt(Some(-i)),
                            Atom::UInt(u) => Value::BigInt(Some(
                                -((*u).try_into().or(Err(Error::Todo("UInt too large")))?),
                            )),
                            Atom::Float(f) => Value::Double(Some(-f)),
                            _ => return Err(Error::Todo("Can't be negative")),
                        },
                        _ => return Err(Error::Todo("Can't be negative")),
                    }),
                    UnaryOp::DoubleNot | UnaryOp::DoubleMinus => expresion.to_sql(tp)?.expr,
                },
            },
            ExpressionInner::FunctionCall(x) => x.to_sql(tp)?,
            ExpressionInner::Index(expression, index) => {
                Self::index(tp, expression.as_ref(), *index)?
            }
        })
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        Type::Unknown
    }
}

impl Expression {
    fn index(tp: &Transpiler, ex: &Self, index: i64) -> Result<TypedExpression> {
        Ok(match &*ex.inner {
            ExpressionInner::Variable(variable) => match variable {
                Variable::List(_) => TypedExpression {
                    ty: Type::Unknown,
                    expr: ex
                        .to_sql(tp)?
                        .expr
                        .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(index)))),
                },
                Variable::SqlSubQuery(select_statement, _) => subquery(
                    select_statement
                        .clone()
                        .offset(index.try_into()?)
                        .limit(1)
                        .take(),
                ),

                Variable::SqlSubQueryAtom(select_statement, _) if index == 0 => {
                    subquery(*select_statement.clone())
                }
                Variable::SqlAny(simple_expr) => TypedExpression {
                    ty: Type::Unknown,
                    expr: SimpleExpr::SubQuery(
                        None,
                        Box::new(SubQueryStatement::SelectStatement(
                            Query::select()
                                .expr(*simple_expr.clone())
                                .offset(index.try_into()?)
                                .limit(1)
                                .take(),
                        )),
                    ),
                },
                _ => return Err(Error::Todo("Can not be accessed")),
            },
            ExpressionInner::Index(inner, inner_index) => {
                let inner = Self::index(tp, inner, *inner_index)?;
                TypedExpression {
                    ty: Type::Unknown,
                    expr: match inner.expr {
                        // Chane ->> to -> for inner expression
                        SimpleExpr::Binary(a, op, b) if op == PgBinOper::CastJsonField.into() => {
                            a.get_json_field(*b)
                        }
                        x => x,
                    }
                    .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(index)))),
                }
            }
            ExpressionInner::Access(acc) => {
                let inner = acc.to_sql(tp)?;
                TypedExpression {
                    ty: Type::Unknown,
                    expr: match inner.expr {
                        // Chane ->> to -> for inner expression
                        SimpleExpr::Binary(a, op, b) if op == PgBinOper::CastJsonField.into() => {
                            a.get_json_field(*b)
                        }
                        x => x,
                    }
                    .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(index)))),
                }
            }
            _ => TypedExpression {
                ty: Type::Unknown,
                expr: ex
                    .to_sql(tp)?
                    .expr
                    .cast_json_field(SimpleExpr::Constant(Value::BigInt(Some(index)))),
            },
        })
    }

    #[must_use]
    pub fn is_object(&self) -> bool {
        match &*self.inner {
            ExpressionInner::Variable(var) => var.is_object(),
            _ => false,
        }
    }

    pub(crate) fn as_postive_integer(&self) -> Result<u64> {
        match &*self.inner {
            ExpressionInner::Variable(variable) => variable.as_postive_integer(),
            _ => Err(ParseError::Todo("Must be an integer")),
        }
    }

    pub fn as_single_ident(&self) -> Result<&Ident> {
        match &*self.inner {
            ExpressionInner::Access(a) => a.as_single_ident(),
            _ => Err(Error::Todo("Not an ident")),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match &*self.inner {
            ExpressionInner::Variable(Variable::Atom(Atom::String(s))) => Ok(s),
            _ => Err(Error::Todo("Not a str")),
        }
    }

    #[must_use]
    pub fn as_variable(&self) -> Option<&Variable> {
        match &*self.inner {
            ExpressionInner::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

impl Debug for ExpressionInner {
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
            Self::FunctionCall(_) => f.debug_tuple("FunctionCall").finish_non_exhaustive(),
        }
    }
}

pub trait ToIntermediate {
    fn to_sqcel(&self, tp: &Transpiler) -> Result<Expression>;
}
