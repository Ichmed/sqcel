pub mod access_chain;
pub mod cel;
pub mod to_sql;
pub mod variables;

use crate::{
    Error, Result, Transpiler,
    functions::{Function, iter::Iterable},
    intermediate::{
        access_chain::AccessChain,
        to_sql::try_iterate_fallback,
        variables::{Atom, Object, Variable},
    },
    sql_extensions::SqlExtension,
    transpiler::str_alias,
    types::{SqlType, Type, TypedExpression, json::JsonType},
    types2::Cell,
};
use cel_interpreter::{Value as CelValue, objects::Key};
use cel_parser::{ArithmeticOp, RelationOp, UnaryOp};
use sea_query::{
    BinOper, CaseStatement, DynIden, ExprTrait, IntoIden, SimpleExpr, Value,
    extension::postgres::PgExpr,
};
use std::fmt::{Debug, Display};
pub use to_sql::ToSql;

pub type Rc<T> = sea_query::RcOrArc<T>;

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
    Index(Box<Expression>, Box<Expression>),
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
            ExpressionInner::Index(expression, _) => expression.returntype(tp),
            ExpressionInner::Variable(variable) => variable.returntype(tp),
            ExpressionInner::Arithmetic(a, _, _) => a.returntype(tp),
            ExpressionInner::Relation(_, _, _) => Type::Column(None, SqlType::Boolean),
            ExpressionInner::Ternary(_, expression1, _) => expression1.returntype(tp),
            ExpressionInner::Or(_, _) => Type::Column(None, SqlType::Boolean),
            ExpressionInner::And(_, _) => Type::Column(None, SqlType::Boolean),
            ExpressionInner::Unary(_, expression) => expression.returntype(tp),
            ExpressionInner::FunctionCall(function) => function.returntype(tp),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ident(pub(crate) Rc<String>);

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
        str_alias(self.0)
    }
}

impl IntoIden for &Ident {
    fn into_iden(self) -> sea_query::DynIden {
        str_alias(self.0.to_string())
    }
}

impl Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl ToSql for Atom {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match self {
            Self::Null => SimpleExpr::Constant(Value::Json(None)),
            Self::Bool(b) => SimpleExpr::Constant(Value::Bool(Some(*b))),
            Self::String(s) => SimpleExpr::Constant(Value::String(Some(Box::new(s.to_string())))),
            Self::Bytes(b) => {
                SimpleExpr::Constant(Value::Bytes(Some(Box::new(b.as_ref().clone()))))
            }
            Self::Int(i) => SimpleExpr::Constant(Value::BigInt(Some(*i))),
            Self::UInt(u) => SimpleExpr::Constant(Value::BigUnsigned(Some(*u))),
            Self::Float(f) => SimpleExpr::Constant(Value::Double(Some(*f))),
        }
        .with_type(self.returntype(tp)))
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        Cell::Literal(match self {
            Self::Null => SqlType::Null,
            Self::Bool(_) => SqlType::Boolean,
            // Self::String(s) => SqlType::String(sea_query::StringLen::N(s.len() as u32)),
            Self::String(_) => SqlType::Text,
            Self::Bytes(v) => SqlType::Binary(v.len() as u32),
            Self::Int(_) => SqlType::BigInteger,
            Self::UInt(_) => SqlType::BigUnsigned,
            Self::Float(_) => SqlType::Double,
        })
        .into()
    }

    fn try_iterate(&self, tp: &Transpiler, var: DynIden) -> Result<Iterable> {
        try_iterate_fallback(self, tp, &var)
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

impl TryFrom<Expression> for CelValue {
    type Error = CantBeAValue;

    fn try_from(value: Expression) -> std::result::Result<Self, Self::Error> {
        match &*value.inner {
            ExpressionInner::Variable(var) => var.clone().try_into(),
            _ => Err(CantBeAValue),
        }
    }
}

impl ToSql for Expression {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match &*self.inner {
            ExpressionInner::Access(chain) => chain.to_sql(tp)?,
            ExpressionInner::Variable(variable) => variable.to_sql(tp)?,
            ExpressionInner::Arithmetic(a, op, b) => {
                let bin_oper = match op {
                    ArithmeticOp::Add => BinOper::Add,
                    ArithmeticOp::Subtract => BinOper::Sub,
                    ArithmeticOp::Divide => BinOper::Div,
                    ArithmeticOp::Multiply => BinOper::Mul,
                    ArithmeticOp::Modulus => BinOper::Mod,
                };
                let a = a.to_sql(tp)?;
                let b = b.to_sql(tp)?;

                let ty: Type = Cell::Value(match (&a.ty.col_type(), &b.ty.col_type()) {
                    (Some(SqlType::Time), _) | (_, Some(SqlType::Time)) => SqlType::Time,
                    _ => SqlType::Integer,
                })
                .into();

                let a = a.with_type(ty.clone());
                let b = b.with_type(ty.clone());

                let expr = {
                    SimpleExpr::Binary(
                        Box::new(a.expr.into_json_cast()),
                        bin_oper,
                        Box::new(b.expr.into_json_cast()),
                    )
                };
                TypedExpression { expr, ty }
            }

            ExpressionInner::Relation(a, op, b) => {
                adjust_bin_oper(tp, a, b, op)?.with_type(Cell::Value(SqlType::Boolean))
            }
            ExpressionInner::Ternary(r#if, then, r#else) => SimpleExpr::Case(Box::new(
                CaseStatement::new()
                    .case(r#if.to_sql(tp)?.expr, then.to_sql(tp)?.expr)
                    .finally(r#else.to_sql(tp)?.expr),
            ))
            .with_type(Cell::Value(SqlType::Boolean)),
            ExpressionInner::Or(a, b) => a
                .to_sql(tp)?
                .expr
                .or(b.to_sql(tp)?.expr)
                .with_type(Cell::Value(SqlType::Boolean)),
            ExpressionInner::And(a, b) => a
                .to_sql(tp)?
                .expr
                .and(b.to_sql(tp)?.expr)
                .with_type(Cell::Value(SqlType::Boolean)),
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
                Self::index(tp, expression.as_ref(), index)?
            }
        })
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        match &*self.inner {
            ExpressionInner::Access(access_chain) => access_chain.returntype(tp),
            ExpressionInner::Index(expression, _) => expression
                .returntype(tp)
                .col_type()
                .cloned()
                .unwrap_or_default()
                .into(),
            ExpressionInner::Variable(variable) => variable.returntype(tp),
            ExpressionInner::Arithmetic(ex, _, _) | ExpressionInner::Ternary(_, ex, _) => {
                ex.returntype(tp)
            }
            ExpressionInner::Relation(_, _, _)
            | ExpressionInner::Or(_, _)
            | ExpressionInner::And(_, _) => Cell::Value(SqlType::Boolean).into(),
            ExpressionInner::Unary(_, expression) => expression.returntype(tp),
            ExpressionInner::FunctionCall(function) => function.returntype(tp),
        }
    }

    fn try_iterate(&self, tp: &Transpiler, var: DynIden) -> Result<Iterable> {
        match &*self.inner {
            ExpressionInner::Access(access_chain) => access_chain.try_iterate(tp, var),
            // ExpressionInner::Index(expression, expression1) => todo!(),
            ExpressionInner::Variable(variable) => variable.try_iterate(tp, var),

            _ => try_iterate_fallback(self, tp, &var),
        }
    }

    // fn columns(&self, tp: &Transpiler) -> HashMap<String, crate::structure::Column> {
    //     match &*self.inner {
    //         ExpressionInner::Access(access_chain) => access_chain.columns(tp),
    //         ExpressionInner::FunctionCall(function) => function.columns(tp),

    //         _ => Default::default(),
    //     }
    // }
}

fn adjust_bin_oper(
    tp: &Transpiler,
    a: &Expression,
    b: &Expression,
    op: &RelationOp,
) -> Result<SimpleExpr> {
    let a = a.to_sql(tp)?;
    let b = b.to_sql(tp)?;
    let op = match op {
        RelationOp::LessThan => BinOper::SmallerThan,
        RelationOp::LessThanEq => BinOper::SmallerThanOrEqual,
        RelationOp::GreaterThan => BinOper::GreaterThan,
        RelationOp::GreaterThanEq => BinOper::GreaterThanOrEqual,
        RelationOp::Equals => BinOper::Equal,
        RelationOp::NotEquals => BinOper::NotEqual,
        RelationOp::In => {
            return Err(Error::Todo("IN operator for lists, subqueries etc"));
        }
    };

    let (a, b) = if a.ty.is_json() || b.ty.is_json() {
        (a.expr.into_json_get(), b.expr.into_json_get())
    } else {
        (a.expr.into_json_cast(), b.expr.into_json_cast())
    };

    Ok(match (a, op, b) {
        (SimpleExpr::Constant(v), op, x) | (x, op, SimpleExpr::Constant(v)) if v == v.as_null() => {
            match op {
                BinOper::Equal => x.is_null(),
                BinOper::NotEqual => x.is_not_null(),
                _ => return Ok(SimpleExpr::Constant(false.into())),
            }
        }
        (a, op, b) => SimpleExpr::Binary(Box::new(a), op, Box::new(b)),
    })
}

impl Expression {
    fn index(tp: &Transpiler, ex: &Self, index: &Self) -> Result<TypedExpression> {
        let TypedExpression { expr, ty } = ex.to_sql(tp)?;

        // Ok(match ty {
        //     Type::Sql(SqlType::JSON)
        //     | Type::Json(JsonType::Any | JsonType::List | JsonType::Map) => TypedExpression {
        //         expr: expr.get_json_field(index.to_sql(tp)?.expr),
        //         ty,
        //     },
        //     ty @ Type::RecordSet(_) => subquery(
        //         TypedExpression { expr, ty }
        //             .to_record_set(tp)?
        //             .offset(index.as_postive_integer()?)
        //             .limit(1)
        //             .take(),
        //     ),

        //     ty => return Err(ParseError::CanNotIndexType(ty)),
        // })
        Ok(match ty {
            Type::Column(_, col_type)
            | Type::Cell(Cell::Literal(col_type) | Cell::Value(col_type))
                if col_type.is_json() =>
            {
                expr.get_json_field(index.to_sql(tp)?.expr)
                    .with_type(Cell::Value(JsonType::Any.into()))
            }
            Type::Row(_items) => todo!(),
            Type::NamedRow(_items) => todo!(),
            Type::View(_items) => todo!(),
            Type::NamedView(_index_map) => todo!(),
            ty => return Err(Error::CanNotIndexType(ty)),
        })
    }

    #[must_use]
    pub fn is_object(&self) -> bool {
        match &*self.inner {
            ExpressionInner::Variable(var) => var.is_object(),
            _ => false,
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
