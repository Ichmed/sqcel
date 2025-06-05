mod count;
pub mod dyn_fn;
mod last;
mod list_oper;
mod map;
mod slice;

use std::{any::Any, fmt::Display};

use crate::{
    Transpiler,
    intermediate::{Expression, ExpressionInner, Rc, ToSql},
    magic::x,
    transpiler::{ParseError, Result, alias},
    types::{SqlType, Type, TypedExpression},
};
use count::Count;
use dyn_fn::{DynFunc, Signature};
use last::Last;
use list_oper::{all, any};
use map::Map;

use sea_query::{Query, SelectStatement, SimpleExpr as SqlExpression, SubQueryStatement};

pub trait Function: ToSql + Any + Send + Sync {
    fn returntype(&self) -> Type;
}

pub fn get(
    tp: &Transpiler,
    name: Expression,
    rec: Option<Expression>,
    args: Vec<Expression>,
) -> Result<Rc<dyn Function>> {
    if let ExpressionInner::Access(name) = &*name {
        Ok(
            match (
                name.to_path()
                    .ok_or(ParseError::Todo("Function paths can't be empty"))?
                    .as_ref(),
                rec.as_ref(),
                args.as_slice(),
            ) {
                ("int", None, [x]) => cast("integer", SqlType::Integer.into(), x, tp)?,
                ("int", _, _) => Err(WrongFunctionArgs::given("int", rec, args).allowed(false, 1))?,

                ("string", None, [x]) => cast("text", SqlType::String.into(), x, tp)?,
                ("string", _, _) => {
                    Err(WrongFunctionArgs::given("string", rec, args).allowed(false, 1))?
                }

                ("bool", None, [x]) => cast("boolean", SqlType::Boolean.into(), x, tp)?,
                ("bool", _, _) => {
                    Err(WrongFunctionArgs::given("bool", rec, args).allowed(false, 1))?
                }

                ("double", None, [x]) => cast("double precision", SqlType::Double.into(), x, tp)?,
                ("double", _, _) => {
                    Err(WrongFunctionArgs::given("double", rec, args).allowed(false, 1))?
                }

                ("time", None, [x]) => cast(
                    "timestamp with time zone",
                    Type::Sql(SqlType::TimeNoZone),
                    x,
                    tp,
                )?,
                ("time", _, _) => {
                    Err(WrongFunctionArgs::given("time", rec, args).allowed(false, 1))?
                }

                // ("first", Some(list), []) => First::new(list)?,
                // ("first", None, [list]) => First::new(list)?,
                ("count", None, [x]) => Count::new(x)?,
                ("count", Some(x), []) => Count::new(x)?,
                ("count", _, _) => Err(WrongFunctionArgs::given("count", rec, args)
                    .allowed(false, 1)
                    .allowed(true, 0))?,

                ("map", Some(rec), [var, func]) => Map::new(rec, var, None, func)?,
                ("map", Some(rec), [var, filter, func]) => Map::new(rec, var, Some(filter), func)?,
                ("map", _, _) => Err(WrongFunctionArgs::given("map", rec, args)
                    .allowed(true, 2)
                    .allowed(true, 3))?,

                // Implement .filter(x, pred) as .map(x, pred, x)
                ("filter", Some(rec), [var, filter]) => Map::new(rec, var, Some(filter), var)?,
                ("filter", _, _) => {
                    Err(WrongFunctionArgs::given("filter", rec, args).allowed(true, 2))?
                }

                ("last", None, [amount]) => Last::new(Some(amount), None)?,
                ("last", None, [amount, predicate]) => Last::new(Some(amount), Some(predicate))?,
                ("last", _, _) => Err(WrongFunctionArgs::given("last", rec, args)
                    .allowed(false, 1)
                    .allowed(false, 2))?,

                ("all", None, [list]) => all(list, &x(), &x())?,
                ("all", Some(list), []) => all(list, &x(), &x())?,
                ("all", Some(list), [var, predicate]) => all(list, var, predicate)?,
                ("all", None, [list, var, predicate]) => all(list, var, predicate)?,
                ("all", _, _) => Err(WrongFunctionArgs::given("all", rec, args)
                    .allowed(true, 0)
                    .allowed(true, 2)
                    .allowed(false, 1)
                    .allowed(false, 3))?,

                ("any", None, [list]) => any(list, &x(), &x())?,
                ("any", Some(list), []) => any(list, &x(), &x())?,
                ("any", Some(list), [var, predicate]) => any(list, var, predicate)?,
                ("any", None, [list, var, predicate]) => any(list, var, predicate)?,
                ("any", _, _) => Err(WrongFunctionArgs::given("any", rec, args)
                    .allowed(true, 0)
                    .allowed(true, 2)
                    .allowed(false, 1)
                    .allowed(false, 3))?,

                (name, _, _) => get_dynamic_function(tp, name, rec, args)?,
            },
        )
    } else {
        Err(ParseError::Todo("Only idents can be function names"))
    }
}

fn get_dynamic_function(
    tp: &Transpiler,
    name: &str,
    rec: Option<Expression>,
    args: Vec<Expression>,
) -> Result<Rc<DynFunc>> {
    Ok(Rc::new(
        tp.functions
            .get(&Signature {
                name: name.to_string(),
                rec: rec.is_some(),
                args: args.len(),
            })
            .map(|(f, rt)| DynFunc {
                f: f.clone(),
                rec,
                args,
                rt: rt.clone(),
            })
            .ok_or(ParseError::Todo("Unknown function or arg configuration"))?,
    ))
}

#[derive(Debug)]
pub struct WrongFunctionArgs {
    name: String,
    #[allow(dead_code)]
    given: (Option<Expression>, Vec<Expression>),
    expected: Vec<(bool, usize)>,
}

impl WrongFunctionArgs {
    pub fn given(name: impl ToString, rec: Option<Expression>, args: Vec<Expression>) -> Self {
        Self {
            name: name.to_string(),
            given: (rec, args),
            expected: Default::default(),
        }
    }

    fn allowed(mut self, rec: bool, args: usize) -> Self {
        self.expected.push((rec, args));
        self
    }
}

impl std::error::Error for WrongFunctionArgs {}

impl Display for WrongFunctionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Invalid call to ")?;
        f.write_str(&self.name)?;

        if !self.expected.is_empty() {
            f.write_str("\nValid call structures:")?;

            for (rec, args) in self.expected.iter() {
                f.write_str("\n- ")?;
                if *rec {
                    f.write_str("x.")?;
                }
                self.name.fmt(f)?;
                f.write_str("(")?;
                if *args > 0 {
                    f.write_str("arg_0")?;
                }
                for i in 1..*args {
                    f.write_str(", arg_")?;
                    i.fmt(f)?;
                }

                f.write_str(")")?;
            }
        }

        Ok(())
    }
}

struct Cast(Expression, &'static str, Type);
impl Function for Cast {
    fn returntype(&self) -> Type {
        self.2.clone()
    }
}

impl ToSql for Cast {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(TypedExpression {
            ty: self.2.clone(),
            expr: self.0.to_sql(tp)?.expr.cast_as(alias(self.1)),
        })
    }
}

#[inline]
fn cast(tyn: &'static str, ty: Type, exp: &Expression, _: &Transpiler) -> Result<Rc<dyn Function>> {
    Ok(Rc::new(Cast(exp.clone(), tyn, ty)))
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

pub fn subquery(s: SelectStatement) -> TypedExpression {
    TypedExpression {
        ty: Type::Unknown,
        expr: SqlExpression::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(s))),
    }
}
