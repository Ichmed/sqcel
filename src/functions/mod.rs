pub mod cast;
pub mod dyn_fn;
pub mod iter;
pub mod json;
mod list_oper;
mod map;
pub mod or;
pub mod reduce;
pub mod string_operations;

use std::{any::Any, fmt::Display, sync::Arc};

use crate::{
    Transpiler,
    functions::{
        cast::cast,
        reduce::{Reduce, Reducer},
        string_operations::{Contains, EndsWith, Matches, StartsWith},
    },
    intermediate::{Expression, ExpressionInner, Rc, ToSql},
    transpiler::{Error, Result},
    types::SqlType,
};
use dyn_fn::{DynFunc, Signature};
use list_oper::{all, any};
use map::Map;

use sea_query::{Query, SelectStatement, SimpleExpr as SqlExpression, SubQueryStatement};

pub trait Function: ToSql + Any + Send + Sync {}

macro_rules! r_arm {
    (($name:literal, $rec:ident, $var:expr, $agg:expr) $variant:ident) => {{
        match ($rec, $agg) {
            (Some(x), agg) => Reduce::new(Reducer::$variant, x, $var, agg)?,
            (None, [x]) => Reduce::new(Reducer::$variant, x, $var, Default::default())?,
            (rec, args) => Err(WrongFunctionArgs::given($name, rec, args)
                .allowed(false, 1)
                .allowed(true, 0))?,
        }
    }};
}

#[allow(clippy::match_same_arms, reason = "Make valid call patterns explicit")]
pub fn get(
    tp: &Transpiler,
    name: &Expression,
    rec: Option<&Expression>,
    args: &[Expression],
) -> Result<Rc<dyn Function>> {
    if let ExpressionInner::Access(name) = &**name {
        let name = name
            .to_path()
            .ok_or(Error::Todo("Function paths can't be empty"))?;
        let name = name.as_ref();

        Ok(match (name, rec, args) {
            ("or", Some(rec), [alt]) => Arc::new(or::Or {
                rec: rec.clone(),
                alt: alt.clone(),
            }),

            ("collect_object", Some(x), []) => Arc::new(json::CollectJsonRecursive(x.clone())),
            ("collect_object", rec, args) => {
                Err(WrongFunctionArgs::given("collect_object", rec, args).allowed(false, 1))?
            }

            ("int", None, [x]) => cast(SqlType::Integer, x),
            ("int", rec, args) => {
                Err(WrongFunctionArgs::given("int", rec, args).allowed(false, 1))?
            }

            ("string", None, [x]) => cast(SqlType::Text, x),
            ("string", rec, args) => {
                Err(WrongFunctionArgs::given("string", rec, args).allowed(false, 1))?
            }

            ("bool", None, [x]) => cast(SqlType::Boolean, x),
            ("bool", rec, args) => {
                Err(WrongFunctionArgs::given("bool", rec, args).allowed(false, 1))?
            }

            ("double", None, [x]) => cast(SqlType::Double, x),
            ("double", rec, args) => {
                Err(WrongFunctionArgs::given("double", rec, args).allowed(false, 1))?
            }

            ("time", None, [x]) => cast(SqlType::Time, x),
            ("time", rec, args) => {
                Err(WrongFunctionArgs::given("time", rec, args).allowed(false, 1))?
            }

            ("min", a, [x, b @ ..]) => r_arm!(("min", a, Some(x), b) Min),
            ("min", Some(a), []) => Reduce::new(Reducer::Min, a, None, &[])?,

            ("max", a, [x, b @ ..]) => r_arm!(("count", a, Some(x), b) Max),
            ("max", Some(a), []) => Reduce::new(Reducer::Max, a, None, &[])?,

            ("count", a, [x, b @ ..]) => r_arm!(("count", a, Some(x), b) Count),
            ("count", Some(a), []) => Reduce::new(Reducer::Count, a, None, &[])?,
            ("size", Some(a), []) => Reduce::new(Reducer::Count, a, None, &[])?,
            ("size", None, [a]) => Reduce::new(Reducer::Count, a, None, &[])?,

            ("sum", a, [x, b @ ..]) => r_arm!(("sum", a, Some(x), b) Sum),
            ("sum", Some(a), []) => Reduce::new(Reducer::Sum, a, None, &[])?,

            ("mean", a, [x, b @ ..]) => r_arm!(("mean", a, Some(x), b) Mean),
            ("mean", Some(a), []) => Reduce::new(Reducer::Mean, a, None, &[])?,

            ("map", Some(rec), [var, func]) => Map::new(rec, var, None, func)?,
            ("map", Some(rec), [var, filter, func]) => Map::new(rec, var, Some(filter), func)?,
            ("map", rec, args) => Err(WrongFunctionArgs::given("map", rec, args)
                .allowed(true, 2)
                .allowed(true, 3))?,

            // Implement .filter(x, pred) as .map(x, pred, x)
            ("filter", Some(rec), [var, filter]) => Map::new(rec, var, Some(filter), var)?,
            ("filter", rec, args) => {
                Err(WrongFunctionArgs::given("filter", rec, args).allowed(true, 2))?
            }

            // ("latest", None, [amount]) => Latest::new(Some(amount), None)?,
            // ("latest", None, [amount, pred]) => Latest::new(Some(amount), Some(pred))?,
            ("latest", rec, args) => Err(WrongFunctionArgs::given("latest", rec, args)
                .allowed(false, 1)
                .allowed(false, 2))?,

            ("all", Some(list), [var, predicate]) => all(list, var, predicate)?,
            ("all", rec, args) => Err(WrongFunctionArgs::given("all", rec, args).allowed(true, 2))?,

            ("exists", Some(list), [var, predicate]) => any(list, var, predicate)?,
            ("exists", rec, args) => {
                Err(WrongFunctionArgs::given("exists", rec, args).allowed(true, 2))?
            }

            ("starts_with", Some(rec), [pattern]) => Arc::new(StartsWith {
                rec: rec.clone(),
                arg: pattern.clone(),
            }),
            ("ends_with", Some(rec), [pattern]) => Arc::new(EndsWith {
                rec: rec.clone(),
                arg: pattern.clone(),
            }),
            ("matches", Some(rec), [pattern]) => Arc::new(Matches {
                rec: rec.clone(),
                arg: pattern.clone(),
            }),
            ("contains", Some(rec), [pattern]) => Arc::new(Contains {
                rec: rec.clone(),
                arg: pattern.clone(),
            }),

            (name, rec, args) => get_dynamic_function(tp, name, rec, args)?,
        })
    } else {
        Err(Error::Todo("Only idents can be function names"))
    }
}

fn get_dynamic_function(
    tp: &Transpiler,
    name: &str,
    rec: Option<&Expression>,
    args: &[Expression],
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
                rec: rec.cloned(),
                args: args.to_vec(),
                rt: rt.clone(),
            })
            .ok_or(Error::Todo("Unknown function or arg configuration"))?,
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
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn given(name: impl ToString, rec: Option<&Expression>, args: &[Expression]) -> Self {
        Self {
            name: name.to_string(),
            given: (rec.cloned(), args.to_vec()),
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

            for (rec, args) in &self.expected {
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

#[must_use]
pub fn to_select(expr: SqlExpression) -> SelectStatement {
    match expr {
        SqlExpression::SubQuery(_, boxed) => match *boxed {
            SubQueryStatement::SelectStatement(select_statement) => select_statement,
            _ => unimplemented!(
                "SQCEL should never generate a statement that is not a select statement"
            ),
        },
        x => Query::select().expr(x).take(),
    }
}

// #[must_use]
// pub fn subquery(s: SelectStatement) -> TypedExpression {
//     TypedExpression {
//         ty: Type::RecordSet(Default::default()),
//         expr: SqlExpression::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(s))),
//     }
// }
