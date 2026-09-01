use sea_query::{Func, IntoIden, Query};
use strum::EnumString;

use crate::{
    Transpiler,
    functions::{Function, FunctionArgs, FunctionOrigin, FunctionPattern, ReturnType},
    intermediate::{Rc, ToSql},
    magic::{self},
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::Result,
    types::{SqlType, TypedExpression},
};

#[non_exhaustive]
#[derive(Clone, Copy, EnumString, strum::AsRefStr)]
pub enum Reducer {
    Count,
    Sum,
    Mean,
    Max,
    Min,
}

#[must_use]
pub fn size() -> Function {
    reduce_function(
        "size",
        "Count the elements in the array",
        Reducer::Count,
        FunctionOrigin::Cel,
    )
}

#[must_use]
pub fn sum() -> Function {
    reduce_function(
        "sum",
        "Sum up the elements in the array",
        Reducer::Sum,
        FunctionOrigin::Sqcel,
    )
}

#[must_use]
pub fn mean() -> Function {
    reduce_function(
        "mean",
        "Calculate the arithmetic mean of the elements in the array",
        Reducer::Mean,
        FunctionOrigin::Sqcel,
    )
}

#[must_use]
pub fn min() -> Function {
    reduce_function(
        "min",
        "Return the smallest element in the array",
        Reducer::Min,
        FunctionOrigin::Sqcel,
    )
}

#[must_use]
pub fn max() -> Function {
    reduce_function(
        "max",
        "Return the largest element in the array",
        Reducer::Max,
        FunctionOrigin::Sqcel,
    )
}

fn reduce_function(
    name: &str,
    description: &str,
    reducer: Reducer,
    origin: FunctionOrigin,
) -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: name.into(),
            receiver: Some(SqlType::Array(Rc::new(SqlType::Inferred)).into()),
            args: vec![],
            variadic: None,
            returns: match reducer {
                Reducer::Count => SqlType::Unsigned.into(),
                _ => ReturnType::SameAsReceiverInner,
            },
        },
        description,
        move |tp, args| to_sql(tp, args, reducer),
        origin,
    )
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs, reducer: Reducer) -> Result<TypedExpression> {
    let var_expression = args.arg(0).cloned().unwrap_or_else(|_| magic::x());
    let var = var_expression.as_single_ident()?.into_iden();
    let source = args.receiver()?.try_iterate(tp, var.clone())?;

    let tp_inner = tp.iterate(&source);

    let rt = match reducer {
        Reducer::Count => SqlType::Unsigned.cell(),
        _ => SqlType::Float.cell(),
    };

    let expr = var_expression.to_sql(&tp_inner)?.cast(tp, rt.col_type())?;

    let expr = match reducer {
        Reducer::Count => Func::count(expr),
        Reducer::Min => Func::min(expr),
        Reducer::Max => Func::max(expr),
        Reducer::Sum => Func::sum(expr),
        Reducer::Mean => Func::avg(expr),
    };

    Ok(Query::select()
        .expr(expr)
        .from(source)
        .take()
        .into_expr()
        .with_type(rt))
}
