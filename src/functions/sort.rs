use sea_query::Order;

use crate::{
    Result, Transpiler,
    functions::{Function, FunctionArgs, FunctionOrigin, FunctionPattern, Pattern, view},
    intermediate::ToSql,
    sql_extensions::{IntoSqlExpression, SqlExtension},
    types::TypedExpression,
};

use super::{ArgPosition, ReturnType};

pub fn sort() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "sort".to_owned(),
            receiver: Some(view()),
            args: vec![Pattern::Ident],
            variadic: Some(Pattern::Lambda),
            returns: ReturnType::Arg(ArgPosition::Receiver),
        },
        "Sort the view by the given expressions",
        to_sql,
        FunctionOrigin::Sqcel,
    )
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    let TypedExpression { expr, ty } = args.receiver()?.to_sql(tp)?;

    let tp_inner = tp.iterate(
        &args
            .receiver()?
            .try_iterate(tp, args.arg(0)?.as_single_ident()?)?,
    );

    let mut select_statement = expr.as_select()?.clone();

    for ord in args.variadics(1) {
        select_statement.order_by_expr_with_nulls(
            ord.to_sql(&tp_inner)?.expr,
            Order::Asc,
            sea_query::NullOrdering::First,
        );
    }

    Ok(select_statement.take().into_expr().with_type(ty))
}
