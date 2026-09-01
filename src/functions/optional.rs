use indoc::indoc;
use sea_query::Func;

use crate::{
    func_args,
    functions::{Function, FunctionOrigin},
    intermediate::ToSql,
    sql_extensions::SqlExtension,
    types::agree::unify,
};

#[must_use]
pub fn or() -> Function {
    Function::define_with_origin(
        func_args!(or(self: Inferred, Inferred) -> Inferred),
        indoc!(
            r#"
                Returns its receiver it is non-null or its argument if the receiver is null
                
                Examples:
                "something".startsWith("else")  // "something"
                null.startsWith("other")        // "other"
                "#
        ),
        |tp, x| {
            let a = x.receiver()?.returntype(tp);
            let b = x.arg(0)?.returntype(tp);
            let rt = unify(a.clone(), b).unwrap_or(a);
            Ok(Func::coalesce([
                x.receiver()?.to_sql(tp)?.reshape(tp, &rt)?.expr,
                x.arg(0)?.to_sql(tp)?.reshape(tp, &rt)?.expr,
            ])
            .with_type(rt))
        },
        FunctionOrigin::Sqcel,
    )
}
