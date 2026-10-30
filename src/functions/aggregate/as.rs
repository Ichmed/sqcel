use crate::{
    Result, Transpiler,
    functions::{
        Function, FunctionArgs, FunctionOrigin, FunctionPattern, Pattern, ReturnType, column,
    },
    intermediate::{Rc, ToSql},
    types::Type,
};

pub fn r#as() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "as".to_owned(),
            receiver: Some(column()),
            args: vec![Pattern::Ident],
            variadic: None,
            returns: ReturnType::Custom(
                "Column".to_owned(),
                Rc::new(|tp, args| return_type(tp, args).unwrap_or_default()),
            ),
        },
        "Rename a column (as long as the column name is never evaluated this is a noop)",
        |tp, args| {
            Ok(args
                .receiver()?
                .to_sql(tp)?
                .with_type(return_type(tp, args)?))
        },
        FunctionOrigin::Sqcel,
    )
}

fn return_type(tp: &Transpiler, args: &FunctionArgs) -> Result<Type> {
    Ok(args
        .receiver()?
        .returntype(tp)
        .into_column()?
        .rename(args.arg(0)?.as_single_ident()?.to_string()))
}
