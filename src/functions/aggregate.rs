use sea_query::{Func, IntoIden, Query, SelectExpr};

use crate::{
    Error, Result, Transpiler,
    functions::{
        Function, FunctionArgs, FunctionOrigin, FunctionPattern, Pattern, ReturnType,
        reduce::Reducer,
    },
    intermediate::{Rc, ToSql},
    magic,
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::str_alias,
    types::{ColumnType, Type, TypedExpression},
};

#[must_use]
pub fn count() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "count".to_owned(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident],
            variadic: Some(Pattern::Lambda),
            returns: ReturnType::Custom(
                "Unsigned Integer or Named View".to_owned(),
                Rc::new(|tp, args| return_type(tp, args, Reducer::Count).unwrap_or_default()),
            ),
        },
        "Count the elements in the array, object or view. Optionally group by columns",
        move |tp, args| to_sql(tp, args, Reducer::Count),
        FunctionOrigin::Sqcel,
    )
}

fn return_type(tp: &Transpiler, args: &FunctionArgs, reducer: Reducer) -> Result<Type> {
    let source = args
        .receiver()?
        .try_iterate(tp, args.arg(0)?.as_single_ident()?.into_iden())?;

    let tp_inner = tp.iterate(&source);

    let aggregations = args
        .variadics(1)
        .iter()
        .map(|x| x.to_sql(&tp_inner))
        .collect::<Result<Vec<_>>>()?;

    return_type_inner(&aggregations, reducer)
}

fn return_type_inner(aggs: &[TypedExpression], reducer: Reducer) -> Result<Type> {
    Ok(Type::NamedView(
        aggs.iter()
            .enumerate()
            .map(|(i, x)| {
                let name = format!("c_{i}");
                Ok((name, x.ty.col_type().cloned().unwrap_or_default()))
            })
            .chain([Ok((
                reducer.as_ref().to_lowercase(),
                match reducer {
                    Reducer::Count => ColumnType::Unsigned,
                    _ => ColumnType::Float,
                },
            ))])
            .collect::<Result<_>>()?,
    ))
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs, reducer: Reducer) -> Result<TypedExpression> {
    let var_expression = args.arg(0).cloned().unwrap_or_else(|_| magic::x());
    let var = var_expression.as_single_ident()?.into_iden();
    let source = args.receiver()?.try_iterate(tp, var.clone())?;

    let tp_inner = tp.iterate(&source);

    let aggregations = args
        .variadics(1)
        .iter()
        .map(|x| x.to_sql(&tp_inner))
        .collect::<Result<Vec<_>>>()?;

    let return_type = return_type_inner(&aggregations, reducer)?;

    let var_expr_cast = var_expression.to_sql(&tp_inner)?;
    let expr = match reducer {
        Reducer::Count => Func::count(var_expr_cast),
        Reducer::Min => Func::min(var_expr_cast),
        Reducer::Max => Func::max(var_expr_cast),
        Reducer::Sum => Func::sum(var_expr_cast),
        Reducer::Mean => Func::avg(var_expr_cast),
    };
    let exprs = aggregations
        .iter()
        .enumerate()
        .map(|(index, expr)| SelectExpr {
            expr: expr.expr.clone(),
            alias: Some(str_alias(format!("c_{index}"))),
            window: None,
        });
    Ok(Query::select()
        .exprs(exprs)
        .add_group_by(aggregations.iter().map(|x| &x.expr).cloned())
        .expr(expr)
        .from(source)
        .take()
        .into_expr()
        .with_type(return_type))
}

#[cfg(test)]
mod test {

    use sea_query::PostgresQueryBuilder;

    use crate::{
        Transpiler,
        hacks::{self, get_plaintext_expression},
        structure::{Database, Schema, SqlLayout, Table},
        types::SqlType,
    };

    #[test]
    fn count_func() {
        let tp = Transpiler::quick(
            vec![("sch", vec![("foo", vec![("number", SqlType::Integer)])])].into(),
        )
        .to_builder()
        .build()
        .unwrap();

        dbg!(&tp.layout);

        let sql = hacks::get_plaintext_expression(
            "foo.filter(x, x.number == 2)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x".* FROM "foo" AS "x" WHERE "x"."number" = 2)"#
        );

        let sql = hacks::get_plaintext_expression(
            "foo.filter(x, x.number == 2).count(x, x.number)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number" AS "c_0", COUNT("x".*) FROM (SELECT "x".* FROM "foo" AS "x" WHERE "x"."number" = 2) AS "x" GROUP BY "x"."number")"#
        );
    }

    #[test]
    fn count_agg() {
        let tp = Transpiler::quick(
            vec![("sch", vec![("foo", vec![("number", SqlType::Integer)])])].into(),
        )
        .to_builder()
        .build()
        .unwrap();

        let sql =
            hacks::get_plaintext_expression("foo.count(x, x.number)", &tp, PostgresQueryBuilder)
                .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."number" AS "c_0", COUNT("x".*) FROM "foo" AS "x" GROUP BY "x"."number")"#
        );
    }

    #[test]
    fn count_agg_single() {
        let tp = Transpiler::quick(
            vec![("postgres", vec![("foo", vec![("bar", SqlType::Unsigned)])])].into(),
        );

        let sql =
            get_plaintext_expression("foo.count(x, x.bar)", &tp, PostgresQueryBuilder).unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar" AS "c_0", COUNT("x".*) FROM "foo" AS "x" GROUP BY "x"."bar")"#
        );
    }

    #[test]
    fn count_agg_multi() {
        let tp = Transpiler::new()
            .layout(SqlLayout::new(Database::new().schema(
                Schema::new("postgres").table(Table::new("foo").columns([
                    ("bar_1", SqlType::Unsigned),
                    ("bar_2", SqlType::Unsigned),
                    ("bar_3", SqlType::Unsigned),
                ])),
            )))
            .enter_schema("postgres")
            .build()
            .unwrap();

        let sql = get_plaintext_expression(
            "foo.count(x, x.bar_1, x.bar_2, x.bar_3)",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar_1" AS "c_0", "x"."bar_2" AS "c_1", "x"."bar_3" AS "c_2", COUNT("x".*) FROM "foo" AS "x" GROUP BY "x"."bar_1", "x"."bar_2", "x"."bar_3")"#
        );
    }
}
