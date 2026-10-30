use crate::{
    functions::{
        Function, FunctionOrigin, FunctionPattern, ReturnType,
        aggregate::{return_type, to_sql},
        reduce::Reducer,
        view,
    },
    intermediate::Rc,
};

pub fn no_args() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "count".to_owned(),
            receiver: Some(view()),
            args: vec![],
            variadic: None,
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
            "foo.filter(x, x.number == 2).count()",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT COUNT(*) FROM (SELECT "x".* FROM "foo" AS "x" WHERE "x"."number" = 2) AS "x")"#
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
            hacks::get_plaintext_expression("foo.count()", &tp, PostgresQueryBuilder).unwrap();
        assert_eq!(sql, r#"(SELECT COUNT(*) FROM "foo" AS "x")"#);
    }

    #[test]
    fn count_agg_single() {
        let tp = Transpiler::quick(
            vec![("postgres", vec![("foo", vec![("bar", SqlType::Unsigned)])])].into(),
        );

        let sql =
            get_plaintext_expression("foo.group_by(x, x.bar).count()", &tp, PostgresQueryBuilder)
                .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar", COUNT(*) FROM "foo" AS "x" GROUP BY "x"."bar")"#
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
            "foo.group_by(x, x.bar_1, x.bar_2, x.bar_3 / 3).count()",
            &tp,
            PostgresQueryBuilder,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"(SELECT "x"."bar_1", "x"."bar_2", "x"."bar_3" / 3 AS "c_2", COUNT(*) FROM "foo" AS "x" GROUP BY "x"."bar_1", "x"."bar_2", "x"."bar_3" / 3)"#
        );
    }
}
