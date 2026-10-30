use sea_query::{
    Alias, ColumnRef, DynIden, Func, IntoIden, IntoTableRef, Query, SelectStatement, SimpleExpr,
    TableRef,
};

use crate::{
    Error, Result, Transpiler,
    functions::{Function, FunctionArgs, FunctionPattern, view},
    intermediate::{Rc, ToSql},
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::alias::DynTableAlias,
    types::{Cell, ColumnType, JsonObject, Type, TypedExpression},
};

use super::FunctionOrigin;

pub fn collect_object() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "collect_object".to_owned(),
            receiver: Some(view()),
            args: vec![],
            variadic: None,
            returns: Type::Cell(Cell::Value(JsonObject::AnyContent.into())).into(),
        },
        "Collect a multidimensional data view into a single json value",
        to_sql,
        FunctionOrigin::Sqcel,
    )
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    let select_statement = match args.receiver()?.returntype(tp) {
        Type::NamedView { fields, .. } => collect_object_recursive(
            tp,
            &fields
                .iter()
                .map(|(x, c)| (Alias::new(x).into_iden(), c))
                .collect::<Vec<_>>(),
            0,
            args.receiver()?
                .try_iterate(tp, tp.alias().into_iden())?
                .into_table_ref(),
        )?,
        x => return Err(Error::CanNotReduceType(Box::new(x))),
    };
    Ok(select_statement
        .into_expr()
        .with_type(Cell::Value(JsonObject::AnyContent.into())))
}

fn collect_object_recursive(
    tp: &Transpiler,
    cols: &[(DynIden, &ColumnType)],
    offset: usize,
    source: TableRef,
) -> Result<SelectStatement> {
    if cols.len() < offset + 1 {
        return Err(Error::NotEnoughColumns(offset + 1, cols.len()));
    }
    let expr = Func::cust("jsonb_object_agg")
        .arg(
            SimpleExpr::from(ColumnRef::Column(cols[offset].0.clone()))
                .with_type(cols[offset].1.clone())
                .force_cast(tp, &ColumnType::Text)?,
        )
        .arg(ColumnRef::Column(cols[offset + 1].0.clone()));

    let names = cols[..offset + 2].iter().map(|(x, _)| x.clone()).collect();
    let tbl_ref = if offset == cols.len() - 2 {
        source.alias(DynTableAlias(Rc::new((0, names))).into_iden())
    } else {
        TableRef::SubQuery(
            collect_object_recursive(tp, cols, offset + 1, source)?,
            DynTableAlias(Rc::new((0, names))).into_iden(),
        )
    };

    Ok(Query::select()
        .exprs(
            cols[..offset]
                .iter()
                .map(|x| ColumnRef::Column(x.0.clone())),
        )
        .expr(expr)
        .group_by_columns(
            cols[..offset]
                .iter()
                .map(|x| ColumnRef::Column(x.0.clone())),
        )
        .from(tbl_ref)
        .take())
}

#[cfg(test)]
mod test {

    use sea_query::PostgresQueryBuilder;

    use crate::{
        Result, Transpiler,
        hacks::get_plaintext_expression,
        intermediate::Rc,
        types::{ColumnType, JsonType},
    };

    fn compile(s: &str) -> Result<String> {
        let tp = Transpiler::quick(
            vec![(
                "sch",
                vec![(
                    "foo",
                    vec![
                        ("number", ColumnType::Integer),
                        ("liste", JsonType::List(Rc::new(JsonType::Any)).into()),
                    ],
                )],
            )]
            .into(),
        )
        .to_builder()
        .reduce(false)
        .build()
        .unwrap();

        get_plaintext_expression(s, &tp, PostgresQueryBuilder)
    }

    macro_rules! assert_sql {
        ($left:tt, $right:tt) => {
            let c = regex::Regex::new(r"[\n\s]+").unwrap();
            let left = compile($left).unwrap();
            let right = c.replace_all($right, " ");
            let right = right.replace("( ", "(").replace(" )", ")");
            let right = format!("({})", right.trim());

            assert_eq!(left, right);
        };
    }

    #[test]
    fn simple_recursive() {
        assert_sql!(
            "foo.group_by(x, x.number).count().collect_object()",
            r#"
                SELECT jsonb_object_agg(CAST("number" AS text), "count")
                FROM
                    (SELECT "x"."number", COUNT(*) FROM "foo" AS "x" GROUP BY "x"."number")
                AS "t_0"("number","count")
                
            "#
        );
    }

    #[test]
    fn double_recursive() {
        assert_sql!(
            "foo.group_by(x, x.number, x.liste).count().collect_object()",
            r#"
                SELECT jsonb_object_agg(CAST("number" AS text), "liste")
                FROM (
                    SELECT "number", jsonb_object_agg(CAST("liste" AS text), "count")
                    FROM (
                        SELECT "x"."number", "x"."liste", COUNT(*) 
                        FROM "foo" AS "x" GROUP BY "x"."number", "x"."liste"
                    ) AS "t_0"("number","liste","count")
                    GROUP BY "number"
                ) AS "t_0"("number","liste")
            "#
        );
    }
}
