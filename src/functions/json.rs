use sea_query::{
    Alias, ColumnRef, DynIden, Func, IntoIden, IntoTableRef, Query, SelectStatement, TableRef,
};

use crate::{
    Error, Result, Transpiler,
    functions::{Function, FunctionArgs, FunctionPattern},
    intermediate::{Rc, ToSql},
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::alias::DynTableAlias,
    types::{Cell, JsonObject, Type, TypedExpression},
};

use super::FunctionOrigin;

pub fn collect_object() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "collect_object".to_owned(),
            receiver: Some(Type::View(None).into()),
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
        Type::NamedView(index_map) => collect_object_recursive(
            &index_map
                .keys()
                .map(|x| Alias::new(x).into_iden())
                .collect::<Vec<_>>(),
            0,
            args.receiver()?
                .try_iterate(tp, tp.alias().into_iden())?
                .into_table_ref(),
        )?,
        x => return Err(Error::CanNotReduceType(x)),
    };
    Ok(select_statement
        .into_expr()
        .with_type(Cell::Value(JsonObject::AnyContent.into())))
}

fn collect_object_recursive(
    cols: &[DynIden],
    offset: usize,
    source: TableRef,
) -> Result<SelectStatement> {
    let expr = Func::cust("jsonb_object_agg")
        .arg(ColumnRef::Column(cols[offset].clone()))
        .arg(ColumnRef::Column(cols[offset + 1].clone()));
    let tbl_ref = if offset == cols.len() - 2 {
        source.alias(DynTableAlias(Rc::new((0, cols[..offset + 2].to_vec()))).into_iden())
    } else {
        TableRef::SubQuery(
            collect_object_recursive(cols, offset + 1, source)?,
            DynTableAlias(Rc::new((0, cols[..offset + 2].to_vec()))).into_iden(),
        )
    };
    Ok(Query::select()
        .exprs(cols[..offset].iter().map(|x| ColumnRef::Column(x.clone())))
        .expr(expr)
        .group_by_columns(cols[..offset].iter().map(|x| ColumnRef::Column(x.clone())))
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
            "foo.count(x, x.number).collect_object()",
            r#"
                SELECT jsonb_object_agg("c_0", "count")
                FROM
                    (SELECT "x"."number" AS "c_0", COUNT("x".*) FROM "foo" AS "x" GROUP BY "x"."number")
                AS "t_0"("c_0","count")
                
            "#
        );
    }

    #[test]
    fn double_recursive() {
        assert_sql!(
            "foo.count(x, x.number, x.liste).collect_object()",
            r#"
                SELECT jsonb_object_agg("c_0", "c_1")
                FROM (
                    SELECT "c_0", jsonb_object_agg("c_1", "count")
                    FROM (
                        SELECT "x"."number" AS "c_0", "x"."liste" AS "c_1", COUNT("x".*) 
                        FROM "foo" AS "x" GROUP BY "x"."number", "x"."liste"
                     ) AS "t_0"("c_0","c_1","count")
                    GROUP BY "c_0"
                 ) AS "t_0"("c_0","c_1")
            "#
        );
    }
}
