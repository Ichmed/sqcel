use sea_query::{
    Alias, ColumnRef, DynIden, Func, IntoIden, IntoTableRef, Query, SelectStatement, TableRef,
};

use crate::{
    Error, Result, Transpiler,
    functions::Function,
    intermediate::{Expression, Rc, ToSql},
    sql_extensions::{IntoSqlExpression, SqlExtension},
    transpiler::alias::DynTableAlias,
    types::{Cell, JsonObject, Type, TypedExpression},
};

pub struct CollectJsonRecursive(pub Expression);

impl Function for CollectJsonRecursive {}

impl ToSql for CollectJsonRecursive {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        let var = tp.alias();
        Ok(match self.0.returntype(tp) {
            Type::NamedView(index_map) => collect_object_recursive(
                tp,
                index_map
                    .keys()
                    .map(|x| Alias::new(x).into_iden())
                    .collect(),
                0,
                self.0.try_iterate(tp, var.into_iden())?.into_table_ref(),
            )?,
            _ => return Err(Error::Todo("Can not reduce this type")),
        }
        .into_expr()
        .with_type(self.returntype(tp)))
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        Type::Cell(Cell::Value(JsonObject::AnyContent.into()))
    }
}

fn collect_object_recursive(
    tp: &Transpiler,
    cols: Vec<DynIden>,
    offset: usize,
    source: TableRef,
) -> Result<SelectStatement> {
    Ok(Query::select()
        .exprs(
            cols[..offset]
                .into_iter()
                .map(|x| ColumnRef::Column(x.clone())),
        )
        .expr(
            Func::cust("jsonb_object_agg")
                .arg(ColumnRef::Column(cols[offset].clone()))
                .arg(ColumnRef::Column(cols[offset + 1].clone())),
        )
        .group_by_columns(
            cols[..offset]
                .into_iter()
                .map(|x| ColumnRef::Column(x.clone())),
        )
        .from(if offset == cols.len() - 2 {
            source.alias(DynTableAlias(Rc::new((0, cols[..offset + 2].to_vec()))).into_iden())
        } else {
            TableRef::SubQuery(
                collect_object_recursive(tp, cols.clone(), offset + 1, source)?,
                DynTableAlias(Rc::new((0, cols[..offset + 2].to_vec()))).into_iden(),
            )
        })
        .take())
}

#[cfg(test)]
mod test {

    use sea_query::PostgresQueryBuilder;

    use crate::{
        Result, Transpiler,
        hacks::get_plaintext_expression,
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
                        ("liste", JsonType::List(Box::new(JsonType::Any)).into()),
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
                    (SELECT "x"."number" AS "c_0", COUNT("x") FROM "foo" AS "x" GROUP BY "x"."number")
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
                        SELECT "x"."number" AS "c_0", "x"."liste" AS "c_1", COUNT("x") 
                        FROM "foo" AS "x" GROUP BY "x"."number", "x"."liste"
                     ) AS "t_0"("c_0","c_1","count")
                    GROUP BY "c_0"
                 ) AS "t_0"("c_0","c_1")
            "#
        );
    }
}
