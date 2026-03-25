use super::Function;
use crate::Transpiler;
use crate::intermediate::{Expression, Rc, ToSql};
use crate::sql_extensions::{IntoSqlExpression, SqlExtension};
use crate::types::{Type, TypedExpression};
use crate::{Result, intermediate::Ident};
use sea_query::{IntoIden, Query};

pub struct Map {
    rec: Expression,
    var: Ident,
    filter: Option<Expression>,
    func: Expression,
}

impl Map {
    pub fn new(
        rec: &Expression,
        var: &Expression,
        filter: Option<&Expression>,
        func: &Expression,
    ) -> Result<Rc<Self>> {
        Ok(Rc::new(Self {
            rec: (rec.clone()),
            var: var.as_single_ident()?.clone(),
            filter: filter.cloned(),
            func: func.clone(),
        }))
    }

    pub fn is_identity(&self) -> bool {
        self.is_filter() && self.filter.is_none()
    }

    pub fn is_filter(&self) -> bool {
        self.func.as_single_ident().is_ok_and(|f| *f == self.var)
    }
}

impl Function for Map {}

impl ToSql for Map {
    fn returntype(&self, tp: &Transpiler) -> Type {
        if self.is_identity() {
            return self.rec.returntype(tp);
        }
        let Ok(source) = self.rec.try_iterate(tp, self.var.clone().into_iden()) else {
            return Type::default();
        };

        match self.func.returntype(&tp.iterate(&source)) {
            Type::Cell(cell) => Type::Column(None, cell.col_type().clone()),
            x => x,
        }
    }

    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        if self.is_identity() {
            return self.rec.to_sql(tp);
        }

        let source = self.rec.try_iterate(tp, self.var.clone().into_iden())?;

        let inner_tp = tp.iterate(&source);

        Ok(Query::select()
            .expr(self.func.to_sql(&inner_tp)?)
            .from(source)
            .and_where_option(
                self.filter
                    .as_ref()
                    .map(|filter| filter.to_sql(&inner_tp))
                    .transpose()?
                    .map(|x| x.expr),
            )
            .take()
            .into_expr()
            .with_type(self.returntype(tp)))
    }
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
                        (
                            "liste",
                            ColumnType::Json(JsonType::List(Box::new(JsonType::Any)), false),
                        ),
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

    // #[test]
    // fn map() {
    #[test]
    fn map_json_array_literal() {
        assert_sql!(
            "[1, 2, 3].map(x, int(x) + 1)",
            r#"
                SELECT CAST("x" AS bigint) + 1 
                FROM jsonb_array_elements(jsonb_build_array(1, 2, 3)) AS "t_1"("x")
            "#
        );
    }

    #[test]
    fn map_json_array_objects_literal() {
        assert_sql!(
            r#"[{"number": 1}, {"number":2}].map(x, int(x.number) + 1)"#,
            r#"
                SELECT CAST(("x" ->> 'number') AS bigint) + 1 
                FROM jsonb_array_elements(jsonb_build_array(jsonb_build_object('number', 1), jsonb_build_object('number', 2))) AS "t_1"("x")
            "#
        );
    }

    #[test]
    fn map_table_column() {
        assert_sql!(
            r#"foo.map(x, int(x.number) + 1)"#,
            r#"
                SELECT "x"."number" + 1 
                FROM "foo" AS "x"
            "#
        );
    }

    #[test]
    fn map_column_containing_json_list() {
        assert_sql!(
            r#"foo.liste.map(x, int(x.number) + 1)"#,
            r#"
                SELECT CAST(("x" ->> 'number') AS bigint) + 1 
                FROM jsonb_array_elements("foo"."liste") AS "t_1"("x")
            "#
        );
    }

    #[test]
    fn identity_map() {
        let map_0 = compile("foo").unwrap();
        let map_1 = compile("foo.map(x, x)").unwrap();
        let map_2 = compile("foo.map(x, x).map(x, x)").unwrap();
        let map_3 = compile("foo.map(x, x).map(x, x).map(x, x)").unwrap();

        assert_eq!(map_0, map_1);
        assert_eq!(map_0, map_2);
        assert_eq!(map_0, map_3);
    }

    #[test]
    fn filter() {
        assert_sql!(
            "foo.filter(x, x.number == 4)",
            r#"
                SELECT "x".* 
                FROM "foo" AS "x" 
                WHERE "x"."number" = 4"#
        );
    }
}
