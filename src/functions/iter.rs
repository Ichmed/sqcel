use super::{Function, FunctionOrigin};
use crate::functions::{FunctionArgs, FunctionPattern, Pattern, ReturnType};
use crate::intermediate::{Expression, ToSql};
use crate::sql_extensions::{IntoSqlExpression, SqlExtension};
use crate::types::{ColumnType, TypedExpression};
use crate::Result;
use indoc::indoc;
use sea_query::{
    IntoIden, IntoTableRef, Query, SimpleExpr, SubQueryOper, SubQueryStatement, TableRef,
};

use crate::{
    Transpiler,
    structure::{Column, Schema, Table},
};

#[derive(Clone, Debug)]
pub struct Iterable {
    pub expr: TableRef,
    pub kind: IterKind,
}

#[derive(Clone, Debug)]
pub enum IterKind {
    Column(Column),
    Table(Table),
    Schema(Schema),
}

impl IntoTableRef for Iterable {
    fn into_table_ref(self) -> TableRef {
        self.expr
    }
}

impl Transpiler {
    #[must_use]
    #[allow(clippy::missing_panics_doc, reason = "Will not panic")]
    pub fn iterate(&self, iter: &Iterable) -> Self {
        let mut b = self.to_builder();
        match &iter.kind {
            IterKind::Column(col) => b.add_temp_column_to_table(col.clone()),
            IterKind::Table(table) => b.add_temp_table_to_schema(table.clone()),
            IterKind::Schema(schema) => b.add_temp_schema_to_database(schema.clone()),
        }
        .build()
        .unwrap()
    }
}

pub fn map() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "map".into(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident, Pattern::Lambda],
            variadic: None,
            returns: ReturnType::SameAsArg(1),
        },
        indoc!(
            r"
        *   transforms a list `e` by taking each element `x` to the
            function given by the expression `t`, which can use the variable `x`. For
            instance, `[1, 2, 3].map(n, n * n)` evaluates to `[1, 4, 9]`. Any evaluation
            error for any element causes the macro to raise an error.
        *   transforms a map `e` by taking each key in the map `x` to the function
            given by the expression `t`, which can use the variable `x`. For
            instance, `{'one': 1, 'two': 2}.map(k, k)` evaluates to `['one', 'two']`.
            Any evaluation error for any element causes the macro to raise an error.
        "
        ),
        map_body,
        FunctionOrigin::Cel,
    )
}

fn map_body(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    filter_map_impl(tp, args.receiver()?, args.arg(0)?, None, Some(args.arg(1)?))
}

pub fn filter() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "filter".into(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident, Pattern::Lambda],
            variadic: None,
            returns: ReturnType::SameAsReceiver,
        },
        indoc!(
            r"
        *   for a list `e`, returns the sublist of all elements `x` which
            evaluate to `true` in the predicate expression `p` (which can use variable
            `x`). For instance, `[1, 2, 3].filter(i, i % 2 > 0)` evaluates to `[1, 3]`.
            If no elements evaluate to `true`, the result is an empty list. Any
            evaluation error for any element causes the macro to raise an error.
        *   for a map `e`, returns the list of all map keys `x` which
            evaluate to `true` in the predicate expression `p` (which can use variable
            `x`). For instance, `{'one': 1, 'two': 2}.filter(k, k == 'one')` evaluates
            to `['one']`. If no elements evaluate to `true`, the result is an empty
            list. Any evaluation error for any element causes the macro to raise an error.
            "
        ),
        filter_body,
        FunctionOrigin::Cel,
    )
}

fn filter_body(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    filter_map_impl(tp, args.receiver()?, args.arg(0)?, Some(args.arg(1)?), None)
}

pub fn filter_map() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "map".into(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident, Pattern::Lambda, Pattern::Lambda],
            variadic: None,
            returns: ReturnType::SameAsArg(2),
        },
        "Same as the two-arg map but with a conditional p filter before the value is transformed.",
        filter_map_body,
        FunctionOrigin::Cel,
    )
}

fn filter_map_body(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    filter_map_impl(
        tp,
        args.receiver()?,
        args.arg(0)?,
        Some(args.arg(1)?),
        Some(args.arg(2)?),
    )
}

fn filter_map_impl(
    tp: &Transpiler,
    receiver: &Expression,
    var: &Expression,
    filter: Option<&Expression>,
    lambda: Option<&Expression>,
) -> Result<TypedExpression> {
    if is_identity_function(var, filter, lambda) {
        return receiver.to_sql(tp);
    }
    let source = receiver.try_iterate(tp, var.clone().as_single_ident()?.into_iden())?;

    let tp = tp.iterate(&source);

    let expr = lambda
        .map_or_else(|| var.to_sql(&tp), |x| x.to_sql(&tp))?
        .expr;
    let filter = filter.map(|x| x.to_sql(&tp)).transpose()?.map(|x| x.expr);

    Ok(Query::select()
        .expr(expr)
        .from(source)
        .and_where_option(filter)
        .take()
        .into_expr()
        .with_type(lambda.unwrap_or(var).returntype(&tp)))
}

fn is_identity_function(
    var: &Expression,
    filter: Option<&Expression>,
    lambda: Option<&Expression>,
) -> bool {
    let is_none = filter.is_none_or(|filter| *filter == true.into());
    let is_identity = lambda.and_then(|x| x.as_single_ident().ok()) == var.as_single_ident().ok();
    is_none && is_identity
}

#[must_use]
pub fn exists() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "exists".into(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident, Pattern::Lambda],
            variadic: None,
            returns: ColumnType::Boolean.into(),
        },
        indoc!(
            r#"
            like the all() macro, but combines the predicate results with the "or" (||) operator, 
            so if any predicate evaluates to true, the macro evaluates to true, 
            ignoring any errors from other predicates.
            "#
        ),
        |tp, args| to_sql(tp, args, SubQueryOper::Any),
        FunctionOrigin::Cel,
    )
}

#[must_use]
pub fn all() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "all".into(),
            receiver: Some(ColumnType::Inferred.into()),
            args: vec![Pattern::Ident, Pattern::Lambda],
            variadic: None,
            returns: ColumnType::Boolean.into(),
        },
        indoc!(
            r#"
            tests whether a predicate holds for all elements of a list e or keys of a map e. 
            Here x is a simple identifier to be used in p which binds to the element or key. 
            The all() macro combines per-element predicate results with the "and" (&&) operator, 
            so if any predicate evaluates to false, the macro evaluates to false, 
            ignoring any errors from other predicates.
            "#
        ),
        |tp, args| to_sql(tp, args, SubQueryOper::All),
        FunctionOrigin::Cel,
    )
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs, oper: SubQueryOper) -> Result<TypedExpression> {
    // TODO: If the predicate is a single comparisson with a static
    // value, the static value can be moved outside the subquery
    // e.g. `<static> = ANY (<subquery>)`
    // e.g. `<static> = ALL (<subquery>)`

    let source = args
        .receiver()?
        .try_iterate(tp, args.arg(0)?.as_single_ident()?)?;
    let inner_tp = tp.iterate(&source);

    let compare = SimpleExpr::Constant(true.into());
    let predicate = args
        .arg(1)?
        .to_sql(&inner_tp)?
        .expr
        .cast_as(ColumnType::Boolean);

    let stream = SimpleExpr::SubQuery(
        Some(oper),
        Box::new(SubQueryStatement::SelectStatement(
            Query::select().expr(predicate).from(source).take(),
        )),
    );

    Ok(compare.eq(stream).with_type(ColumnType::Boolean))
}

#[cfg(test)]
mod test {
    use sea_query::PostgresQueryBuilder;
    use sea_query::{Asterisk, Query};

    use crate::{
        Result, Transpiler,
        hacks::get_plaintext_expression,
        hacks::postgres,
        intermediate::{ToIntermediate, ToSql},
        structure::*,
        transpiler::*,
        types::ColumnType,
        types::JsonType,
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
                WHERE "x"."number" = 4
            "#
        );
    }

    #[test]
    fn de_sugar_all() {
        assert_eq!(postgres("[true, false].all(x, x)").unwrap(), "FALSE");
        assert_eq!(postgres("[true, true].all(x, x)").unwrap(), "TRUE");
    }

    #[test]
    fn table_access_all() {
        let tp = TranspilerBuilder::default()
            .layout({
                let mut layout = SqlLayout::new(
                    Database::new().schema(
                        Schema::new("my_sch").table(
                            Table::new("my_tab")
                                .column(Column::new("my_data", JsonType::Any))
                                .column_alias(
                                    "my_alias",
                                    Column::new("hidden", ColumnType::Boolean),
                                ),
                        ),
                    ),
                );
                layout.enter_schema("my_sch").enter_table("my_tab");
                layout
            })
            .reduce(false)
            .build()
            .unwrap();

        let q = cel_parser::parse("my_data.list.exists(x, int(x) == -1)")
            .unwrap()
            .to_sqcel(&tp)
            .unwrap()
            .to_sql(&tp)
            .unwrap();

        let sql = Query::select()
            .column(Asterisk)
            .and_where(q.expr)
            .take()
            .build(PostgresQueryBuilder)
            .0;

        eprintln!("{sql}");

        assert_eq!(
            sql,
            r#"SELECT * WHERE TRUE = ANY(SELECT CAST((CAST("x" AS bigint) = -1) AS bool) FROM jsonb_array_elements("my_data" -> 'list') AS "t_1"("x"))"#
        );
    }

    #[test]
    fn de_sugar_any() {
        assert_eq!(postgres("[true, true].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[true, false].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[false, false].exists(x, x)").unwrap(), "FALSE");
    }
}
