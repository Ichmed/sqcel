use indexmap::IndexMap;
use sea_query::{Alias, IntoIden, SelectStatement};

use crate::{
    Error, Result, Transpiler,
    functions::{
        ArgPosition, Function, FunctionArgs, FunctionOrigin, FunctionPattern, Pattern, ReturnType,
        view,
    },
    intermediate::{Expression, Ident, ToSql},
    sql_extensions::{IntoSqlExpression, SqlExtension},
    structure::Table,
    types::{ColumnType, Type, TypedExpression},
};

pub fn group_by() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "group_by".to_owned(),
            receiver: Some(view()),
            args: vec![Pattern::Ident],
            variadic: Some(Pattern::Lambda),
            returns: ReturnType::Arg(ArgPosition::Receiver),
        },
        "pre-group by some expression, before aplying an aggregation",
        parser,
        FunctionOrigin::Sqcel,
    )
}

fn parser(tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
    let var = args.arg(0)?.as_single_ident()?;
    let mut buckets = args.receiver()?.try_as_buckets(tp, var)?;

    let fields: IndexMap<_, _> = buckets
        .layout
        .layout
        .iter()
        .map(|(k, v)| (k.clone(), v.ty.clone()))
        .collect();

    let tp_inner = tp.collect(&buckets);
    let groups = args
        .variadics(1)
        .iter()
        .map(|arg| modify_grouping(&tp_inner, &mut buckets.select, var, arg))
        .collect::<Result<_>>()?;

    Ok(buckets
        .select
        .into_expr()
        .with_type(Type::NamedView { fields, groups }))
}

fn modify_grouping(
    tp: &Transpiler,
    buckets: &mut SelectStatement,
    var: &Ident,
    arg: &Expression,
) -> Result<(String, ColumnType)> {
    let expr = arg.to_sql(tp)?;

    buckets.add_group_by([expr.expr.clone()]);

    let ty = expr
        .ty
        .col_type()
        .cloned()
        .ok_or(Error::WrongFunctionArgType(
            "group_by".to_owned(),
            Box::new(expr.ty.clone()),
        ))?;

    let key = if let Ok(name) = arg.as_field_on(var) {
        buckets.expr(expr);
        name.to_string()
    } else if let Type::Column(Some(name), _) = expr.ty {
        buckets.expr_as(expr.expr, Alias::new(name.clone()));
        name
    } else {
        let alias = tp.alias().into_iden();

        buckets.expr_as(expr, alias.clone());
        alias.to_string()
    };
    Ok((key, ty))
}

#[derive(Clone, Debug)]
pub struct Buckets {
    pub select: SelectStatement,
    pub layout: Table,
    pub groups: IndexMap<String, ColumnType>,
}

impl From<Buckets> for SelectStatement {
    fn from(value: Buckets) -> Self {
        value.select
    }
}

impl Transpiler {
    #[allow(clippy::missing_panics_doc, reason = "Will not panic")]
    pub fn collect(&self, iter: &Buckets) -> Self {
        self.to_builder()
            .add_temp_table_to_schema(iter.layout.clone())
            .build()
            .unwrap()
    }
}

// here.group_by(int(timestamp) / 10).count()
// here.group_by(int(timestamp) / 10).max(log_level)
