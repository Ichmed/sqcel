use indexmap::IndexMap;
use sea_query::{Alias, ColumnRef, Func, IntoIden, SimpleExpr};

use crate::{
    Result, Transpiler,
    functions::{
        Function, FunctionArgs, FunctionOrigin, FunctionPattern, Pattern, ReturnType,
        reduce::Reducer, view,
    },
    intermediate::{Rc, ToSql},
    magic,
    sql_extensions::{IntoSqlExpression, SqlExtension},
    types::{ColumnType, Type, TypedExpression},
};

pub mod r#as;
pub mod buckets;
pub mod count;

pub fn count() -> Function {
    aggregate(Reducer::Count)
}

pub fn sum() -> Function {
    aggregate(Reducer::Sum)
}

pub fn mean() -> Function {
    aggregate(Reducer::Mean)
}

pub fn max() -> Function {
    aggregate(Reducer::Max)
}

pub fn min() -> Function {
    aggregate(Reducer::Min)
}

pub fn aggregate(reducer: Reducer) -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: reducer.as_str().to_owned(),
            receiver: Some(view()),
            args: vec![Pattern::Ident],
            variadic: Some(Pattern::Lambda),
            returns: ReturnType::Custom(
                "View".to_owned(),
                Rc::new(move |tp, args| return_type(tp, args, reducer).unwrap_or_default()),
            ),
        },
        "Count the elements in the array, object or view. Optionally group by columns",
        move |tp, args| to_sql(tp, args, reducer),
        FunctionOrigin::Sqcel,
    )
}

fn to_sql(tp: &Transpiler, args: &FunctionArgs, reducer: Reducer) -> Result<TypedExpression> {
    let var_expression = args.arg(0).cloned().unwrap_or_else(|_| magic::x());
    let var = var_expression.as_single_ident()?.into_iden();
    let bucketed = args.receiver()?.try_as_buckets(tp, var)?;

    let exprs = if args.args.is_empty() && reducer == Reducer::Count {
        vec![
            Func::count(SimpleExpr::Column(ColumnRef::Asterisk)).with_type(Type::Column(
                Some("count".to_owned()),
                ColumnType::BigInteger,
            )),
        ]
    } else {
        let tp_inner = tp.collect(&bucketed);
        args.variadics(1)
            .iter()
            .map(|v| {
                let TypedExpression { expr, ty } = v.to_sql(&tp_inner)?;
                let ty = match (reducer, ty) {
                    (Reducer::Count, Type::Column(name, _)) => {
                        Type::Column(name, ColumnType::BigInteger)
                    }
                    (_, ty) => ty,
                };
                Ok(match reducer {
                    Reducer::Count => Func::count(expr),
                    Reducer::Min => Func::min(expr),
                    Reducer::Max => Func::max(expr),
                    Reducer::Sum => Func::sum(expr),
                    Reducer::Mean => Func::avg(expr),
                }
                .with_type(ty))
            })
            .collect::<Result<_>>()?
    };

    let ty = return_type_inner(bucketed.groups, exprs.iter().map(|x| &x.ty));
    let mut returned_view = bucketed.select;
    for expr in exprs {
        match expr.ty {
            Type::Column(Some(name), _) if dbg!(&name) != dbg!(reducer.as_str()) => {
                returned_view.expr_as(expr.expr, Alias::new(name.clone()));
            }
            _ => {
                returned_view.expr(expr);
            }
        }
    }

    Ok(returned_view.take().into_expr().with_type(ty))
}

fn return_type(tp: &Transpiler, args: &FunctionArgs, reducer: Reducer) -> Result<Type> {
    let var_expression = args.arg(0).cloned().unwrap_or_else(|_| magic::x());
    let var = var_expression.as_single_ident()?.into_iden();
    let bucketed = args.receiver()?.try_as_buckets(tp, var)?;

    let tys = if args.args.is_empty() && reducer == Reducer::Count {
        vec![Type::Column(
            Some("count".to_owned()),
            ColumnType::BigInteger,
        )]
    } else {
        let tp_inner = tp.collect(&bucketed);
        args.variadics(1)
            .iter()
            .map(|v| {
                let ty = v.returntype(&tp_inner);
                match (reducer, ty) {
                    (Reducer::Count, Type::Column(name, _)) => {
                        Type::Column(name, ColumnType::BigInteger)
                    }
                    (_, ty) => ty,
                }
            })
            .collect()
    };

    Ok(return_type_inner(bucketed.groups, tys.iter()))
}

fn return_type_inner<'a>(
    groups: IndexMap<String, ColumnType>,
    fields: impl Iterator<Item = &'a Type>,
) -> Type {
    Type::NamedView {
        fields: groups
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(fields.filter_map(|x| match x.clone() {
                Type::Column(name, t) => name.map(|name| (name, t)),
                _ => None,
            }))
            .collect(),
        groups,
    }
}
