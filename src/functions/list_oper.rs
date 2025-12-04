use crate::{
    Transpiler,
    intermediate::{Expression, Ident},
    structure::Column,
    transpiler::alias,
    types::{SqlType, Type, TypedExpression},
};
use sea_query::{Query, SimpleExpr, SubQueryOper, SubQueryStatement};

use crate::{
    Result,
    intermediate::{Rc, ToSql},
};

use super::Function;

#[derive(Debug)]
pub struct ListOper {
    source: Expression,
    var: Ident,
    predicate: Expression,
    oper: SubQueryOper,
    compare: Option<Expression>,
}

pub fn any(source: &Expression, var: &Expression, predicate: &Expression) -> Result<Rc<ListOper>> {
    Ok(Rc::new(ListOper {
        var: var.as_single_ident()?.clone(),
        source: source.clone(),
        predicate: predicate.clone(),
        compare: None,
        oper: SubQueryOper::Any,
    }))
}

pub fn all(source: &Expression, var: &Expression, predicate: &Expression) -> Result<Rc<ListOper>> {
    Ok(Rc::new(ListOper {
        var: var.as_single_ident()?.clone(),
        source: source.clone(),
        predicate: predicate.clone(),
        compare: None,
        oper: SubQueryOper::All,
    }))
}

impl ToSql for ListOper {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        // TODO: If the predicate is a single comparisson with a static
        // value, the static value can be moved outside the subquery

        let varname = self.var.to_string();
        let source = self.source.to_record_set(tp, alias(&varname))?;
        let inner_tp = tp
            .clone()
            .enter_anonymous_table([(varname.clone(), Column::new(varname, SqlType::JSON))].into());

        let compare = self
            .compare
            .as_ref()
            .map(|x| Result::Ok(x.to_sql(&inner_tp)?.expr))
            .transpose()?
            .unwrap_or(SimpleExpr::Constant(true.into()));
        let predicate = self
            .predicate
            .to_sql(&inner_tp)?
            .expr
            .cast_as(alias("bool"));

        let stream = SimpleExpr::SubQuery(
            Some(self.oper),
            Box::new(SubQueryStatement::SelectStatement(
                Query::select()
                    .expr(predicate)
                    .from_subquery(source, self.var.clone())
                    .take(),
            )),
        );

        Ok(TypedExpression {
            ty: Type::Sql(SqlType::Boolean),
            expr: compare.eq(stream),
        })
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        SqlType::Boolean.into()
    }
}

impl Function for ListOper {}

#[cfg(test)]
mod test {

    use sea_query::{Asterisk, PostgresQueryBuilder, Query};

    use crate::{
        hacks::postgres,
        intermediate::{ToIntermediate, ToSql},
        structure::*,
        transpiler::*,
        types::SqlType,
    };

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
                                .column(Column::new("my_data", SqlType::JSON))
                                .column_alias("my_alias", Column::new("hidden", SqlType::Boolean)),
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
            r#"SELECT * WHERE TRUE = ANY(SELECT CAST((CAST("x" AS integer) = -1) AS bool) FROM (SELECT jsonb_array_elements("my_data" -> 'list') AS "x") AS "x")"#
        );
    }

    #[test]
    fn de_sugar_any() {
        assert_eq!(postgres("[true, true].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[true, false].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[false, false].exists(x, x)").unwrap(), "FALSE");
    }
}
