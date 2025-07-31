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
        let varname = self.var.to_string();
        let tp = tp
            .clone()
            .enter_anonymous_table([(varname.clone(), Column::new(varname, SqlType::JSON))].into());

        let compare = self
            .compare
            .as_ref()
            .map(|x| Result::Ok(x.to_sql(&tp)?.expr))
            .transpose()?
            .unwrap_or(SimpleExpr::Constant(true.into()));
        let predicate = self.predicate.to_sql(&tp)?.expr.cast_as(alias("bool"));
        let source = self.source.to_record_set(&tp, alias("_"))?;

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

    use crate::hacks::postgres;

    #[test]
    fn de_sugar_all() {
        assert_eq!(postgres("[true, false].all(x, x)").unwrap(), "FALSE");
        assert_eq!(postgres("[true, true].all(x, x)").unwrap(), "TRUE");
    }

    #[test]
    fn de_sugar_any() {
        assert_eq!(postgres("[true, true].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[true, false].exists(x, x)").unwrap(), "TRUE");
        assert_eq!(postgres("[false, false].exists(x, x)").unwrap(), "FALSE");
    }
}
