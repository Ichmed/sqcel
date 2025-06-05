use crate::{
    intermediate::{Expression, Ident},
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
        let tp = tp.to_builder().column(self.var.as_str()).build();

        let compare = self
            .compare
            .as_ref()
            .map(|x| Result::Ok(x.to_sql(&tp)?.expr))
            .transpose()?
            .unwrap_or(SimpleExpr::Constant(true.into()));
        let predicate = self.predicate.to_sql(&tp)?.expr.cast_as(alias("bool"));
        let source = self.source.to_record_set(&tp, self.var.as_str())?;

        let stream = SimpleExpr::SubQuery(
            Some(self.oper),
            Box::new(SubQueryStatement::SelectStatement(
                Query::select()
                    .expr(predicate)
                    .from_subquery(source, alias("_"))
                    .take(),
            )),
        );

        Ok(TypedExpression {
            ty: Type::Sql(SqlType::Boolean),
            expr: compare.eq(stream),
        })
    }
}

impl Function for ListOper {
    fn returntype(&self) -> Type {
        Type::Sql(SqlType::Boolean)
    }
}

#[cfg(test)]
mod test {

    use crate::hacks::postgres;

    #[test]
    fn de_sugar_all() {
        let verbose = postgres("all([true, false], x, x)").unwrap();
        let short = postgres("all([true, false])").unwrap();

        let method = postgres("[true, false].all()").unwrap();

        assert_eq!(verbose, short);
        assert_eq!(verbose, method);
    }

    #[test]
    fn de_sugar_any() {
        let verbose = postgres("any([true, false], x, x)").unwrap();
        let short = postgres("any([true, false])").unwrap();

        let method = postgres("[true, false].any()").unwrap();

        assert_eq!(verbose, short);
        assert_eq!(verbose, method);
    }
}
