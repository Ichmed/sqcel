use sea_query::Func;

use crate::{
    Result,
    functions::Function,
    intermediate::{Expression, ToSql},
    sql_extensions::SqlExtension,
    types::{Type, TypedExpression, agree::unify},
};

pub struct Or {
    pub rec: Expression,
    pub alt: Expression,
}

impl Function for Or {}

impl ToSql for Or {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let rt = self.returntype(tp);
        Ok(Func::coalesce([
            self.rec.to_sql(tp)?.reshape(tp, &rt)?.expr,
            self.alt.to_sql(tp)?.reshape(tp, &rt)?.expr,
        ])
        .with_type(rt))
    }

    fn returntype(&self, tp: &crate::Transpiler) -> Type {
        let a = self.rec.returntype(tp);
        let b = self.alt.returntype(tp);
        unify(a.clone(), b).unwrap_or(a)
    }
}
