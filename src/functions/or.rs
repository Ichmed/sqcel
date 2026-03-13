use sea_query::Func;

use crate::{
    Result,
    functions::Function,
    intermediate::{Expression, ToSql},
    sql_extensions::SqlExtension,
    types2::{Type, TypedExpression},
};

pub struct Or {
    pub rec: Expression,
    pub alt: Expression,
}

impl Function for Or {}

impl ToSql for Or {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let base = self.rec.to_sql(tp)?.reshape(tp, &self.returntype(tp))?.expr;
        // let base = self.rec.to_sql(tp)?.expr;
        let alt = self.alt.to_sql(tp)?.expr;

        Ok(Func::coalesce([base, alt]).with_type(self.returntype(tp)))
    }

    fn returntype(&self, tp: &crate::Transpiler) -> Type {
        self.alt.returntype(tp)
    }
}
