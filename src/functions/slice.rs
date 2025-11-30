use sea_query::{Asterisk, Query};

use crate::{
    Result, Transpiler,
    functions::{Function, subquery},
    intermediate::{Expression, Rc, ToSql},
    transpiler::alias,
    types::{Type, TypedExpression},
};

pub struct First(Expression);

impl First {
    #[must_use]
    pub fn new(arg: &Expression) -> Rc<Self> {
        Rc::new(Self(arg.clone()))
    }
}

impl Function for First {}

impl ToSql for First {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        Ok(subquery(
            Query::select()
                .column(Asterisk)
                .from_subquery(self.0.to_record_set(tp, alias("_"))?, alias("_"))
                .limit(1)
                .take(),
        ))
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        self.0.returntype(tp)
    }
}
