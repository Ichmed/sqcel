
use crate::{
    Result, Transpiler,
    functions::Function,
    intermediate::{Expression, Rc, ToSql},
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
    fn to_sql(&self, _tp: &crate::Transpiler) -> Result<TypedExpression> {
        // Ok(Query::select()
        //     .column(Asterisk)
        //     .from_subquery(self.0.to_record_set(tp)?, tp.alias())
        //     .limit(1)
        //     .take()
        //     .into_expr()
        //     .with_type(self.returntype(tp)))
        todo!()
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        self.0.returntype(tp)
    }
}
