use crate::{
    Result, Transpiler,
    functions::Function,
    intermediate::{Expression, Rc, ToSql},
    types::{Type, TypeConversion, TypedExpression},
};

struct Cast(Expression, Type);
impl Function for Cast {}

impl ToSql for Cast {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(self.1.try_convert(tp, self.0.to_sql(tp)?)?)
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        self.1.clone()
    }
}

#[inline]
#[must_use]
pub fn cast(ty: Type, exp: &Expression) -> Rc<dyn Function> {
    Rc::new(Cast(exp.clone(), ty))
}
