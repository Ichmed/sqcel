use crate::{
    Result, Transpiler,
    functions::Function,
    intermediate::{Expression, Rc, ToSql},
    types::{Type, TypedExpression},
    types::ColumnType,
};

struct Cast(Expression, ColumnType);
impl Function for Cast {}

impl ToSql for Cast {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(self.0.to_sql(tp)?.convert(tp, &self.1)?)
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        self.0
            .returntype(tp)
            .replace_type(&self.1)
            .unwrap_or_default()
    }
}

#[inline]
#[must_use]
pub fn cast(ty: impl Into<ColumnType>, exp: &Expression) -> Rc<dyn Function> {
    Rc::new(Cast(exp.clone(), ty.into()))
}
