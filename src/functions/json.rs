
use crate::{
    functions::Function,
    intermediate::{Expression, ToSql},
    types::{JsonType, TypeConversion},
};

/// Coerce a non-json database type to json
/// or explicity erase the type info on this json
///
/// Equivalent to CEL `dyn(_)`
pub struct Json(pub Expression);

impl Function for Json {}

impl ToSql for Json {
    fn to_sql(&self, tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Ok(JsonType::Any.try_convert(self.0.to_sql(tp)?)?)
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        JsonType::Any.into()
    }
}
