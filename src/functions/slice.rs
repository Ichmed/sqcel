// use sea_query::{Asterisk, Query};

// use crate::{
//     Result,
//     functions::{Function, subquery},
//     intermediate::{Expression, Rc, ToSql},
//     transpiler::alias,
// };

// pub struct First(Expression);

// impl First {
//     pub fn new(arg: &Expression) -> Result<Rc<Self>> {
//         Ok(Rc::new(Self(arg.clone())))
//     }
// }

// impl Function for First {
//     fn returntype(&self) -> super::FunctionReturn {
//         super::FunctionReturn::SubqueryAtom
//     }
// }

// impl ToSql for First {
//     fn to_sql(&self, tp: &crate::Transpiler) -> crate::Result<sea_query::SimpleExpr> {
//         Ok(subquery(
//             Query::select()
//                 .column(Asterisk)
//                 .from_subquery(self.0.to_record_set(tp, "_")?, alias("_"))
//                 .limit(1)
//                 .take(),
//         ))
//     }
// }
