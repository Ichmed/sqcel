use super::{Function, FunctionReturn, subquery};
use crate::intermediate::{Expression, Rc, ToSql};
use crate::transpiler::alias;
use crate::variables::{Object, Variable};
use crate::{Result, intermediate::Ident};
use sea_query::{ArrayType, Func, Query, SimpleExpr, Value};

pub struct Map {
    rec: Expression,
    var: Ident,
    filter: Option<Expression>,
    func: Expression,
}

impl Map {
    pub fn new(
        rec: &Expression,
        var: &Expression,
        filter: Option<&Expression>,
        func: &Expression,
    ) -> Result<Rc<Self>> {
        Ok(Rc::new(Self {
            rec: (rec.clone()),
            var: var.as_single_ident()?.clone(),
            filter: filter.cloned(),
            func: func.clone(),
        }))
    }
}

impl Function for Map {
    fn returntype(&self) -> super::FunctionReturn {
        FunctionReturn::Array
    }
}

impl ToSql for Map {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<SimpleExpr> {
        let tp = tp.to_builder().column(&self.var).build();

        let func = self.func.to_sql(&tp)?;
        let rec = match &self.rec {
            Expression::Variable(Variable::Object(Object { data, .. })) => Query::select()
                .expr_as(
                    SimpleExpr::Constant(Value::Array(
                        ArrayType::String,
                        Some(Box::new(
                            data.iter()
                                .map(|(k, _)| k.as_str())
                                .collect::<Result<Vec<_>>>()?
                                .into_iter()
                                .map(|k| sea_query::Value::String(Some(Box::new(k.to_owned()))))
                                .collect(),
                        )),
                    )),
                    alias(self.var.as_str()),
                )
                .take(),
            rec => rec.to_record_set(&tp, self.var.as_str())?,
        };
        let filter = self.filter.as_ref().map(|x| x.to_sql(&tp)).transpose()?;

        Ok(SimpleExpr::FunctionCall(
            Func::cust(alias("array")).arg(subquery(
                Query::select()
                    .expr(func)
                    .from_subquery(rec, alias("_"))
                    .and_where_option(filter)
                    .take(),
            )),
        ))
    }
}
