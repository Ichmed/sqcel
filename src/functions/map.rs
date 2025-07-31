use super::{Function, subquery};
use crate::Transpiler;
use crate::intermediate::{Expression, ExpressionInner, Rc, ToSql};
use crate::structure::Column;
use crate::transpiler::alias;
use crate::types::{RecordSet, SqlType, Type, TypedExpression};
use crate::variables::{Object, Variable};
use crate::{Result, intermediate::Ident};
use sea_query::{ArrayType, ColumnRef, Func, Query, SimpleExpr, Value};

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

impl Function for Map {}

impl ToSql for Map {
    fn returntype(&self, _tp: &Transpiler) -> Type {
        Type::RecordSet(Box::new(RecordSet(Default::default(), true)))
    }

    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let varname = self.var.to_string();
        let tp = tp
            .clone()
            .enter_anonymous_table([(varname.clone(), Column::new(varname, SqlType::JSON))].into());

        let func = match self.func.as_single_ident() {
            Ok(func) if *func == self.var => SimpleExpr::Column(ColumnRef::Asterisk)
                .assume(&tp, RecordSet(Default::default(), true).into())?,
            _ => self.func.to_sql(&tp)?,
        };
        let filter = self
            .filter
            .as_ref()
            .map(|x| Result::Ok(x.to_sql(&tp)?.expr))
            .transpose()?;

        let mut q = Query::select();
        q.expr(func.expr).and_where_option(filter);

        Ok(subquery(
            match &*self.rec {
                ExpressionInner::Variable(Variable::Object(Object { data, .. })) => {
                    q.from_function(object_keys(data)?, self.var.clone())
                }
                _ => q.from_subquery(
                    self.rec.to_record_set(&tp, alias(self.var.clone()))?,
                    self.var.clone(),
                ),
            }
            .take(),
        )
        .assume_is(RecordSet(Default::default(), true).into()))
    }
}

fn object_keys(data: &[(Expression, Expression)]) -> Result<sea_query::FunctionCall> {
    Ok(Func::cust("unnest").arg(SimpleExpr::Constant(Value::Array(
        ArrayType::String,
        Some(Box::new(
            data.iter()
                .map(|(k, _)| k.as_str())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|k| sea_query::Value::String(Some(Box::new(k.to_owned()))))
                .collect(),
        )),
    ))))
}
