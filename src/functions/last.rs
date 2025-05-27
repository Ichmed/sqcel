use super::{Function, subquery};
use crate::{
    intermediate::{Expression, Rc, ToSql},
    magic::{here, we},
    transpiler::{Result, alias},
};
use sea_query::{ColumnRef, Query, SimpleExpr};

/// Returns the last `n` / all previous records that
/// fullfill some predicate
#[derive(Default)]
pub struct Last {
    amount: Option<u64>,
    predicate: Option<Expression>,
}

impl Last {
    pub fn new(amount: Option<&Expression>, predicate: Option<&Expression>) -> Result<Rc<Self>> {
        let amount = amount.map(Expression::as_postive_integer).transpose()?;
        Ok(Rc::new(Self {
            amount,
            predicate: predicate.cloned(),
        }))
    }
}

impl Function for Last {
    fn returntype(&self) -> super::FunctionReturn {
        super::FunctionReturn::SubqueryList
    }
}

impl ToSql for Last {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<SimpleExpr> {
        let mut q = Query::select();

        q.order_by(ColumnRef::Column(alias("time")), sea_query::Order::Desc);

        if let Some(predicate) = self.predicate.as_ref() {
            q.expr(here().to_sql(tp)?);
            q.and_where(predicate.to_sql(tp)?);
        } else {
            q.expr(we().to_sql(tp)?);
        }
        if let Some(amount) = self.amount {
            q.limit(amount);
        }

        Ok(subquery(q))
    }
}
