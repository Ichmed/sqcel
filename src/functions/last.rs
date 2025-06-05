use super::{Function, subquery};
use crate::{
    intermediate::{Expression, Rc, ToSql},
    magic::{here, we},
    transpiler::{ParseError, Result, alias},
    types::{Type, TypedExpression},
};
use sea_query::{ColumnRef, Query};

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
    fn returntype(&self) -> Type {
        Type::RecordSet
    }
}

impl ToSql for Last {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let mut q = Query::select();

        let time = tp
            .columns
            .get("time")
            .ok_or(ParseError::Todo("No time column set"))?;

        q.order_by(ColumnRef::Column(alias(time)), sea_query::Order::Desc);

        if let Some(predicate) = self.predicate.as_ref() {
            q.expr(here().to_sql(tp)?.expr);
            q.and_where(predicate.to_sql(tp)?.expr);
        } else {
            q.expr(we().to_sql(tp)?.expr);
        }
        if let Some(amount) = self.amount {
            q.limit(amount);
        }

        Ok(subquery(q))
    }
}
