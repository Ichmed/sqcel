use sea_query::Query;

use super::Function;
use crate::{
    Transpiler,
    functions::subquery,
    intermediate::{Expression, Rc, ToSql},
    magic::{here, we},
    transpiler::{ParseError, Result},
    types::{RecordSet, Type, TypedExpression},
};

/// Returns the last `n` / all previous records that
/// fullfill some predicate
#[derive(Default)]
pub struct Latest {
    amount: Option<u64>,
    predicate: Option<Expression>,
}

impl Latest {
    pub fn new(amount: Option<&Expression>, predicate: Option<&Expression>) -> Result<Rc<Self>> {
        let amount = amount.map(Expression::as_postive_integer).transpose()?;
        Ok(Rc::new(Self {
            amount,
            predicate: predicate.cloned(),
        }))
    }
}

impl Function for Latest {}

impl ToSql for Latest {
    fn returntype(&self, _tp: &Transpiler) -> Type {
        RecordSet(Default::default(), true).into()
    }

    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let mut q = Query::select();

        let time = tp
            .layout
            .column(["time"])
            .ok_or(ParseError::Todo("No time column set"))?
            .0;

        q.order_by(time, sea_query::Order::Desc);

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
