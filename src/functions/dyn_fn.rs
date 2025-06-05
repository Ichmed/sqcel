use super::Function;
use crate::{
    Result, Transpiler,
    intermediate::{Expression, Rc, ToSql},
    types::{Type, TypedExpression},
    variables::Variable,
};
use std::fmt::Debug;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Signature<'a> {
    pub name: &'a str,
    pub rec: bool,
    pub args: usize,
}

pub(crate) struct DynFunc {
    pub(crate) f: Rc<dyn DynamicFunction + Send + Sync>,
    pub(crate) rec: Option<Expression>,
    pub(crate) args: Vec<Expression>,
    pub(crate) rt: Option<Type>,
}

pub trait DynamicFunction: Debug + Send + Sync {
    fn insert(
        &self,
        tp: &Transpiler,
        rec: Option<&Expression>,
        args: &[Expression],
    ) -> Result<TypedExpression>;
}

impl ToSql for DynFunc {
    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        self.f.insert(tp, self.rec.as_ref(), self.args.as_slice())
    }
}

impl Function for DynFunc {
    fn returntype(&self) -> Type {
        self.rt.clone().unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct CelFunction<'a> {
    pub(crate) code: Expression,
    #[allow(dead_code)]
    pub(crate) name: &'a str,
    pub(crate) method: bool,
    pub(crate) args: Vec<String>,
}

impl DynamicFunction for CelFunction<'_> {
    fn insert(
        &self,
        tp: &Transpiler,
        rec: Option<&Expression>,
        args: &[Expression],
    ) -> Result<TypedExpression> {
        let mut builder = tp.to_builder();
        if self.method {
            builder.var("this", rec.unwrap().as_variable().unwrap().clone());
        }
        for (key, val) in self.args.iter().zip(args) {
            match val.as_variable() {
                Some(val) => builder.var(key, val.clone()),
                None => builder.var(key, Variable::SqlAny(Box::new(val.to_sql(tp)?.expr))),
            };
        }
        self.code.to_sql(&builder.build())
    }
}
