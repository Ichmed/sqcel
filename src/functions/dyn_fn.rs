use super::Function;
use crate::{
    Result, Transpiler,
    intermediate::{Expression, Rc, ToIntermediate, ToSql},
    transpiler::ParseError,
    types::{Type, TypedExpression},
    variables::Variable,
};
use std::fmt::Debug;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
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
pub struct CelFunction {
    pub(crate) code: Expression,
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) method: bool,
    pub(crate) args: Vec<String>,
    pub(crate) rt: Option<Type>,
}

impl DynamicFunction for CelFunction {
    fn insert(
        &self,
        tp: &Transpiler,
        rec: Option<&Expression>,
        args: &[Expression],
    ) -> Result<TypedExpression> {
        let mut builder = tp.to_builder();
        if self.method {
            builder.var("self", rec.unwrap().as_variable().unwrap().clone());
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

impl CelFunction {
    pub fn parse(tp: &Transpiler, code: &str) -> Result<Self> {
        let (name, rest) = code
            .split_once("(")
            .ok_or(ParseError::Todo("No arg-bracket"))?;

        let (args, rest) = rest
            .split_once(") ->")
            .ok_or(ParseError::Todo("No arrow"))?;

        let args = args.trim();
        let (method, args) = if !args.is_empty() {
            let args = args
                .split(",")
                .map(|s| s.trim().to_owned())
                .collect::<Vec<_>>();
            match args.as_slice() {
                [first, args @ ..] if first == "self" => (true, args.into()),
                _ => (false, args),
            }
        } else {
            (false, Vec::new())
        };

        let (ty, code) = rest.split_once(":").ok_or(ParseError::Todo("No colon"))?;

        Ok(Self {
            code: cel_parser::parse(code)?.to_sqcel(tp)?,
            name: name.to_string(),
            method,
            args,
            rt: None,
        })
    }
}

#[cfg(test)]
mod test {
    use sea_query::PostgresQueryBuilder;

    use crate::{Transpiler, functions::dyn_fn::CelFunction};

    #[test]
    fn parse_minimal() {
        CelFunction::parse(&Default::default(), "foo() -> null: null").unwrap();
    }

    #[test]
    fn use_identitiy() {
        let f = CelFunction::parse(&Default::default(), "identity(a) -> any: a").unwrap();

        let result = crate::hacks::get_plaintext_expression(
            "identity(1)",
            &Transpiler::new()
                .add_dyn_func(&f.name, f.method, f.args, f.code, f.rt)
                .unwrap()
                .build(),
            PostgresQueryBuilder,
        )
        .unwrap()
        .0;

        assert_eq!(result, "1");
    }

    #[test]
    fn use_add() {
        let f = CelFunction::parse(&Default::default(), "add(a, b) -> int: a + b").unwrap();
        let result = crate::hacks::get_plaintext_expression(
            "add(1, 2)",
            &Transpiler::new()
                .add_dyn_func(&f.name, f.method, f.args, f.code, f.rt)
                .unwrap()
                .build(),
            PostgresQueryBuilder,
        )
        .unwrap()
        .0;
        assert_eq!(result, "1 + 2");
    }
}
