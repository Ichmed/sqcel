use std::{
    collections::HashMap,
    fmt::{Debug, Display, Write},
};

pub mod aggregate;
pub mod iter;
pub mod json;
pub mod optional;
pub mod reduce;
pub mod string;

use itertools::Itertools;

use crate::{
    Error, Result, Transpiler,
    functions::Pattern::Lambda,
    intermediate::{Expression, Rc, ToSql},
    types::{ColumnType, Type, TypedExpression},
};

type CustomFunctionParser = dyn Fn(&Transpiler, &FunctionArgs) -> std::prelude::v1::Result<TypedExpression, Error>
    + 'static
    + Send
    + Sync;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionOrigin {
    Cel,
    Disabled(String),
    Sqcel,
    Custom,
}

#[derive(Clone)]
pub struct Function {
    pattern: Rc<FunctionPattern>,
    parser: Rc<CustomFunctionParser>,
    description: String,
    origin: FunctionOrigin,
}

impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
            // && self.parser == other.parser
            && self.description == other.description
            && self.origin == other.origin
    }
}

impl Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Function")
            .field("pattern", &self.pattern)
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl Function {
    pub fn define(
        pattern: FunctionPattern,
        description: impl Into<String>,
        parser: impl Fn(&Transpiler, &FunctionArgs) -> Result<TypedExpression> + 'static + Send + Sync,
    ) -> Self {
        Self::define_with_origin(pattern, description, parser, FunctionOrigin::Custom)
    }

    fn define_with_origin(
        pattern: FunctionPattern,
        description: impl Into<String>,
        parser: impl Fn(&Transpiler, &FunctionArgs) -> Result<TypedExpression> + 'static + Send + Sync,
        origin: FunctionOrigin,
    ) -> Self {
        Self {
            pattern: Rc::new(pattern),
            parser: Rc::new(parser),
            description: description.into(),
            origin,
        }
    }

    fn define_disabled(
        pattern: FunctionPattern,
        description: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::define_with_origin(
            pattern,
            description,
            |_, _| unimplemented!(),
            FunctionOrigin::Disabled(reason.into()),
        )
    }

    fn do_match(&self, receiver: Option<&Expression>, args: &[Expression]) -> Result<&Self> {
        self.pattern
            .do_match(receiver, args)
            .map(|()| self)
            .and_then(|v| match self.origin {
                FunctionOrigin::Disabled(ref reason) => {
                    Err(Error::FunctionDisabled(reason.clone()))
                }
                _ => Ok(v),
            })
    }

    pub fn parse(&self, tp: &Transpiler, args: &FunctionArgs) -> Result<TypedExpression> {
        (self.parser)(tp, args)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionBundle {
    pub function: Rc<Function>,
    pub args: FunctionArgs,
}

impl ToSql for FunctionBundle {
    fn returntype(&self, tp: &Transpiler) -> Type {
        match (self.function.pattern).returns {
            ReturnType::Static(ref x) => x.clone(),
            ReturnType::SameAsReceiver => self.args.receiver().map_or_default(|x| x.returntype(tp)),
            ReturnType::SameAsReceiverInner => self
                .args
                .receiver()
                .map_or_default(|x| x.returntype(tp))
                .array_type()
                .cloned()
                .unwrap_or_default()
                .into(),
            ReturnType::SameAsArg(index) => {
                self.args.arg(index).map_or_default(|x| x.returntype(tp))
            }
            ReturnType::Custom(_, ref f) => f(tp, &self.args),
        }
    }

    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        self.function.parse(tp, &self.args)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionArgs {
    pub receiver: Option<Expression>,
    pub args: Vec<Expression>,
}

impl FunctionArgs {
    #[must_use]
    pub fn new(receiver: Option<Expression>, mut args: Vec<Expression>) -> Self {
        args.reverse();
        Self { receiver, args }
    }

    pub fn receiver(&self) -> Result<&Expression> {
        self.receiver.as_ref().ok_or(Error::PatternIsNotAMethod)
    }

    pub fn arg(&self, index: usize) -> Result<&Expression> {
        self.args.get(index).ok_or(Error::PatternWrongArgNumber)
    }

    #[must_use]
    pub fn variadics(&self, start: usize) -> &[Expression] {
        if start < self.args.len() {
            &self.args[start..]
        } else {
            &[]
        }
    }
}

#[derive(Debug, Clone)]
pub enum Pattern {
    /// Accept any expression of the given type
    ///
    /// If type is `Type::Unknown` accept all expressions
    Expression(Type),
    ExpressionCast(Type),
    Ident,
    Lambda,
    Custom {
        display: String,
        accepts: fn(&Expression) -> Result<()>,
    },
}

fn _a(x: Function) {
    fn i(_: impl Send + Sync) {}
    i(x);
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Expression(l0), Self::Expression(r0))
            | (Self::ExpressionCast(l0), Self::ExpressionCast(r0)) => l0 == r0,
            (Self::Custom { display: l, .. }, Self::Custom { display: r, .. }) => l == r,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Pattern {
    fn do_match(&self, expr: &Expression) -> Result<()> {
        match self {
            Self::Expression(_) | Self::ExpressionCast(_) | Lambda => Ok(()),
            Self::Custom { accepts, .. } => accepts(expr),
            Self::Ident => expr
                .as_single_ident()
                .map_err(|_| Error::PatternDoesNotMatch)
                .map(|_| ()),
        }
    }
}

impl<T: Into<Type>> From<T> for Pattern {
    fn from(value: T) -> Self {
        Self::Expression(value.into())
    }
}

impl Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expression(t) => t.col_type().cloned().unwrap_or_default().fmt_arg(f),
            Self::ExpressionCast(t) => {
                f.write_char('<')?;
                t.col_type().cloned().unwrap_or_default().fmt_arg(f)?;
                f.write_char('>')
            }
            Self::Custom {
                display,
                accepts: _,
            } => f.write_str(display),
            Self::Ident => f.write_char('x'),
            Self::Lambda => f.write_str("f(x)"),
        }
    }
}

type CustomReturnTypeFn = Rc<dyn Fn(&Transpiler, &FunctionArgs) -> Type + Send + Sync + 'static>;

#[derive(Clone)]
pub enum ReturnType {
    Static(Type),
    SameAsReceiver,
    SameAsReceiverInner,
    SameAsArg(usize),
    Custom(String, CustomReturnTypeFn),
}

impl PartialEq for ReturnType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Static(l0), Self::Static(r0)) => l0 == r0,
            (Self::SameAsArg(l0), Self::SameAsArg(r0)) => l0 == r0,
            (Self::Custom(l0, _), Self::Custom(r0, _)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Debug for ReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(arg0) => f.debug_tuple("Static").field(arg0).finish(),
            Self::SameAsReceiver => write!(f, "SameAsReceiver"),
            Self::SameAsReceiverInner => write!(f, "SameAsReceiverInner"),
            Self::SameAsArg(arg0) => f.debug_tuple("SameAsArg").field(arg0).finish(),
            Self::Custom(arg0, _) => f.debug_tuple("Custom").field(arg0).finish_non_exhaustive(),
        }
    }
}

impl<T: Into<Type>> From<T> for ReturnType {
    fn from(value: T) -> Self {
        Self::Static(value.into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionPattern {
    name: String,
    receiver: Option<Pattern>,
    args: Vec<Pattern>,
    variadic: Option<Pattern>,
    returns: ReturnType,
}

impl FunctionPattern {
    fn do_match(&self, receiver: Option<&Expression>, args: &[Expression]) -> Result<()> {
        match (receiver, &self.receiver) {
            (None, None) => None,
            (Some(receiver), Some(pattern)) => Some(pattern.do_match(receiver)?),
            (Some(_), None) => return Err(Error::PatternIsNotAMethod),
            (None, Some(_)) => return Err(Error::PatternIsAMethod),
        };

        let mut args_iter = args.iter();
        self.args
            .iter()
            .map(|pat| pat.do_match(args_iter.next().ok_or(Error::PatternWrongArgNumber)?))
            .collect::<Result<Vec<_>>>()?;
        args_iter
            .map(|exp| {
                self.variadic
                    .as_ref()
                    .ok_or(Error::PatternWrongArgNumber)?
                    .do_match(exp)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }
}

impl Display for FunctionPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(receiver) = &self.receiver {
            Display::fmt(&receiver, f)?;
            f.write_char('.')?;
        }
        f.write_str(&self.name)?;
        f.write_char('(')?;
        let mut args = self.args.iter();
        if let Some(pat) = args.next() {
            Display::fmt(&pat, f)?;
        }
        for pat in args.chain(&self.variadic) {
            f.write_str(", ")?;
            Display::fmt(&pat, f)?;
        }
        if self.variadic.is_some() {
            f.write_str("...")?;
        }
        f.write_str(") -> ")?;

        match &self.returns {
            ReturnType::Static(r) => r.col_type().cloned().unwrap_or_default().fmt_arg(f),
            ReturnType::SameAsReceiver => Debug::fmt(&self.receiver, f),
            ReturnType::SameAsReceiverInner => match &self.receiver {
                Some(Pattern::Expression(x)) => {
                    Debug::fmt(&x.inner_type().cloned().unwrap_or_default(), f)
                }
                _x => f.write_str("Content of Receiver"),
            },

            ReturnType::SameAsArg(i) => Debug::fmt(&self.args[*i], f),
            ReturnType::Custom(display, _) => f.write_str(display),
        }
    }
}

#[macro_export]
macro_rules! func_args {
    ($receiver:ident.$ident:ident ($($arg:expr),*) -> $rt:expr) => {{
            #[allow(unused_imports)]
            use $crate::types::ColumnType::*;
            $crate::functions::FunctionPattern {
                name: stringify!($ident).to_owned(),
                receiver: Some($receiver.into()),
                args: vec![$($arg.into(),)*],
                variadic: None,
                returns: $rt.into(),
            }}
    };
    ($ident:ident ($($arg:expr),*) -> $rt:expr) => {{
            #[allow(unused_imports)]
            use $crate::types::ColumnType::*;
            $crate::functions::FunctionPattern {
                name: stringify!($ident).to_owned(),
                receiver: None,
                args: vec![$($arg.into(),)*],
                variadic: None,
                returns: $rt.into(),
            }}
    };
}

#[derive(Debug)]
pub struct FunctionRegistry {
    inner: HashMap<String, Vec<Rc<Function>>>,
}

impl FunctionRegistry {
    pub fn register(&mut self, f: Function) -> &mut Self {
        self.inner
            .entry(f.pattern.name.clone())
            .or_default()
            .push(Rc::new(f));
        self
    }

    pub fn get(
        &self,
        name: &str,
        receiver: Option<&Expression>,
        args: &[Expression],
    ) -> Result<Rc<Function>> {
        let (matching_patterns, not_matching_patterns): (Vec<_>, _) = self
            .inner
            .get(name)
            .ok_or_else(|| Error::FunctionNotFound(name.to_owned()))?
            .iter()
            .map(|x| {
                x.do_match(receiver, args)
                    .map_err(|err| (x.pattern.clone(), err))
                    .map(|_| Rc::clone(x))
            })
            .partition_result();

        if let Some(m) = matching_patterns.first() {
            Ok(m.clone())
        } else {
            Err(Error::NotMatchingPattern(not_matching_patterns))
        }
    }
}

fn cast(name: &str, ty: ColumnType) -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: name.to_owned(),
            receiver: None,
            args: vec![Pattern::ExpressionCast(ty.clone().into())],
            variadic: None,
            returns: ty.clone().into(),
        },
        format!("Coerce the argument into a {name}"),
        move |tp, x| Ok(x.arg(0)?.to_sql(tp)?.convert(tp, &ty)?),
        FunctionOrigin::Cel,
    )
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        let mut reg = Self {
            inner: Default::default(),
        };

        reg.register(json::collect_object())
            .register(cast("uint", ColumnType::Unsigned))
            .register(cast("int", ColumnType::Integer))
            .register(cast("string", ColumnType::Text))
            .register(cast("bool", ColumnType::Boolean))
            .register(cast("double", ColumnType::Float))
            .register(cast("timestamp", ColumnType::TimestampWithTimeZone))
            .register(reduce::min())
            .register(reduce::max())
            .register(reduce::mean())
            .register(reduce::sum())
            .register(reduce::size())
            .register(aggregate::count())
            .register(iter::map())
            .register(iter::filter())
            .register(iter::filter_map())
            .register(iter::all())
            .register(iter::exists())
            .register(string::starts_with())
            .register(string::ends_with())
            .register(string::matches())
            .register(string::contains())
            .register(string::size())
            .register(optional::or());

        reg
    }
}

#[cfg(test)]
mod test {

    use crate::{Transpiler, functions::FunctionRegistry, intermediate::ToIntermediate};

    #[test]
    fn print() {
        let reg = FunctionRegistry::default();

        let tp = Transpiler::new().build().unwrap();

        let e = cel_parser::parse("this").unwrap().to_sqcel(&tp).unwrap();

        println!("{}", reg.get("or", Some(&e), &[]).unwrap_err());
    }
}
