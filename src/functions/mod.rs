use std::{
    collections::HashMap,
    fmt::{Debug, Display, Write},
};

pub mod aggregate;
pub mod iter;
pub mod json;
pub mod optional;
pub mod reduce;
pub mod sort;
pub mod string;

use itertools::Itertools;

use crate::{
    Error, Result, Transpiler,
    intermediate::{
        Expression, Rc, ToSql, access_chain::json_to_iterable, to_sql::try_iterate_fallback,
    },
    types::{ColumnType, ConversionError, JsonType, Type, TypedExpression},
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
#[must_use]
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

    fn do_match(
        &self,
        tp: &Transpiler,
        receiver: Option<&Expression>,
        args: &[Expression],
    ) -> Result<&Self> {
        self.pattern
            .do_match(tp, receiver, args)
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
        self.determine_returntype(tp, &(self.function.pattern).returns)
    }

    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        self.function.parse(tp, &self.args)
    }

    fn try_iterate(
        &self,
        tp: &Transpiler,
        var: impl sea_query::IntoIden,
    ) -> Result<iter::Iterable> {
        let converted = self.to_sql(tp)?;
        let var = var.into_iden();

        match converted.ty.col_type().cloned() {
            Some(column_type) => json_to_iterable(tp, var, converted.expr, None, column_type),
            _ => try_iterate_fallback(self, tp, &var),
        }
    }
}

impl FunctionBundle {
    fn determine_returntype(&self, tp: &Transpiler, rt: &ReturnType) -> Type {
        match rt {
            ReturnType::Static(x) => x.clone(),
            ReturnType::Arg(ArgPosition::Receiver) => {
                self.args.receiver().map_or_default(|x| x.returntype(tp))
            }
            ReturnType::Arg(ArgPosition::Arg(n)) => {
                self.args.arg(*n).map_or_default(|x| x.returntype(tp))
            }
            ReturnType::Arg(ArgPosition::Variadic(n)) => self
                .args
                .variadics(*n)
                .first()
                .map_or_default(|x| x.returntype(tp)),
            ReturnType::ArgInner(x) => self
                .determine_returntype(tp, &ReturnType::Arg(x.clone()))
                .array_type()
                .cloned()
                .unwrap_or_default()
                .into(),
            ReturnType::ArgList(x) => Type::Column(
                None,
                self.determine_returntype(tp, &ReturnType::Arg(x.clone()))
                    .col_type()
                    .cloned()
                    .unwrap_or_default(),
            ),
            ReturnType::Custom(_, f) => f(tp, &self.args),
        }
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
    Iter(Type),
    IterCast(Type),
    Ident,
    Lambda,
    Custom {
        display: String,
        accepts: fn(&Transpiler, &Expression) -> Result<()>,
    },
}

fn view() -> Pattern {
    Pattern::Custom {
        display: "View".to_owned(),
        accepts: |tp, ex| {
            let returntype = ex.returntype(tp);
            (returntype == Type::Unknown || matches!(returntype, Type::NamedView { .. })).ok_or(
                ConversionError::CantConvert(
                    Box::new(returntype),
                    Box::new(Type::NamedView {
                        fields: Default::default(),
                        groups: Default::default(),
                    }),
                )
                .into(),
            )
        },
    }
}

fn column() -> Pattern {
    Pattern::Custom {
        display: "Column".to_owned(),
        accepts: |tp, ex| {
            let returntype = ex.returntype(tp);
            match returntype {
                Type::Unknown | Type::Column(_, _) | Type::Cell(_) => Ok(()),

                _ => Err(ConversionError::CantConvert(
                    Box::new(returntype),
                    Box::new(Type::Column(None, Default::default())),
                )
                .into()),
            }
        },
    }
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
    fn do_match(&self, tp: &Transpiler, position: ArgPosition, expr: &Expression) -> Result<()> {
        match self {
            Self::Expression(t) => {
                let returntype = expr.returntype(tp);
                if t.col_type() == Some(&ColumnType::Inferred) || returntype.is_subtype(t) {
                    Ok(())
                } else {
                    Err(Error::WrongArgumentType(
                        position,
                        Box::new(t.clone()),
                        Box::new(returntype),
                    ))
                }
            }
            Self::ExpressionCast(t) => {
                let returntype = expr.returntype(tp);
                if returntype.can_convert_to(t) {
                    Ok(())
                } else {
                    Err(Error::ConversionError(Box::new(
                        ConversionError::CantConvert(Box::new(returntype), Box::new(t.clone())),
                    )))
                }
            }
            Self::Iter(t) => {
                let iter_type = expr.returntype(tp).iter_type();
                if t.col_type() == Some(&ColumnType::Inferred) || iter_type.as_ref() == t.col_type()
                {
                    Ok(())
                } else {
                    Err(Error::PatternDoesNotMatch)
                }
            }
            Self::IterCast(t) => {
                let returntype = expr.returntype(tp);
                let iter_type = returntype.iter_type();

                if iter_type
                    .as_ref()
                    .and_then(|x| Some(x.can_convert_to(t.col_type()?)))
                    .unwrap_or_default()
                {
                    Ok(())
                } else {
                    Err(
                        Error::ConversionError(Box::new(ConversionError::CantConvert(
                            Box::new(returntype),
                            Box::new(t.clone()),
                        )))
                        .with_context("Could not match inner type"),
                    )
                }
            }
            Self::Lambda => Ok(()),
            Self::Custom { accepts, .. } => accepts(tp, expr),
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
            Self::Iter(t) => {
                f.write_char('[')?;
                t.col_type().cloned().unwrap_or_default().fmt_arg(f)?;
                f.write_char(']')
            }
            Self::IterCast(t) => {
                f.write_char('[')?;
                f.write_char('<')?;
                t.col_type().cloned().unwrap_or_default().fmt_arg(f)?;
                f.write_char('>')?;
                f.write_char(']')
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
    Arg(ArgPosition),
    ArgInner(ArgPosition),
    ArgList(ArgPosition),
    Custom(String, CustomReturnTypeFn),
}

impl Default for ReturnType {
    fn default() -> Self {
        Self::Static(Type::Unknown)
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ArgPosition {
    Receiver,
    Arg(usize),
    Variadic(usize),
}

impl Display for ArgPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Receiver => f.write_str("receiver"),
            Self::Arg(i) => f.write_fmt(format_args!("arg({i})")),
            Self::Variadic(_) => f.write_str("variadics..."),
        }
    }
}

impl PartialEq for ReturnType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Static(l0), Self::Static(r0)) => l0 == r0,
            (Self::Arg(l0), Self::Arg(r0))
            | (Self::ArgInner(l0), Self::ArgInner(r0))
            | (Self::ArgList(l0), Self::ArgList(r0)) => l0 == r0,
            (Self::Custom(l0, _), Self::Custom(r0, _)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Debug for ReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(arg0) => f.debug_tuple("Static").field(arg0).finish(),
            Self::Arg(arg0) => f.debug_tuple("Arg").field(arg0).finish(),
            Self::ArgInner(arg0) => f.debug_tuple("ArgInner").field(arg0).finish(),
            Self::ArgList(arg0) => f.debug_tuple("ArgList").field(arg0).finish(),
            Self::Custom(arg0, _) => f.debug_tuple("Custom").field(arg0).finish_non_exhaustive(),
        }
    }
}

impl<T: Into<Type>> From<T> for ReturnType {
    fn from(value: T) -> Self {
        Self::Static(value.into())
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FunctionPattern {
    pub name: String,
    pub receiver: Option<Pattern>,
    pub args: Vec<Pattern>,
    pub variadic: Option<Pattern>,
    pub returns: ReturnType,
}

impl FunctionPattern {
    fn do_match(
        &self,
        tp: &Transpiler,
        receiver: Option<&Expression>,
        args: &[Expression],
    ) -> Result<()> {
        match (receiver, &self.receiver) {
            (None, None) => None,
            (Some(receiver), Some(pattern)) => {
                Some(pattern.do_match(tp, ArgPosition::Receiver, receiver)?)
            }
            (Some(_), None) => return Err(Error::PatternIsNotAMethod),
            (None, Some(_)) => return Err(Error::PatternIsAMethod),
        };

        let mut args_iter = args.iter();
        self.args
            .iter()
            .enumerate()
            .map(|(i, pat)| {
                pat.do_match(
                    tp,
                    ArgPosition::Arg(i),
                    args_iter.next().ok_or(Error::PatternWrongArgNumber)?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        args_iter
            .map(|exp| {
                self.variadic
                    .as_ref()
                    .ok_or(Error::PatternWrongArgNumber)?
                    .do_match(tp, ArgPosition::Variadic(self.args.len()), exp)
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
            ReturnType::Arg(x) => f.write_str(&format!("Same as {x}")),
            ReturnType::ArgInner(x) => f.write_str(&format!("Content of {x}")),
            ReturnType::ArgList(x) => f.write_str(&format!("List of {x}")),
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

#[derive(Debug, Clone)]
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
        tp: &Transpiler,
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
                x.do_match(tp, receiver, args)
                    .map_err(|err| (x.pattern.clone(), err))
                    .map(|_| Rc::clone(x))
            })
            .partition_result();

        matching_patterns.into_iter().exactly_one().map_err(|i| {
            let v = i.map(|x| x.pattern.clone()).collect::<Vec<_>>();
            if v.is_empty() {
                Error::NoMatchingPattern(not_matching_patterns)
            } else {
                Error::MultipleMatchingPatterns(v)
            }
        })
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
        FunctionOrigin::Sqcel,
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
            .register(cast("dyn", JsonType::Any.into()))
            .register(cast("timestamp", ColumnType::TimestampWithTimeZone))
            .register(reduce::min())
            .register(reduce::max())
            .register(reduce::mean())
            .register(reduce::sum())
            .register(reduce::size())
            .register(aggregate::buckets::group_by())
            .register(aggregate::r#as::r#as())
            .register(aggregate::count::no_args())
            .register(aggregate::count())
            .register(aggregate::sum())
            .register(aggregate::mean())
            .register(aggregate::max())
            .register(aggregate::min())
            .register(iter::map())
            .register(iter::filter())
            .register(iter::filter_map())
            .register(iter::all())
            .register(iter::exists())
            .register(iter::join())
            .register(sort::sort())
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

        println!("{}", reg.get(&tp, "or", Some(&e), &[]).unwrap_err());
    }
}
