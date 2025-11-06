use crate::{
    functions::{
        WrongFunctionArgs,
        dyn_fn::{CelFunction, DynamicFunction, Signature},
    },
    intermediate::{Expression, Rc, ToIntermediate, ToSql},
    structure::{Column, SqlLayout, Table},
    types::{ConversionError, Type},
    variables::Variable,
};
use cel_interpreter::{Context, ExecutionError, Value as CelValue};
use cel_parser::Expression as CelExpr;
use derive_builder::Builder;
use miette::{Diagnostic, LabeledSpan};
use sea_query::{
    Alias, Asterisk, ColumnRef, DynIden, Query, SeaRc, SimpleExpr as SqlExpr, TableRef,
};
use std::{collections::HashMap, iter::once};
use thiserror::Error;

/// A simple example
///```
/// # use sqcel::{Transpiler, Query, PostgresQueryBuilder};
/// // Create a `Transpiler`
/// let my_expr = Transpiler::new().transpile("5").unwrap();
/// // Use a sea_query `Query` to actually use the result
/// assert_eq!(
///     Query::select().expr(my_expr).to_string(PostgresQueryBuilder),
///     r#"SELECT 5"#
/// )
///```
#[derive(Clone, Debug, Default, Builder)]
#[builder(default, build_fn(name = "_build", private))]
pub struct Transpiler {
    /// Identifiers included in vars will become
    /// constants during conversion (instead of bind params)
    pub(crate) variables: HashMap<String, Variable>,
    pub(crate) records: HashMap<String, String>,

    pub(crate) layout: SqlLayout,

    /// List of known message types
    pub(crate) types: HashMap<String, protobuf::descriptor::DescriptorProto>,

    pub(crate) functions: HashMap<Signature, (Rc<dyn DynamicFunction>, Option<Type>)>,

    /// Whether to accept unknown types
    ///
    /// Known types will still be valdiated
    pub(crate) accept_unknown_types: bool,

    /// Transform all column accesses into record accesses
    ///
    /// e.g. `foo` becomes `NEW."foo"`
    pub(crate) trigger_mode: bool,

    /// Should the transpiler try to reduce CEL expressions
    /// before translating them into SQL
    pub(crate) reduce: bool,
}

impl Transpiler {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> TranspilerBuilder {
        TranspilerBuilder::create_empty()
    }

    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        self.transpile_expr(cel_parser::parse(src)?)
    }

    pub fn transpile_expr(&self, expr: CelExpr) -> Result<SqlExpr> {
        expr.to_sqcel(self)?.to_sql(self).map(|x| x.expr)
    }

    pub fn to_builder(&self) -> TranspilerBuilder {
        let Transpiler {
            variables,
            records,

            types,
            accept_unknown_types,
            trigger_mode,
            reduce,
            functions,
            layout,
        } = self;
        TranspilerBuilder {
            variables: Some(variables.clone()),
            records: Some(records.clone()),
            types: Some(types.clone()),
            accept_unknown_types: Some(*accept_unknown_types),
            trigger_mode: Some(*trigger_mode),
            reduce: Some(*reduce),
            functions: Some(functions.clone()),
            layout: Some(layout.clone()),
        }
    }

    pub fn to_context(&self) -> Result<Context<'_>> {
        let mut c = Context::default();
        for (name, value) in self.variables.iter() {
            if let Ok(value) = CelValue::try_from(value.clone()) {
                c.add_variable_from_value(name, value)
            };
        }
        Ok(c)
    }

    pub fn enter_anonymous_table(mut self, content: HashMap<String, Column>) -> Self {
        self.layout.enter_anonymous_table(content);
        self
    }
}

impl TranspilerBuilder {
    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        self.transpile_expr(cel_parser::parse(src)?)
    }

    pub fn transpile_expr(&self, expr: CelExpr) -> Result<SqlExpr> {
        let tp = self._build()?;
        expr.to_sqcel(&tp)?.to_sql(&tp).map(|x| x.expr)
    }

    pub fn var(&mut self, key: impl ToString, val: impl Into<Variable>) -> &mut Self {
        self.variables
            .get_or_insert_default()
            .insert(key.to_string(), val.into());
        self
    }

    pub fn vars(
        &mut self,
        vars: impl IntoIterator<Item = (impl ToString, impl Into<Variable>)>,
    ) -> &mut Self {
        self.variables.get_or_insert_default().extend(
            vars.into_iter()
                .map(|(key, val)| (key.to_string(), val.into())),
        );
        self
    }

    pub fn build(&mut self) -> Transpiler {
        self._build().unwrap()
    }

    pub fn enter_schema(&mut self, name: impl ToString) -> &mut Self {
        self.layout.get_or_insert_default().enter_schema(name);
        self
    }

    pub fn enter_table(&mut self, name: impl ToString) -> &mut Self {
        self.layout.get_or_insert_default().enter_table(name);
        self
    }

    pub fn enter_anonymous_schema(&mut self, content: HashMap<String, Table>) -> &mut Self {
        self.layout
            .get_or_insert_default()
            .enter_anonymous_schema(content);
        self
    }

    pub fn enter_anonymous_table(&mut self, content: HashMap<String, Column>) -> &mut Self {
        self.layout
            .get_or_insert_default()
            .enter_anonymous_table(content);
        self
    }

    pub fn column(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<(ColumnRef, Option<Type>)> {
        self.layout.as_ref()?.column(name)
    }

    pub fn table(&self, name: impl IntoIterator<Item = impl ToString>) -> Option<TableRef> {
        self.layout.as_ref()?.table(name)
    }

    pub fn table_asterisk(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<ColumnRef> {
        self.layout.as_ref()?.table_asterisk(name)
    }

    pub fn record(&mut self, record: impl ToString) -> &mut Self {
        self.records
            .get_or_insert_default()
            .insert(record.to_string(), record.to_string());
        self
    }

    pub fn record_alias(&mut self, alias: impl ToString, record: impl ToString) -> &mut Self {
        self.records
            .get_or_insert_default()
            .insert(alias.to_string(), record.to_string());
        self
    }

    pub fn add_dyn_func(
        &mut self,
        name: impl ToString,
        method: bool,
        args: impl IntoIterator<Item = impl ToString>,
        code: impl Into<Expression>,
        rt: Option<Type>,
    ) -> Result<&mut Self> {
        let f = CelFunction {
            code: code.into(),
            name: name.to_string(),
            method,
            args: args.into_iter().map(|x| x.to_string()).collect(),
            rt: rt.clone(),
        };

        self.functions.get_or_insert_default().insert(
            Signature {
                name: name.to_string(),
                rec: method,
                args: f.args.len(),
            },
            (Rc::new(f), rt),
        );
        Ok(self)
    }

    pub fn add_cel_func(&mut self, code: impl AsRef<str>) -> Result<&mut Self> {
        let f = CelFunction::parse(&Default::default(), code.as_ref())?;
        self.add_dyn_func(&f.name, f.method, f.args, f.code, f.rt)
    }

    pub fn view(&mut self, name: &str, from: &str, selector: SqlExpr) -> Result<&mut Self> {
        let x = Query::select()
            .column(Asterisk)
            .from(alias(from))
            .and_where(selector)
            .take();

        let columns = self
            .layout
            .get_or_insert_default()
            .table_columns([from])
            .ok_or(ParseError::TableNotFound(from.to_owned()))?;

        self.var(name, Variable::SqlSubQuery(Box::new(x), columns));
        Ok(self)
    }
}

pub(crate) fn alias(s: impl ToString) -> DynIden {
    SeaRc::new(Alias::new(s.to_string()))
}

#[derive(Debug, Error, Diagnostic)]
pub enum ParseError {
    #[error("A non ident column, field or schema name was supplied")]
    NonIdentColumnFieldSchema,
    #[error(transparent)]
    CelError(CelParseError),
    #[error(transparent)]
    CelResolveError(#[from] ExecutionError),
    #[error("A non ident function name was supplied")]
    NonIdentFunctionName,
    #[error(
        "Transpiler bug: Tried to convert Members::Fields directly (should be caught in a previous stage)"
    )]
    BugDirectFields,
    #[error("Transpiler bug: Tried to bake a function item (should not propagate to the top)")]
    BugBakedFunction,
    #[error("Unknown message type `{}`", .0)]
    UnknownType(String),
    #[error("Unknown field `{}` on type `{}`", .0, .1)]
    UnknownField(String, String),
    #[error("Message of type `{}` is missing the fields: {:?}", .0, .1)]
    MissingFields(String, Vec<String>),

    #[error(transparent)]
    Builder(#[from] TranspilerBuilderError),

    #[error(transparent)]
    WrongFunctionArgs(#[from] WrongFunctionArgs),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),

    #[error("Schema \"{}\" could not be found in the layout", .0)]
    SchemaNotFound(String),

    #[error("Table \"{}\" could not be found in the layout", .0)]
    TableNotFound(String),

    #[error("Column \"{}\" could not be found in the layout", .0)]
    ColumnNotFound(String),

    #[error("TODO: {}", .0)]
    Todo(&'static str),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct CelParseError(cel_parser::ParseError);

impl Diagnostic for CelParseError {
    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        let start = self.0.span.start.clone()?.absolute;
        let end = self.0.span.end.clone()?.absolute;

        Some(Box::new(once(LabeledSpan::underline(start..end))))
    }
}

impl From<cel_parser::ParseError> for ParseError {
    fn from(value: cel_parser::ParseError) -> Self {
        ParseError::CelError(CelParseError(value))
    }
}

pub type Result<T> = std::result::Result<T, ParseError>;

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indoc::indoc;
    use sea_query::{PostgresQueryBuilder, Query};

    use crate::{
        structure::{Column, Database, Schema, SqlLayout, Table},
        transpiler::Transpiler,
        types::SqlType,
    };

    use super::TranspilerBuilder;

    #[test]
    fn test() {
        let tp = Transpiler::new().build();

        let input = indoc!(r#"[1, 2, 3].map(x, int(x) > 1, int(x) + 1)"#);
        let sql = helper(input, &tp);
        println!("{input}\n\n{sql}");
    }

    fn read_proto() -> HashMap<String, protobuf::descriptor::DescriptorProto> {
        protobuf_parse::Parser::new()
            .pure()
            .include(".")
            .input("test.proto")
            .parse_and_typecheck()
            .unwrap()
            .file_descriptors
            .remove(0)
            .message_type
            .into_iter()
            .map(|m| (m.name().to_owned(), m))
            .collect()
    }

    fn helper(code: &str, tp: &Transpiler) -> String {
        Query::select()
            .expr(tp.transpile(code).unwrap())
            .build(PostgresQueryBuilder)
            .0
    }

    #[test]
    #[should_panic]
    fn proto_unknwon_field() {
        TranspilerBuilder::create_empty()
            .types(read_proto())
            .transpile(r#"M{unknown: 5}"#)
            .unwrap();
    }

    #[test]
    fn proto_fields() {
        let tp = Transpiler::new().types(read_proto()).build();
        assert_eq!(
            helper(r#"M{needed: 5, not_needed: 5}"#, &tp),
            r#"SELECT jsonb_build_object('needed', 5, 'not_needed', 5)"#
        );
        assert_eq!(
            helper(r#"M{needed: 5}"#, &tp),
            r#"SELECT jsonb_build_object('needed', 5)"#
        );
    }
    #[test]
    #[should_panic]
    fn proto_missing_field() {
        TranspilerBuilder::create_empty()
            .types(read_proto())
            .build()
            .transpile(r#"M{not_needed: 5}"#)
            .unwrap();
    }

    #[test]
    fn table_access() {
        // let mut db = Database::default();
        // db.with_schema("my_sch")
        //     .with_table("my_tab")
        //     .with_column("my_col", SqlType::Integer)
        //     .with_column_alias("hidden", "my_alias", SqlType::Integer)
        //     .finish()
        //     .finish();

        let tp = TranspilerBuilder::create_empty()
            .layout({
                let mut layout = SqlLayout::new(
                    Database::new().schema(
                        Schema::new("my_sch").table(
                            Table::new("my_tab")
                                .column(Column::new("my_col", SqlType::Boolean))
                                .column_alias("my_alias", Column::new("hidden", SqlType::Boolean)),
                        ),
                    ),
                );
                layout.enter_schema("my_sch").enter_table("my_tab");
                layout
            })
            .build();

        assert_eq!(helper("my_col", &tp), r#"SELECT "my_col""#);
        assert_eq!(helper("my_alias", &tp), r#"SELECT "hidden""#);
        assert_eq!(helper("my_tab.my_col", &tp), r#"SELECT "my_tab"."my_col""#);
        assert_eq!(
            helper("my_sch.my_tab.my_col", &tp),
            r#"SELECT "my_sch"."my_tab"."my_col""#
        );
    }
}
