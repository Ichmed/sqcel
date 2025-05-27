use crate::{
    intermediate::{ToIntermediate, ToSql},
    variables::Variable,
};
use cel_interpreter::{Context, ExecutionError, Value as CelValue};
use cel_parser::Expression as CelExpr;
use derive_builder::Builder;
use sea_query::{Alias, DynIden, SeaRc, SimpleExpr as SqlExpr};
use std::collections::HashMap;
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
/// Or more complex
///```
/// # use sqcel::{Transpiler, Query, PostgresQueryBuilder};
/// # use cel_parser::Atom;
/// // Create a `Transpiler` and set a variable and column
/// let tp = Transpiler::new().var("foo", 2).column("some_col").build();
/// let code = r#"int({"val": foo}.foo) + 1 + some_col"#;
/// let my_expr = tp.transpile(code).unwrap();
/// assert_eq!(
///     Query::select().expr(my_expr).to_string(PostgresQueryBuilder),
///     r#"SELECT CAST((CAST(jsonb_build_object('val', 2) AS jsonb) ->> 'foo') AS integer) + 1 + "some_col""#
/// )
///```
#[derive(Debug, Default, Builder)]
#[builder(default, build_fn(name = "_build", private))]
pub struct Transpiler {
    /// Identifiers included in vars will become
    /// constants during conversion (instead of bind params)
    pub(crate) variables: HashMap<String, Variable>,
    /// These identifiers are treated as column names
    pub(crate) columns: HashMap<String, String>,
    /// These identifiers are treated as table names
    ///
    /// Members of theses identifiers will be interpreted as column names
    pub(crate) tables: HashMap<String, String>,
    /// These identifiers are treated as record names. This is usually
    /// only usefull for use in triggers where the records `NEW` and `OLD`
    /// exist
    ///
    /// Members of theses identifiers will be interpreted as column names
    pub(crate) records: HashMap<String, String>,
    /// These identifiers are treated as schema names
    ///
    /// Members of theses identifiers will be interpreted as table names
    ///
    /// Members of those identifiers will be interpreted as column names
    pub(crate) schemas: HashMap<String, String>,

    /// List of known message types
    pub(crate) types: HashMap<String, protobuf::descriptor::DescriptorProto>,

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
        dbg!(expr.to_sqcel(self))?.to_sql(self)
    }

    pub fn to_builder(&self) -> TranspilerBuilder {
        let Transpiler {
            variables,
            columns,
            tables,
            records,
            schemas,
            types,
            accept_unknown_types,
            trigger_mode,
            reduce,
        } = self;
        TranspilerBuilder {
            variables: Some(variables.clone()),
            columns: Some(columns.clone()),
            tables: Some(tables.clone()),
            records: Some(records.clone()),
            schemas: Some(schemas.clone()),
            types: Some(types.clone()),
            accept_unknown_types: Some(*accept_unknown_types),
            trigger_mode: Some(*trigger_mode),
            reduce: Some(*reduce),
        }
    }

    pub fn to_context(&self) -> Result<Context> {
        let mut c = Context::default();
        for (name, value) in self.variables.iter() {
            if let Ok(value) = CelValue::try_from(value.clone()) {
                c.add_variable_from_value(name, value)
            };
        }
        Ok(c)
    }
}

impl TranspilerBuilder {
    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        self.transpile_expr(cel_parser::parse(src)?)
    }

    pub fn transpile_expr(&self, expr: CelExpr) -> Result<SqlExpr> {
        let tp = self._build()?;
        expr.to_sqcel(&tp)?.to_sql(&tp)
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

    pub fn column(&mut self, column: impl ToString) -> &mut Self {
        self.columns
            .get_or_insert_default()
            .insert(column.to_string(), column.to_string());
        self
    }

    pub fn column_alias(&mut self, alias: impl ToString, column: impl ToString) -> &mut Self {
        self.columns
            .get_or_insert_default()
            .insert(alias.to_string(), column.to_string());
        self
    }

    pub fn table(&mut self, table: impl ToString) -> &mut Self {
        self.tables
            .get_or_insert_default()
            .insert(table.to_string(), table.to_string());
        self
    }

    pub fn table_alias(&mut self, alias: impl ToString, table: impl ToString) -> &mut Self {
        self.tables
            .get_or_insert_default()
            .insert(alias.to_string(), table.to_string());
        self
    }

    pub fn schema(&mut self, schema: impl ToString) -> &mut Self {
        self.schemas
            .get_or_insert_default()
            .insert(schema.to_string(), schema.to_string());
        self
    }

    pub fn schema_alias(&mut self, alias: impl ToString, schema: impl ToString) -> &mut Self {
        self.schemas
            .get_or_insert_default()
            .insert(alias.to_string(), schema.to_string());
        self
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
}

pub(crate) fn alias(s: impl ToString) -> DynIden {
    SeaRc::new(Alias::new(s.to_string()))
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("A non ident column, field or schema name was supplied")]
    NonIdentColumnFieldSchema,
    #[error(transparent)]
    CelError(#[from] cel_parser::error::ParseError),
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
    #[error("Unknown field `{}` on type `{}`", .1, .0)]
    UnknownField(String, String),
    #[error("Message of type `{}` is missing the fields: {:?}", .0, .1)]
    MissingFields(String, Vec<String>),

    #[error(transparent)]
    Builder(#[from] TranspilerBuilderError),

    #[error("TODO")]
    Todo(&'static str),
}

pub type Result<T> = std::result::Result<T, ParseError>;

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indoc::indoc;
    use sea_query::{PostgresQueryBuilder, Query};

    use crate::transpiler::Transpiler;

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
        let q = Query::select()
            .expr(tp.transpile(code).unwrap())
            .build(PostgresQueryBuilder)
            .0;
        q
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
        let tp = TranspilerBuilder::create_empty()
            .column("my_col")
            .column_alias("my_alias", "hidden")
            .table("my_tab")
            .schema("my_sch")
            .build();

        assert_eq!(helper("my_col", &tp), r#"SELECT "my_col""#);
        assert_eq!(helper("my_alias", &tp), r#"SELECT "hidden""#);
        assert_eq!(helper("my_tab.my_col", &tp), r#"SELECT "my_tab"."my_col""#);
        assert_eq!(
            helper("my_sch.my_tab.my_col", &tp),
            r#"SELECT "my_sch"."my_tab"."my_col""#
        );
        assert_eq!(
            helper("my_sch.things.are.nice", &tp),
            r#"SELECT "my_sch"."things"."are" ->> 'nice'"#
        );
    }
}
