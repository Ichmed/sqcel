use std::collections::HashMap;

use crate::{
    transpiler::{TranspilerBuilder, TranspilerBuilderError, views::ViewSource},
    types::{SqlType, Type},
};
use sea_query::{ColumnRef, TableRef};

use crate::transpiler::alias;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlLayout {
    database: Database,
    schema: Option<Schema>,
    table: Option<Table>,
}

impl SqlLayout {
    #[must_use]
    pub fn new(database: Database) -> Self {
        Self {
            database,
            ..Default::default()
        }
    }
}

pub(crate) type LayoutTuple<'a> = Vec<(&'a str, Vec<(&'a str, Vec<(&'a str, SqlType)>)>)>;

impl From<LayoutTuple<'static>> for SqlLayout {
    fn from(value: LayoutTuple) -> Self {
        let mut db = Database::new();
        for (name, schema) in value {
            let mut sch = Schema::new(name);
            for (name, table) in schema {
                let mut tab = Table::new(name);
                for column in table {
                    tab = tab.column(column);
                }
                sch = sch.table(tab);
            }
            db = db.schema(sch);
        }
        Self::new(db)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        structure::{Column, Database, Schema, SqlLayout, Table},
        types::SqlType,
    };

    #[test]
    fn quick_layout() {
        let tpf = SqlLayout::from(vec![(
            "sch",
            vec![("foo", vec![("number", SqlType::Integer)])],
        )]);

        let tp = SqlLayout::new(
            Database::new().schema(
                Schema::new("sch")
                    .table(Table::new("foo").column(Column::new("number", SqlType::Integer))),
            ),
        );

        assert_eq!(tpf, tp);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Database {
    layout: HashMap<String, Schema>,
}

impl Database {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn schema(self, schema: Schema) -> Self {
        self.schema_alias(schema.1.clone(), schema)
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn schema_alias(mut self, name: impl ToString, schema: Schema) -> Self {
        self.layout.insert(name.to_string(), schema);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema(HashMap<String, Table>, String);

impl Schema {
    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn new(name: impl ToString) -> Self {
        Self(Default::default(), name.to_string())
    }

    #[must_use]
    pub fn table(self, table: Table) -> Self {
        self.table_alias(table.name.clone(), table)
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn table_alias(mut self, name: impl ToString, table: Table) -> Self {
        self.0.insert(name.to_string(), table);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    layout: HashMap<String, Column>,
    name: String,
}

impl Table {
    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn new(name: impl ToString) -> Self {
        Self {
            layout: Default::default(),
            name: name.to_string(),
        }
    }

    #[must_use]
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<Column>>) -> Self {
        for c in columns {
            self = self.column(c);
        }
        self
    }

    #[must_use]
    pub fn column(self, column: impl Into<Column>) -> Self {
        let column = column.into();
        self.column_alias(column.name.clone(), column)
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn column_alias(mut self, name: impl ToString, column: impl Into<Column>) -> Self {
        self.layout.insert(name.to_string(), column.into());
        self
    }
}

impl ViewSource for Table {
    fn table_name(&self) -> &str {
        &self.name
    }

    fn columns<'a>(
        &'a self,
        _: &'a TranspilerBuilder,
    ) -> Result<&'a HashMap<String, Column>, TranspilerBuilderError> {
        Ok(&self.layout)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    name: String,
    ty: SqlType,
}

impl Column {
    #[must_use]
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn new(name: impl ToString, ty: SqlType) -> Self {
        Self {
            name: name.to_string(),
            ty,
        }
    }
}

impl<T: ToString> From<(T, Self)> for Column {
    fn from((_, value): (T, Self)) -> Self {
        value
    }
}

impl<T: ToString> From<(T, SqlType)> for Column {
    fn from((name, ty): (T, SqlType)) -> Self {
        Self::new(name.to_string(), ty)
    }
}

impl SqlLayout {
    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn enter_schema(&mut self, name: impl ToString) -> &mut Self {
        self.schema = self.database.layout.get(&name.to_string()).cloned();
        self
    }

    #[allow(clippy::needless_pass_by_value, reason = "To enable passing &str")]
    pub fn enter_table(&mut self, name: impl ToString) -> &mut Self {
        self.table = self
            .schema
            .as_ref()
            .and_then(|x| x.0.get(&name.to_string()).cloned());
        self
    }

    pub fn enter_anonymous_schema(&mut self, layout: HashMap<String, Table>) -> &mut Self {
        self.schema = Some(Schema(layout, "__anonymous__".to_owned()));
        self
    }

    pub fn enter_anonymous_table(&mut self, layout: HashMap<String, Column>) -> &mut Self {
        self.table = Some(Table {
            layout,
            name: "__anonymous__".to_owned(),
        });
        self
    }

    pub fn column(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<(ColumnRef, Option<Type>)> {
        let mut it: Vec<_> = name.into_iter().collect();
        let column = it.pop().map(|x| x.to_string());
        let table = it.pop().map(|x| x.to_string());
        let schema = it.pop().map(|x| x.to_string());

        match (schema, table, column) {
            (None, None, Some(col)) => self.table.as_ref()?.layout.get(&col).map(|col| {
                (
                    ColumnRef::Column(alias(col.name.clone())),
                    Some(col.ty.into()),
                )
            }),
            (None, Some(table), Some(col)) => {
                self.schema.as_ref()?.0.get(&table).and_then(|table| {
                    table.layout.get(&col).map(|col| {
                        (
                            ColumnRef::TableColumn(
                                alias(table.name.clone()),
                                alias(col.name.clone()),
                            ),
                            Some(col.ty.into()),
                        )
                    })
                })
            }
            (Some(schema), Some(table), Some(col)) => {
                self.database.layout.get(&schema).and_then(|schema| {
                    schema.0.get(&table).and_then(|table| {
                        table.layout.get(&col).map(|col| {
                            (
                                ColumnRef::SchemaTableColumn(
                                    alias(schema.1.clone()),
                                    alias(table.name.clone()),
                                    alias(col.name.clone()),
                                ),
                                Some(col.ty.into()),
                            )
                        })
                    })
                })
            }
            _ => None,
        }
    }

    pub fn table(&self, name: impl IntoIterator<Item = impl ToString>) -> Option<TableRef> {
        let mut it: Vec<_> = name.into_iter().collect();
        let b = it.pop().map(|x| x.to_string());
        let a = it.pop().map(|x| x.to_string());

        match (a, b) {
            (None, Some(table)) => self
                .schema
                .as_ref()?
                .0
                .get(&table)
                .map(|table| TableRef::Table(alias(table.name.clone()))),
            (Some(schema), Some(table)) => self.database.layout.get(&schema).and_then(|schema| {
                schema.0.get(&table).map(|table| {
                    TableRef::SchemaTable(alias(schema.1.clone()), alias(table.name.clone()))
                })
            }),
            _ => None,
        }
    }

    pub fn table_asterisk(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<ColumnRef> {
        let mut it: Vec<_> = name.into_iter().collect();
        let b = it.pop().map(|x| x.to_string());
        let a = it.pop().map(|x| x.to_string());

        match (a, b) {
            (None, Some(table)) => self
                .schema
                .as_ref()?
                .0
                .get(&table)
                .map(|table| ColumnRef::TableAsterisk(alias(table.name.clone()))),
            _ => None,
        }
    }

    pub fn table_columns(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<&HashMap<String, Column>> {
        let mut it: Vec<_> = name.into_iter().collect();
        let b = it.pop().map(|x| x.to_string());
        let a = it.pop().map(|x| x.to_string());

        match (a, b) {
            (None, Some(table)) => Some(&self.schema.as_ref()?.0.get(&table)?.layout),
            _ => None,
        }
    }
}
