use std::collections::HashMap;

use crate::types::{SqlType, Type};
use sea_query::{ColumnRef, TableRef};

use crate::transpiler::alias;

#[derive(Debug, Clone, Default)]
pub struct SqlLayout {
    database: Database,
    schema: Option<Schema>,
    table: Option<Table>,
}

impl SqlLayout {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Database {
    layout: HashMap<String, Schema>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            layout: Default::default(),
        }
    }

    pub fn schema(self, schema: Schema) -> Self {
        self.schema_alias(schema.name.clone(), schema)
    }

    pub fn schema_alias(mut self, name: impl ToString, schema: Schema) -> Self {
        self.layout.insert(name.to_string(), schema);
        self
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    layout: HashMap<String, Table>,
    name: String,
}

impl Schema {
    pub fn new(name: impl ToString) -> Self {
        Self {
            layout: Default::default(),
            name: name.to_string(),
        }
    }
    pub fn table(self, table: Table) -> Self {
        self.table_alias(table.name.clone(), table)
    }

    pub fn table_alias(mut self, name: impl ToString, table: Table) -> Self {
        self.layout.insert(name.to_string(), table);
        self
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    layout: HashMap<String, Column>,
    name: String,
}
impl Table {
    pub fn new(name: impl ToString) -> Self {
        Self {
            layout: Default::default(),
            name: name.to_string(),
        }
    }
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<Column>>) -> Self {
        for c in columns {
            self = self.column(c);
        }
        self
    }

    pub fn column(self, column: impl Into<Column>) -> Self {
        let column = column.into();
        self.column_alias(column.name.clone(), column)
    }

    pub fn column_alias(mut self, name: impl ToString, column: impl Into<Column>) -> Self {
        self.layout.insert(name.to_string(), column.into());
        self
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    name: String,
    ty: SqlType,
}

impl Column {
    pub fn new(name: impl ToString, ty: SqlType) -> Self {
        Self {
            name: name.to_string(),
            ty,
        }
    }
}

impl<T: ToString> From<(T, SqlType)> for Column {
    fn from((name, ty): (T, SqlType)) -> Self {
        Column::new(name.to_string(), ty)
    }
}

impl SqlLayout {
    pub fn enter_schema(&mut self, name: impl ToString) -> &mut Self {
        self.schema = self.database.layout.get(&name.to_string()).cloned();
        self
    }

    pub fn enter_table(&mut self, name: impl ToString) -> &mut Self {
        self.table = self
            .schema
            .as_ref()
            .and_then(|x| x.layout.get(&name.to_string()).cloned());
        self
    }

    pub fn enter_anonymous_schema(&mut self, layout: HashMap<String, Table>) -> &mut SqlLayout {
        self.schema = Some(Schema {
            layout,
            name: "__anonymous__".to_owned(),
        });
        self
    }

    pub fn enter_anonymous_table(&mut self, layout: HashMap<String, Column>) -> &mut SqlLayout {
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
                    Some(col.ty.clone().into()),
                )
            }),
            (None, Some(table), Some(col)) => {
                self.schema.as_ref()?.layout.get(&table).and_then(|table| {
                    table.layout.get(&col).map(|col| {
                        (
                            ColumnRef::TableColumn(
                                alias(table.name.clone()),
                                alias(col.name.clone()),
                            ),
                            Some(col.ty.clone().into()),
                        )
                    })
                })
            }
            (Some(schema), Some(table), Some(col)) => {
                self.database.layout.get(&schema).and_then(|schema| {
                    schema.layout.get(&table).and_then(|table| {
                        table.layout.get(&col).map(|col| {
                            (
                                ColumnRef::SchemaTableColumn(
                                    alias(schema.name.clone()),
                                    alias(table.name.clone()),
                                    alias(col.name.clone()),
                                ),
                                Some(col.ty.clone().into()),
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
                .layout
                .get(&table)
                .map(|table| TableRef::Table(alias(table.name.clone()))),
            (Some(schema), Some(table)) => {
                self.database
                    .layout
                    .get(&schema.to_string())
                    .and_then(|schema| {
                        schema.layout.get(&table.to_string()).map(|table| {
                            TableRef::SchemaTable(
                                alias(schema.name.clone()),
                                alias(table.name.clone()),
                            )
                        })
                    })
            }
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
                .layout
                .get(&table)
                .map(|table| ColumnRef::TableAsterisk(alias(table.name.clone()))),
            _ => None,
        }
    }

    pub fn table_columns(
        &self,
        name: impl IntoIterator<Item = impl ToString>,
    ) -> Option<HashMap<String, Column>> {
        let mut it: Vec<_> = name.into_iter().collect();
        let b = it.pop().map(|x| x.to_string());
        let a = it.pop().map(|x| x.to_string());

        match (a, b) {
            (None, Some(table)) => Some(self.schema.as_ref()?.layout.get(&table)?.layout.clone()),
            _ => None,
        }
    }
}
