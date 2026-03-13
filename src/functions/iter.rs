use sea_query::{IntoTableRef, TableRef};

use crate::{
    Transpiler,
    structure::{Column, Schema, Table},
};

#[derive(Clone, Debug)]
pub struct Iterable {
    pub expr: TableRef,
    pub kind: IterKind,
}

#[derive(Clone, Debug)]
pub enum IterKind {
    Column(Column),
    Table(Table),
    Schema(Schema),
}

impl IntoTableRef for Iterable {
    fn into_table_ref(self) -> TableRef {
        self.expr
    }
}

impl Transpiler {
    #[must_use]
    #[allow(clippy::missing_panics_doc, reason = "Will not panic")]
    pub fn iterate(&self, iter: &Iterable) -> Self {
        let mut b = self.to_builder();
        match &iter.kind {
            IterKind::Column(col) => b.add_temp_column_to_table(col.clone()),
            IterKind::Table(table) => b.add_temp_table_to_schema(table.clone()),
            IterKind::Schema(schema) => b.add_temp_schema_to_database(schema.clone()),
        }
        .build()
        .unwrap()
    }
}
