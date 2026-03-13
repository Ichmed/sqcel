use indexmap::IndexMap;

use crate::{
    structure::{Column, Table},
    transpiler::{TranspilerBuilder, TranspilerBuilderError},
};

pub trait ViewSource {
    fn table_name(&self) -> &str;
    fn columns<'a>(
        &'a self,
        tp: &'a TranspilerBuilder,
    ) -> Result<&'a IndexMap<String, Column>, TranspilerBuilderError>;
    fn to_table(&self, tp: &TranspilerBuilder) -> Result<Table, TranspilerBuilderError> {
        Ok(Table::new(self.table_name()).columns(self.columns(tp)?.clone()))
    }
}

impl ViewSource for &'static str {
    fn table_name(&self) -> &str {
        self
    }

    fn columns<'a>(
        &'a self,
        tp: &'a TranspilerBuilder,
    ) -> Result<&'a IndexMap<String, Column>, TranspilerBuilderError> {
        tp.layout
            .as_ref()
            .ok_or(TranspilerBuilderError::UninitializedField("layout"))?
            .table_columns([self])
            .ok_or_else(|| {
                TranspilerBuilderError::ValidationError(format!(
                    "Table {self} not (yet) present in layout"
                ))
            })
    }
}
