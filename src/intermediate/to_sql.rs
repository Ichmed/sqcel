use std::collections::HashMap;

use sea_query::{
    Asterisk, ColumnRef, DynIden, IntoIden, Query, SelectStatement, SimpleExpr, SubQueryStatement,
    TableRef,
};

use crate::{
    Result, Transpiler,
    structure::Column,
    types::{RecordSet, Type, TypeConversion, TypedExpression},
};

pub trait ToSql {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression>;

    fn cast(&self, tp: &Transpiler, ty: Type) -> Result<SimpleExpr> {
        Ok(ty.try_convert(tp, self.to_sql(tp)?)?.expr)
    }

    fn assume(&self, tp: &Transpiler, ty: Type) -> Result<TypedExpression> {
        Ok(TypedExpression {
            expr: self.to_sql(tp)?.expr,
            ty,
        })
    }

    fn returntype(&self, tp: &Transpiler) -> Type;

    fn to_record_set(&self, tp: &Transpiler) -> Result<SelectStatement> {
        self.to_record_set_with_alias(tp, tp.alias().into_iden())
    }

    fn to_record_set_with_alias(&self, tp: &Transpiler, alias: DynIden) -> Result<SelectStatement> {
        Ok(match self.cast(tp, RecordSet::default().into())? {
            SimpleExpr::SubQuery(_, x) => match *x {
                SubQueryStatement::SelectStatement(select_statement) => select_statement,
                _ => unimplemented!(
                    "SQCEL should never generate a statement that is not a select statement"
                ),
            },
            SimpleExpr::Column(x) => match x {
                ColumnRef::Column(col) => Query::select().column(col).take(),
                ColumnRef::TableColumn(tab, col) => Query::select().column(col).from(tab).take(),
                ColumnRef::SchemaTableColumn(sch, tab, col) => Query::select()
                    .column(col)
                    .from(TableRef::SchemaTable(sch, tab))
                    .take(),
                ColumnRef::Asterisk => Query::select().column(Asterisk).take(),
                ColumnRef::TableAsterisk(tab) => Query::select().column(Asterisk).from(tab).take(),
            },
            x => Query::select().expr_as(x, alias).take(),
        })
    }

    fn columns(&self, tp: &Transpiler) -> HashMap<String, Column> {
        match self.returntype(tp) {
            Type::RecordSet(r) => r.colummns.clone(),
            _ => Default::default(),
        }
    }

    fn has_column(&self, s: &str, tp: &Transpiler) -> bool {
        match self.returntype(tp) {
            Type::RecordSet(r) => r.colummns.contains_key(s),
            _ => false,
        }
    }
}

// impl ToSql for SimpleExpr {
//     fn to_sql(&self, _: &Transpiler) -> Result<TypedExpression> {
//         Ok(TypedExpression {
//             expr: self.clone(),
//             ty: Type::Unknown,
//         })
//     }

//     fn returntype(&self, _tp: &Transpiler) -> Type {
//         Type::Unknown
//     }
// }

macro_rules! wrappers {
    ($($(#[$attr:meta])* $name:ident),* $(,)?) => {$(
        #[doc = "An SQL Expression guaranteeing that "]
        $(#[$attr])*
        #[derive(Clone, Debug)]
        pub struct $name(SimpleExpr);

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = SimpleExpr;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$name> for SimpleExpr {
            fn from(value: $name) -> SimpleExpr {
                value.0
            }
        }

    )*

    };
}

wrappers!(
    /// the return type is `jsonb`
    JsonExpression,
    /// the return type is `numeric`
    NumericExpression,
);
