use super::{Function, subquery};
use crate::intermediate::{Expression, Rc, ToSql};
use crate::sql_extensions::SqlExtension;
use crate::structure::{Column, Table};
use crate::types::{JsonType, RecordSet, SqlType, Type, TypedExpression};
use crate::{Result, intermediate::Ident};
use crate::{Transpiler, transpiler::ParseError};
use sea_query::{ColumnRef, Func, Query, SimpleExpr};

pub struct Map {
    rec: Expression,
    var: Ident,
    filter: Option<Expression>,
    func: Expression,
}

impl Map {
    pub fn new(
        rec: &Expression,
        var: &Expression,
        filter: Option<&Expression>,
        func: &Expression,
    ) -> Result<Rc<Self>> {
        Ok(Rc::new(Self {
            rec: (rec.clone()),
            var: var.as_single_ident()?.clone(),
            filter: filter.cloned(),
            func: func.clone(),
        }))
    }
}

impl Function for Map {}

impl ToSql for Map {
    fn returntype(&self, _tp: &Transpiler) -> Type {
        RecordSet::default().into()
    }

    fn to_sql(&self, tp: &crate::Transpiler) -> Result<TypedExpression> {
        let varname = self.var.to_string();

        let rec = self.rec.to_sql(tp)?;

        let tp_inner = match &rec.ty {
            Type::RecordSet(rs) => tp.clone().enter_anonymous_schema(
                [(
                    varname.clone(),
                    Table::new(varname).columns(rs.colummns.clone()),
                )]
                .into(),
            ),
            Type::Sql(SqlType::JSON) | Type::Json(_) => tp.clone().enter_anonymous_table(
                [(varname.clone(), Column::new(varname, SqlType::JSON))].into(),
            ),
            ty => {
                return Err(ParseError::WrongFunctionArgType(
                    "map".to_owned(),
                    ty.clone(),
                ));
            }
        };

        let func = match self.func.as_single_ident() {
            Ok(func) if *func == self.var => {
                SimpleExpr::Column(ColumnRef::Asterisk).with_type(RecordSet::anon_list())
            }
            _ => self.func.to_sql(&tp_inner)?,
        };

        let filter = self
            .filter
            .as_ref()
            .map(|x| Result::Ok(x.to_sql(&tp_inner)?.expr))
            .transpose()?;

        let mut q = Query::select();
        q.expr(func.expr).and_where_option(filter);

        let assume_is = match rec.ty {
            Type::RecordSet(ref build) => subquery(
                q.from_subquery(rec.to_record_set(tp)?, self.var.clone())
                    .take(),
            )
            .assume_is(Type::RecordSet(build.clone())),

            Type::Json(JsonType::Map) => subquery(
                q.from_function(
                    Func::cust("jsonb_object_keys").arg(rec.expr),
                    self.var.clone(),
                )
                .take(),
            )
            .assume_is(RecordSet::anon_list()),
            Type::Sql(SqlType::JSON) | Type::Json(JsonType::Any | JsonType::List) => subquery(
                q.from_function(
                    Func::cust("jsonb_array_elements").arg(rec.expr),
                    self.var.clone(),
                )
                .take(),
            )
            .assume_is(RecordSet::anon_list()),
            ty => {
                return Err(ParseError::WrongFunctionArgType("map".to_owned(), ty));
            }
        };

        Ok(assume_is)
    }

    fn columns(&self, tp: &Transpiler) -> std::collections::HashMap<String, Column> {
        if let Ok(f) = self.func.as_single_ident()
            && *f == self.var
        {
            return self.rec.columns(tp);
        }

        self.func.columns(tp)
    }
}
