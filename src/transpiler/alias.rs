use sea_query::{ColumnRef, Iden, Quote, SeaRc, SimpleExpr};

use crate::intermediate::Rc;

#[derive(Clone, Copy)]
pub struct IncAlias(pub(crate) usize);

impl Iden for IncAlias {
    fn unquoted(&self, s: &mut dyn std::fmt::Write) {
        s.write_str("c_").ok();
        write!(s, "{}", self.0).ok();
    }
}

#[derive(Clone)]
pub struct TableAlias<const N: usize>(pub(crate) Rc<(usize, [IncAlias; N])>);

impl<const N: usize> Iden for TableAlias<N> {
    fn unquoted(&self, s: &mut dyn std::fmt::Write) {
        write!(s, "t_{}(", self.0.0).ok();
        if !self.0.1.is_empty() {
            for i in 0..self.0.1.len() - 1 {
                self.0.1[i].unquoted(s);
                s.write_char(',').ok();
            }
            self.0.1[self.0.1.len() - 1].unquoted(s);
        }
        s.write_char(')').ok();
    }

    fn prepare(&self, s: &mut dyn std::fmt::Write, q: Quote) {
        write!(s, "{}t_{}{}(", q.left(), self.0.0, q.right()).ok();
        if !self.0.1.is_empty() {
            for i in 0..self.0.1.len() - 1 {
                write!(s, "{}", q.left()).ok();
                self.0.1[i].unquoted(s);
                write!(s, "{}", q.right()).ok();
                s.write_char(',').ok();
            }
            write!(s, "{}", q.left()).ok();
            self.0.1[self.0.1.len() - 1].unquoted(s);
            write!(s, "{}", q.right()).ok();
        }
        s.write_char(')').ok();
    }
}

pub type KeyValueAlias = TableAlias<2>;

impl KeyValueAlias {
    #[must_use]
    pub fn key(&self) -> SimpleExpr {
        SimpleExpr::Column(self.key_ref())
    }
    #[must_use]
    pub fn key_ref(&self) -> ColumnRef {
        ColumnRef::Column(SeaRc::new(self.0.1[0]))
    }

    #[must_use]
    pub fn value(&self) -> SimpleExpr {
        SimpleExpr::Column(self.value_ref())
    }

    #[must_use]
    pub fn value_ref(&self) -> ColumnRef {
        ColumnRef::Column(SeaRc::new(self.0.1[1]))
    }
}
