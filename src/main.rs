use crate::transpiler::Transpiler;
use clap::Parser;
use sqcel::{PostgresQueryBuilder, Query};
use std::{collections::HashMap, io::BufRead};

pub mod transpiler;

#[derive(clap::Parser)]
struct Cli {
    /// Identifiers included in vars will become
    /// constants during conversion (instead of bind params)
    #[clap(short)]
    variables: Vec<String>,
    /// These identifiers are treated as column names
    #[clap(short)]
    columns: Vec<String>,
    /// These identifiers are treated as table names
    ///
    /// Members of theses identifiers will be interpreted as column names
    #[clap(short)]
    tables: Vec<String>,
    /// These identifiers are treated as schema names
    ///
    /// Members of theses identifiers will be interpreted as table names
    ///
    /// Members of those identifiers will be interpreted as column names
    #[clap(long)]
    schemas: Vec<String>,
}

impl Cli {
    fn into_transpiler(self) -> Transpiler {
        let Self {
            variables,
            columns,
            tables,
            schemas,
        } = self;

        let mut variables = variables.into_iter();
        let mut variables_map: HashMap<String, sea_query::Value> = Default::default();

        while let Some(entry) = variables.next() {
            if let Some((key, value)) = entry
                .split_once(':')
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
                .or_else(|| variables.next().map(|val| (entry, val)))
            {
                variables_map.insert(
                    key,
                    dbg!(serde_json::from_str::<serde_json::Value>(dbg!(&value)))
                        .unwrap()
                        .into(),
                );
            }
        }

        Transpiler {
            variables: variables_map,
            columns: columns.into_iter().collect(),
            tables: tables.into_iter().collect(),
            schemas: schemas.into_iter().collect(),
        }
    }
}

fn main() {
    let transpiler = Cli::parse().into_transpiler();
    for line in std::io::stdin().lock().lines() {
        match transpiler.transpile(&line.unwrap()) {
            Ok(sql) => {
                let q = Query::select().expr(sql).build(PostgresQueryBuilder);
                println!("{}", q.0);
                if q.1.0.is_empty() {
                    println!();
                    for v in q.1 {
                        println!("{}", v)
                    }
                }
                println!();
            }
            Err(err) => eprintln!("{err}"),
        }
    }
}
