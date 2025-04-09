use crate::transpiler::Transpiler;
use clap::Parser;
use serde_json::Value;
use sqcel::{PostgresQueryBuilder, Query};
use std::io::BufRead;

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

        let variables = variables
            .into_iter()
            .map(|v| {
                v.split_once(":")
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .expect("key:value pairs must contain a colon")
            })
            .map(|(k, v)| (k, serde_json::from_str::<Value>(&v).unwrap().into()))
            .collect();

        let columns = columns
            .into_iter()
            .map(|v| {
                v.split_once(":")
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .unwrap_or((v.clone(), v))
            })
            .collect();

        let tables = tables
            .into_iter()
            .map(|v| {
                v.split_once(":")
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .unwrap_or((v.clone(), v))
            })
            .collect();

        let schemas = schemas
            .into_iter()
            .map(|v| {
                v.split_once(":")
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .unwrap_or((v.clone(), v))
            })
            .collect();

        Transpiler {
            variables,
            columns,
            tables,
            schemas,
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
