pub mod functions;
pub mod hacks;
pub mod intermediate;
pub mod magic;
pub mod transpiler;
pub mod variables;

use crate::transpiler::Transpiler;
use clap::Parser;
use sea_query::{Asterisk, ColumnRef, SimpleExpr as SqlExpression, Value};
use sqcel::{PostgresQueryBuilder, Query};
use std::{io::BufRead, path::PathBuf};
use transpiler::alias;
use variables::Variable;

pub use transpiler::ParseError as Error;
pub use transpiler::Result;

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
    #[clap(long)]
    tables: Vec<String>,
    /// These identifiers are treated as schema names
    ///
    /// Members of theses identifiers will be interpreted as table names
    ///
    /// Members of those identifiers will be interpreted as column names
    #[clap(long)]
    schemas: Vec<String>,

    #[clap(short, long)]
    types: Vec<PathBuf>,

    #[clap(long)]
    accept_unknown_types: bool,

    #[clap(long)]
    trigger_mode: bool,

    #[clap(long)]
    no_reduce: bool,
}

impl Cli {
    fn into_transpiler(self) -> Transpiler {
        let Self {
            variables,
            columns,
            tables,
            schemas,
            types,
            accept_unknown_types,
            trigger_mode,
            no_reduce,
        } = self;

        let variables = variables
            .into_iter()
            .map(|v| {
                v.split_once(":")
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .expect("key:value pairs must contain a colon")
            })
            .map(|(k, v)| {
                (
                    k,
                    Variable::from(serde_json::from_str::<serde_json::Value>(&v).unwrap()),
                )
            });

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

        let types = if !types.is_empty() {
            let mut parser = protobuf_parse::Parser::new();
            parser.include(".");
            for path in types {
                parser.input(path);
            }
            parser
                .parse_and_typecheck()
                .unwrap()
                .file_descriptors
                .remove(0)
                .message_type
                .into_iter()
                .map(|m| (m.name().to_owned(), m))
                .collect()
        } else {
            Default::default()
        };

        Transpiler {
            columns,
            tables,
            schemas,
            types,
            accept_unknown_types,
            trigger_mode,
            reduce: !no_reduce,
            ..Default::default()
        }
        .to_builder()
        .vars(variables)
        .record("NEW")
        .record("OLD")
        .build()
    }
}

fn main() {
    let transpiler = Cli::parse().into_transpiler();
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        match hacks::get_plaintext_expression(&line, &transpiler, PostgresQueryBuilder) {
            Ok((sql, params)) => {
                println!("{sql}");
                if !params.is_empty() {
                    println!();
                    for v in params {
                        println!("{}", v)
                    }
                }
                println!();
            }
            Err(err) => eprintln!("{err}"),
        }
    }
}
