#![doc = include_str!("../README.md")]
#[macro_use]
extern crate log;

mod cli;
mod error;
mod extract_clean;
mod extract_text;
mod impls;
mod lang_codes;
mod ops;
mod versions;

use crate::error::Error;
use clap::AppSettings;
use clap::ArgMatches;
use env_logger::Env;

use crate::cli::Command;
use crate::impls::OscarDoc;

fn build_app() -> clap::App<'static> {
    clap::App::new("oscar-tools")
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(OscarDoc::subcommand())
}

fn run(matches: ArgMatches) -> Result<(), Error> {
    let (version, subcommand) = matches
        .subcommand()
        .ok_or(Error::Custom("No version provided!".to_string()))?;
    match version {
        "v2.0.0" => OscarDoc::run(subcommand),
        x => Err(Error::Custom(format!("Unknown version {x}"))),
    }
}
fn main() -> Result<(), error::Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let app = build_app();
    let matches = app.get_matches();
    run(matches)?;
    Ok(())
}
