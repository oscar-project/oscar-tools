#![doc = include_str!("../README.md")]
#[macro_use]
extern crate log;

mod cli;
mod compress;
mod error;
mod extract_clean;
mod extract_text;
mod impls;
mod lang_codes;
mod ops;
mod split_latest;
mod versions;

use cli::OscarTools;
use cli::Runnable;
use env_logger::Env;
use structopt::StructOpt;

fn main() -> Result<(), error::Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // get options from args
    let opt = OscarTools::from_args();

    // run command
    opt.run()?;

    Ok(())
}
