#[macro_use]
extern crate log;

mod cli;
mod error;
mod lang_codes;

use cli::OscarTools;
use cli::Runnable;
use structopt::StructOpt;

fn main() -> Result<(), error::Error> {
    env_logger::init();

    // get options from args
    let opt = OscarTools::from_args();

    // run command
    opt.run()?;

    Ok(())
}
