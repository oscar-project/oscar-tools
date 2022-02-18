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

use env_logger::Env;

#[cfg(not(tarpaulin_include))]
fn main() -> Result<(), error::Error> {
    use cli::build_app;

    use crate::cli::run;

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let app = build_app();
    let matches = app.get_matches();
    run(matches)?;
    Ok(())
}
