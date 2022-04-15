//! Commands traits and base CLI parsing

use crate::error::Error;
use crate::impls::OscarDoc;
use clap::ArgMatches;

pub trait Command {
    fn hook_to_clap(ctx: clap::App<'static>) -> clap::App<'static>
    where
        Self: Sized,
    {
        ctx.subcommand(Self::subcommand())
    }
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized;

    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized;
}

#[cfg(not(tarpaulin_include))]
pub(crate) fn build_app() -> clap::App<'static> {
    use clap::AppSettings;

    use crate::impls::OscarTxt;

    clap::App::new("oscar-tools")
        .global_setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(OscarDoc::subcommand())
        .subcommand(OscarTxt::subcommand())
}

#[cfg(not(tarpaulin_include))]
pub(crate) fn run(matches: ArgMatches) -> Result<(), Error> {
    use crate::impls::OscarTxt;

    let (version, subcommand) = matches
        .subcommand()
        .ok_or_else(|| Error::Custom("No version provided!".to_string()))?;
    match version {
        //TODO: this should be automatically done by calling a version resolver
        //      Some struct/enum that holds OSCAR versions, and implements a from string that
        //      buils something that implements run and runs the correct OSCAR version
        "v2" => OscarDoc::run(subcommand),
        "v1" => OscarTxt::run(subcommand),
        x => Err(Error::Custom(format!("Unknown version {x}"))),
    }
}

/// Runnable traits have to be implemented by commands
/// in order to be executed from CLI.
// TODO: Currently, run returns (), so if the command
// actually returns something usable, it cannot pass it on.
// shall we provide flexibility to the Runnable trait by using generics
// or provide another trait like Queryable to fetch results?
pub trait Runnable {
    fn run(&self) -> Result<(), Error>;
}
