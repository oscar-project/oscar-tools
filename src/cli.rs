//! Commands enum

use crate::error::Error;
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

/// Runnable traits have to be implemented by commands
/// in order to be executed from CLI.
// TODO: Currently, run returns (), so if the command
// actually returns something usable, it cannot pass it on.
// shall we provide flexibility to the Runnable trait by using generics
// or provide another trait like Queryable to fetch results?
pub trait Runnable {
    fn run(&self) -> Result<(), Error>;
}
