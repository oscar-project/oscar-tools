use crate::error::Error;
use crate::lang_codes::UpdateLangCodes;
use structopt::StructOpt;

/// Runnable traits have to be implemented by commands
/// in order to be executed from CLI.
// TODO: Currently, run returns (), so if the command
// actually returns something usable, it cannot pass it on.
// shall we provide flexibility to the Runnable trait by using generics
// or provide another trait like Queryable to fetch results?
pub trait Runnable {
    fn run(&self) -> Result<(), Error>;
}

#[derive(Debug, StructOpt)]
#[structopt(name = "oscar-tools", about = "A collection of tools for OSCAR corpus")]
/// Holds every command that is callable by the `oscar-tools` command.
pub enum OscarTools {
    #[structopt(about = "update language codes to BCP-47 and fix mistakes from OSCAR v1.")]
    UpdateLangCodes(UpdateLangCodes),
}

impl Runnable for OscarTools {
    fn run(&self) -> Result<(), Error> {
        match self {
            OscarTools::UpdateLangCodes(u) => u.run(),
        }
    }
}
