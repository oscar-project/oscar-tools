/*! struct for implementors !*/
use clap::ArgMatches;

use crate::{
    cli::Command,
    error::Error,
    impls::v1::SampleDoc,
    versions::{Schema, Version},
};

use super::DedupTxt;

pub struct OscarTxt;

impl Schema for OscarTxt {
    fn version() -> Version {
        Version::new(1, 0, 0)
    }
}

impl Command for OscarTxt {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        // add commands here
        let subcommand = clap::App::new(Self::version().to_string())
            .subcommand(DedupTxt::subcommand())
            .subcommand(SampleDoc::subcommand());

        subcommand
    }

    fn run(matches: &ArgMatches) -> Result<(), Error> {
        let (subcommand, matches) = matches.subcommand().unwrap();
        debug!("subcommand is {subcommand}");
        match subcommand {
            "dedup" => DedupTxt::run(matches),
            "sample" => SampleDoc::run(matches),
            x => Err(Error::Custom(format!(
                "{x} op is not supported on this corpus version"
            ))),
        }
    }
}
