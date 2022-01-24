use std::path::PathBuf;

use crate::{impls::OscarDoc, ops::Split};
use structopt::StructOpt;

use crate::{cli::Runnable, error::Error};

#[derive(StructOpt, Debug)]
pub struct SplitLatest {
    #[structopt(help = "source corpus folder. If file, operates the splitting on the file only.")]
    src: PathBuf,
    #[structopt(help = "dest corpus folder.")]
    dst: PathBuf,
    #[structopt(short, long, default_value = "10000000", help = "dest corpus folder.")]
    size: usize,
}

impl Runnable for SplitLatest {
    fn run(&self) -> Result<(), Error> {
        OscarDoc::split(&self.src, &self.dst, self.size)?;
        Ok(())
    }
}
