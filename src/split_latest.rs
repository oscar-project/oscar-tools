//! Splitting of OSCAR Schema v2 corpora
//!
//! Untested but should work on OSCAR Schema v1 corpora
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
    #[structopt(
        short,
        long,
        default_value = "1000000000",
        help = "dest corpus folder."
    )]
    size: usize,
    #[structopt(
        short,
        long,
        default_value = "0",
        help = "Number of threads (ignored if source is a single file)"
    )]
    num_threads: usize,
}

impl Runnable for SplitLatest {
    fn run(&self) -> Result<(), Error> {
        if self.src.is_file() {
            OscarDoc::split_file(&self.src, &self.dst, self.size)?;
        }
        if self.src.is_dir() {
            OscarDoc::split_all(&self.src, &self.dst, self.size, self.num_threads)?;
        }
        Ok(())
    }
}
