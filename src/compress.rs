//! Splitting of OSCAR Schema v2 corpora
//!
//! Untested but should work on OSCAR Schema v1 corpora
use std::path::PathBuf;

use crate::{impls::OscarDoc, ops::Compress};
use structopt::StructOpt;

use crate::{cli::Runnable, error::Error};

#[derive(StructOpt, Debug)]
pub struct CompressCorpus {
    #[structopt(help = "source corpus folder. If file, operates the splitting on the file only.")]
    src: PathBuf,
    #[structopt(help = "dest corpus folder.")]
    dst: PathBuf,

    #[structopt(help = "delete source files", short = "m")]
    del_src: bool,

    #[structopt(
        short,
        long,
        default_value = "0",
        help = "Number of threads (ignored if source is a single file)"
    )]
    num_threads: usize,
}

impl Runnable for CompressCorpus {
    fn run(&self) -> Result<(), Error> {
        if self.src.is_file() {
            OscarDoc::compress_file(&self.src, &self.dst, self.del_src)?;
        }
        if self.src.is_dir() {
            OscarDoc::compress_folder(&self.src, &self.dst, self.del_src, self.num_threads)?;
        }
        Ok(())
    }
}
