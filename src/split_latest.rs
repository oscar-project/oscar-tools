//! Splitting of OSCAR Schema v2 corpora
//!
//! Untested but should work on OSCAR Schema v1 corpora
use std::path::PathBuf;

use crate::{impls::OscarDoc, ops::Split};
use clap::StructOpt;

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

#[cfg(test)]
mod tests {

    use std::{fs::File, io::Write};
    use tempfile::tempdir;

    use crate::{impls::OscarDoc, ops::Split};

    pub fn setup_oscardoc() -> String {
        let mut corpus = String::new();
        for i in 0..10000 {
            corpus.push_str(&format!(r#"{{"item":{}}}"#, i));
            corpus.push('\n');
        }

        corpus
    }

    #[test]
    fn test_split_file() {
        let corpus = setup_oscardoc();

        // write corpus to file
        let test_dir = tempdir().unwrap();
        let corpus_orig = test_dir.path().join("corpus-orig.jsonl");
        let mut f = File::create(&corpus_orig).unwrap();
        f.write_all(&corpus.as_bytes()).unwrap();

        // split
        let split_folder = test_dir.path().join("split");
        std::fs::create_dir(&split_folder).unwrap();

        let corpus_dst = split_folder.join("corpus-split.jsonl");
        OscarDoc::split_file(&corpus_orig, &corpus_dst, 1000).unwrap();

        let mut corpus_from_split = String::with_capacity(corpus.len());

        for file in std::fs::read_dir(split_folder).unwrap() {
            // for file in split_files {
            let file = file.unwrap();
            let split = std::fs::read_to_string(file.path()).unwrap();
            corpus_from_split.push_str(&split);
        }

        let mut from_split_corpus: Vec<&str> = corpus.lines().collect();
        from_split_corpus.sort();
        let mut from_split_list: Vec<&str> = corpus_from_split.lines().collect();
        from_split_list.sort();

        assert_eq!(from_split_corpus, from_split_list);
    }
}
