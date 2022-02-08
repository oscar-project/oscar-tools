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

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{Read, Write},
    };

    use tempfile;

    use crate::{impls::OscarDoc, ops::Compress};

    pub fn setup_oscardoc() -> String {
        let mut corpus = String::new();
        for i in 0..10000 {
            corpus.push_str(&format!(r#"{{"item":{}}}"#, i));
            corpus.push('\n');
        }

        corpus
    }

    // the way of checking results is bad, since we merge then sort results
    // we should rather check the individual files one by one
    #[test]
    fn test_compress() {
        let content = setup_oscardoc();
        let content: Vec<&str> = content.lines().collect();
        let content_files = (&content).chunks(1000).into_iter();
        let tmpdir = tempfile::tempdir().unwrap();
        for (idx, chunk) in content_files.enumerate() {
            // should be safe since it does not rely on rust destructor
            // + it is in a tempfile that will be cleaned at the exit of the test
            let tempfile_path = tmpdir.path().join(format!("file_{idx}.jsonl"));
            let mut tempfile = File::create(tempfile_path).unwrap();
            tempfile.write_all(chunk.join("\n").as_bytes()).unwrap();
        }

        // create destination path and compress
        let tmpdst = tempfile::tempdir().unwrap();
        OscarDoc::compress_folder(tmpdir.path(), tmpdst.path(), false, 1).unwrap();

        println!(
            "{:?}",
            std::fs::read_dir(tmpdir.path())
                .unwrap()
                .collect::<Vec<_>>()
        );
        // let mut items_decompressed = Vec::new();

        let mut decompressed_data = Vec::new();
        for file in std::fs::read_dir(tmpdst.path()).unwrap() {
            println!("file: {:?}", file);
            // for file in split_files {
            let file = file.unwrap();
            let file = File::open(file.path()).unwrap();
            let mut reader = flate2::read::GzDecoder::new(file);
            let mut decompressed = String::new();
            reader.read_to_string(&mut decompressed).unwrap();
            decompressed_data.extend(decompressed.lines().map(|x| x.to_string()).into_iter());
        }

        // sort results
        decompressed_data.sort();
        let mut content = content;
        content.sort();
        assert_eq!(decompressed_data, content);
    }
}
