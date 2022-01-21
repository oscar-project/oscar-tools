/// Extracts a clean corpus (= documents with no annotation)
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

use structopt::StructOpt;

use crate::cli::Runnable;

#[derive(StructOpt, Debug)]
pub struct ExtractCleanCorpus {
    #[structopt(help = "Path to corpus")]
    path: PathBuf,
    #[structopt(help = "Dest of clean corpus")]
    dst: PathBuf,
}

/// Checks whether a document has no annotations
/// returns [True] if the document has no annotations
fn is_clean(document: &serde_json::Value) -> bool {
    document["metadata"]["annotation"] == serde_json::Value::Null
}

impl Runnable for ExtractCleanCorpus {
    fn run(&self) -> Result<(), crate::error::Error> {
        // open src corpus
        let corpus = File::open(&self.path)?;
        let corpus_buf = BufReader::new(corpus);
        let documents = corpus_buf.lines();

        // open dst corpus
        let clean_corpus = File::create(&self.dst)?;
        let mut clean_buf = BufWriter::new(clean_corpus);

        for (count, document) in documents.enumerate() {
            if count % 1_000_000 == 0 {
                info!("Done {count} documents");
            }
            let mut document_str = document?;
            let document: serde_json::Value = serde_json::from_str(&document_str)?;
            if is_clean(&document) {
                // add line return to be jsonl valid
                document_str.push('\n');
                clean_buf.write_all(document_str.as_bytes())?;
            }
        }

        Ok(())
    }
}
