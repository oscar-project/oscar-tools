//! Splitting of OSCAR Schema v2 corpora
//!
//! Untested but should work on OSCAR Schema v1 corpora
use std::path::PathBuf;

use crate::impls::OscarDoc;
use crate::ops::ExtractText as ET;
use crate::{cli::Runnable, error::Error};
use log::error;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct ExtractText {
    #[structopt(help = "source corpus file. Does not work with folders")]
    src: PathBuf,
    #[structopt(help = "dest corpus folder.")]
    dst: PathBuf,

    #[structopt(help = "delete source files", short = "m")]
    del_src: bool,
}

impl Runnable for ExtractText {
    fn run(&self) -> Result<(), Error> {
        if self.src.is_file() {
            OscarDoc::extract_text(&self.src, &self.dst, self.del_src)?;
            Ok(())
        } else {
            error!("Extraction is not supported on folders. Call on each file.");
            Err(Error::Custom(
                "Extraction is not supported on folders. Call on each file.".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile;

    use crate::{impls::OscarDoc, ops::ExtractText};

    pub fn setup_oscardoc() -> (String, String) {
        let mut corpus = String::new();
        let mut content_only = String::new();
        for i in 0..100 {
            let content = format!(r#"document n{0}\nthis is document n{0}"#, i);
            corpus.push_str(&format!(
                r#"{{"content":"{content}", "metadata": ["foo"]}}"#,
            ));
            corpus.push('\n');

            content_only.push_str(&content.replace(r#"\n"#, "\n"));
            content_only.push_str("\n\n");
        }

        (corpus, content_only)
    }

    #[test]
    fn test_extract() {
        //get both documents and expected output
        let (docs, content_only) = setup_oscardoc();
        let mut src = tempfile::NamedTempFile::new().unwrap();

        //write fake corpus
        src.write_all(docs.as_bytes()).unwrap();

        // create destination path and file path
        let dst = tempfile::tempdir().unwrap();
        let dst_path = dst.into_path().join("text_only.txt");

        let src_path = src.into_temp_path();
        OscarDoc::extract_text(&src_path, &dst_path, false).unwrap();

        // read extracted
        let text = std::fs::read_to_string(dst_path).unwrap();

        assert!(src_path.exists());
        assert_eq!(text, content_only);
    }

    #[test]
    fn test_extract_rm_src() {
        //get both documents and expected output
        let (docs, content_only) = setup_oscardoc();
        let mut src = tempfile::NamedTempFile::new().unwrap();

        //write fake corpus
        src.write_all(docs.as_bytes()).unwrap();

        // create destination path and file path
        let dst = tempfile::tempdir().unwrap();
        let dst_path = dst.into_path().join("text_only.txt");

        let src_path = src.into_temp_path();
        OscarDoc::extract_text(&src_path, &dst_path, true).unwrap();

        // read extracted
        let text = std::fs::read_to_string(dst_path).unwrap();

        assert!(!src_path.exists());
        assert_eq!(text, content_only);
    }
}
