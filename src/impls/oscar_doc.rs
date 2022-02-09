//! OSCAR Schema v2 (See [oscar-corpus.com](https://oscar-corpus.com)) operation implementations.
//!
//! Implementations mostly use default trait implementations, as the format is simple.
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
};

use serde_json::Value;

use crate::{
    error::Error,
    ops::{Compress, ExtractText, Split},
    versions::{Schema, Version},
};

/// OSCAR Schema v2.
///
/// Document-oriented, one document per line, formatted in JSONLines.
pub struct OscarDoc;

impl Schema for OscarDoc {
    fn version() -> Version {
        Version::new(2, 0, 0)
    }
}

/// Use default implementation of splitting (see [crate::ops::Split])
impl Split for OscarDoc {}
impl Compress for OscarDoc {}

/// impl block for helper functions related to [ExtractText].
impl OscarDoc {
    /// Extracts content from a Document.
    ///
    /// Fails if the `content` field is missing or is not a string.
    fn extract_from_doc(doc: &str) -> Result<String, Error> {
        let v: Value = serde_json::from_str(doc)?;

        if let Some(content) = v.get("content") {
            if let Value::String(c) = content {
                let mut content_str = c.to_string().replace(r#"\n"#, "\n");
                content_str.push('\n');
                Ok(content_str)
            } else {
                Err(Error::MalformedContent(v))
            }
        } else {
            Err(Error::MissingContent(v))
        }
    }

    fn extract<T: Read, U: Write>(src: T, dst: &mut U) -> Result<(), Error> {
        let b = BufReader::new(src);
        let docs = b.lines();
        for doc in docs {
            //extract and add newline
            let doc = doc?;
            let content = Self::extract_from_doc(&doc)? + "\n";
            let content_length = content.len();

            // check written bytes
            if dst.write(content.as_bytes())? > content_length {
                error!("IO Error: Could not write into destination writer.");
            }
        }

        // flush output
        dst.flush()?;

        Ok(())
    }
}

impl ExtractText for OscarDoc {
    fn extract_text(
        src: &std::path::Path,
        dst: &std::path::Path,
        del_src: bool,
    ) -> Result<(), Error> {
        if !src.is_file() {
            warn!("{:?} is not a file: ignoring", src);
            return Ok(());
        }
        let src_file = File::open(src)?;

        // gen filename
        let filename = src.file_name().unwrap();
        let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
        let extension = String::from(dst.extension().unwrap().to_str().unwrap());
        dst.set_extension(extension + ".txt");

        info!("extracting text from {:?} to {:?}", src, dst);

        let mut dest_file = File::create(dst)?;
        OscarDoc::extract(src_file, &mut dest_file)?;

        if del_src {
            std::fs::remove_file(src)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::impls::OscarDoc;

    fn get_doc() -> &'static str {
        r#"{"content":"foo\nbar\nbaz\nquux"}
{"content":"123456789"}
{"content":"246810"}
{"content":"test"}"#
    }

    #[test]
    fn test_extract_single() {
        let docs = get_doc();
        let doc = docs.lines().next().unwrap().as_bytes();

        let mut buf = Vec::new();
        OscarDoc::extract(doc, &mut buf).unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), "foo\nbar\nbaz\nquux\n\n");
    }
    #[test]
    fn test_extract_multiple() {
        let doc = get_doc().as_bytes();
        let mut buf = Vec::new();
        OscarDoc::extract(doc, &mut buf).unwrap();

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "foo\nbar\nbaz\nquux\n\n123456789\n\n246810\n\ntest\n\n"
        );
    }
    #[test]
    fn extract_no_content() {
        let document = r#"{"no_content": "hehe"}"#;
        let extracted = OscarDoc::extract_from_doc(document);

        assert!(extracted.is_err())
    }

    #[test]
    fn extract_bad_content() {
        let document = r#"{"content": ["hehe"]}"#;
        let extracted = OscarDoc::extract_from_doc(document);

        assert!(extracted.is_err())
    }

    #[test]
    fn text_extract_from_doc() {
        let content = "foo
bar
baz
quux
";

        let document = r#"
        {
            "content":"foo\nbar\nbaz\nquux",
            "warc_headers":{
              "warc-block-digest":"sha1:X3OWP47FG2O5LBNMFSNB44FJF2SSRC26",
              "content-type":"text/plain",
              "warc-refers-to":"<urn:uuid:83f2e1d4-5ed3-41db-86ff-f7826c4c20f9>",
              "content-length":"16",
              "warc-identified-content-language":"eng",
              "warc-target-uri":"http://3dv2015.inria.fr/registration-2/index.html",
              "warc-date":"2021-09-16T11:07:14Z",
              "warc-record-id":"<urn:uuid:3304bc27-17d0-4ffd-a692-340381478a5f>",
              "warc-type":"conversion"
            },
            "metadata":{
              "identification":{
                "label":"en",
                "prob":0.6268374
              },
              "annotation":[
                "short_sentences",
                "footer"
              ],
              "sentence_identifications":[
                {
                  "label":"en",
                  "prob":0.93925816
                },
                null,
                {
                  "label":"en",
                  "prob":0.9937219
                },
                {
                  "label":"en",
                  "prob":0.9996538
                }
              ]
            }
          }
        "#;

        let extracted = OscarDoc::extract_from_doc(document).unwrap();
        assert_eq!(extracted, content);
    }
}
