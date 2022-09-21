/*! Extracts textual content into new files, discarding metadata. Should produce an OSCAR v1 (2019) compatible corpus.
!*/

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use oscar_io::oscar_doc::{Document, SplitFolderReader};

use crate::error::Error;
pub trait ExtractText {
    fn extract_from_path(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error> {
        let mut reader = SplitFolderReader::new(src)?;
        // let file = File::open(src)?;
        // let bufread = BufReader::new(file);
        // if dst.exists() {
        //     error!("File exist!");
        //     return Err(std::io::Error::new(
        //         std::io::ErrorKind::AlreadyExists,
        //         format!("File exist {:?}", dst),
        //     )
        //     .into());
        // }
        let dst_file = File::create(dst)?;
        let mut dst_buf = BufWriter::new(dst_file);
        Self::extract_text(&mut reader, &mut dst_buf)?;
        if del_src {
            std::fs::remove_file(src)?;
        }
        Ok(())
    }
    fn extract_text<T, U>(src: &mut T, dst: &mut U) -> Result<(), Error>
    where
        T: Iterator<Item = Result<Document, oscar_io::error::Error>>,
        U: std::io::Write,
    {
        for doc in src {
            let doc = doc?;
            let mut extracted = Self::extract_content(&doc).to_string();
            extracted.push_str("\n\n");
            let string_size = extracted.len();
            let written_byte = dst.write(extracted.as_bytes())?;
            if string_size != written_byte {
                return Err(Error::Custom("could not write extracted text".to_string()));
            }
        }
        dst.flush()?;
        Ok(())
    }
    fn extract_content(doc: &Document) -> &str {
        doc.content()
    }
    // fn extract_json(doc: String) -> Result<String, Error> {
    //     let document: serde_json::Value = serde_json::from_str(&doc)?;
    //     match &document["content"] {
    //         serde_json::Value::String(content) => Ok(content.to_string()),
    //         other => Err(Error::MalformedContent(other.clone())),
    //     }
    // }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use oscar_io::oscar_doc::{Document, Metadata};

    use super::ExtractText;
    struct TestExtract;
    impl ExtractText for TestExtract {}
    #[test]
    fn test_extract_json() {
        let test = "foo";
        let doc = Document::new(test.to_string(), HashMap::new(), Metadata::default());
        let res = TestExtract::extract_content(&doc);
        assert_eq!("foo", res);
    }
    #[test]
    fn test_extract_text() {
        let mut test = vec![
            "words like words",
            "when to use\nit",
            "not so good",
            "to start\n with",
        ]
        .into_iter()
        .map(|x| {
            Ok(Document::new(
                x.to_string(),
                HashMap::new(),
                Metadata::default(),
            ))
        });
        let mut res = vec![];
        TestExtract::extract_text(&mut test, &mut res).unwrap();
        let res = String::from_utf8_lossy(&res);
        let expected = "words like words

when to use
it

not so good

to start
 with

";
        assert_eq!(res, expected);
    }
}
