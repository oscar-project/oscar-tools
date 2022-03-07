/*! Extracts textual content into new files, discarding metadata.
!*/

use std::fs::{create_dir, File};
use std::io::{BufRead, BufReader, BufWriter};
use std::path::Path;

use rayon::string;

use crate::error::Error;
pub trait ExtractText {
    fn extract_from_path(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error> {
        let file = File::open(src)?;
        let bufread = BufReader::new(file);
        if dst.exists() {
            error!("File exist!");
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File exist {:?}", dst),
            )
            .into());
        }
        let mut dst_file = File::create(dst)?;
        let mut dst_buf =BufWriter::new(dst_file);
        Self::extact_text(bufread, & mut dst_buf)?;
        if del_src {
            std::fs::remove_file(src)?;
        }
        Ok(())
    }
    fn extact_text<T, U>(src: T, dst: &mut U) -> Result<(), Error>
    where
        T: std::io::BufRead,
        U: std::io::Write,
    {
        for line in src.lines() {
            let mut line = line?;
            let mut extracted = Self::extact_json(line)?;
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
    fn extact_json(doc: String) -> Result<String, Error> {
        let document: serde_json::Value = serde_json::from_str(&doc)?;
        match &document["content"] {
            serde_json::Value::String(content) => Ok(content.to_string()),
            other => Err(Error::MalformedContent(other.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::BufReader, os::unix::prelude::OsStrExt};

    use crate::error::Error;

    use super::ExtractText;
    struct TestExtract;
    impl ExtractText for TestExtract {}
    #[test]
    fn test_extract_json() {
        let test = r#"{"content":"foo"}"#;
        let res = TestExtract::extact_json(test.to_string());
        assert_eq!("foo", res.unwrap());
    }
    #[test]
    fn test_not_string() {
        let test = r#"{"content":22}"#;
        let res = TestExtract::extact_json(test.to_string());
        assert!(res.is_err());
        match res.unwrap_err() {
            Error::MalformedContent(_) => assert!(true),
            _ => assert!(false),
        }
    }
    #[test]
    fn test_extact_text() {
        let test = r#"{"content":"ghjj"}
        {"content":"kgjjgo \n gkel"}
        {"content":"hfirh"}
        {"content":"fghh\n gkjrgrjfj"}
        "#;
        //let mut bufread = BufReader::new(test);
        let mut results = vec![];
        TestExtract::extact_text(test.as_bytes(), &mut results);
        let results = String::from_utf8_lossy(&results);
        println!("{}", results);
    }
}
