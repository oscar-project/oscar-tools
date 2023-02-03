/* The goal is to dedup on document level using tlsh */
//use oscar tool
//runiq and write it out
use oscar_io::v3::{Reader, Writer, WriterTrait};
use oxilangtag::LanguageTag;
use serde_json::de::Read;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

use crate::{error::Error, ops::Dedup};
use runiq::filters::{DigestFilter, Filter};

pub struct DedupDoc {
    filter: Box<dyn Filter>,
}

impl DedupDoc {
    fn new(filter: Box<dyn Filter>) -> Self {
        Self { filter }
    }
}

impl Dedup for DedupDoc {
    fn dedup(&mut self, src: &std::path::Path, dst: &std::path::Path) -> Result<(), Error> {
        let dest_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&dst)?;

        let f = File::open(src)?;
        let br = BufReader::new(f);
        let r = Reader::from_path(src)?;
        let lang = src.file_name().unwrap().to_string_lossy().to_string();
        //TODO remove unwrap()
        let lang = LanguageTag::parse(lang).expect("no language tag");

        let bw = BufWriter::new(dest_file);
        let mut w = Writer::new(dst, lang, None)?;
        for document in r {
            let doc = document?;
            if let Some(tlsh) = doc.metadata().tlsh() {
                if self.filter.detect(tlsh.as_bytes()) {
                    w.write_single(&doc)?;
                }
            }
        }
        //TODO Implment flush
        Ok(())
    }
}
impl Default for DedupDoc {
    fn default() -> Self {
        Self {
            filter: Box::new(DigestFilter::default()),
        }
    }
}

#[warn(unused_must_use)]
#[cfg(test)]
mod test {
    extern crate tempdir;
    use tempdir::TempDir;

    use clap::builder;
    use oscar_io::{
        common::Identification,
        v3::{Document, Metadata, Reader, Writer, WriterTrait},
    };
    use oxilangtag::LanguageTag;
    use std::{
        collections::{HashMap, HashSet},
        fs::metadata,
        path::Path,
    };

    use tempfile::{tempfile, NamedTempFile};

    use crate::ops::Dedup;

    use super::DedupDoc;

    #[test]
    //Create a tempdir and write tempfile to it 
    //Do the same with dst 
    fn test_simple() {
        let lang_tag1 = LanguageTag::parse("en").expect("unable to parse language tag");
        let id1 = Identification::new(lang_tag1.into(), 1.0);
        let sent_id1 = [Some(id1.clone())].to_vec();
        let mut meta1 = Metadata::new(&id1, &sent_id1);
        meta1
        .set_tlsh(Some("T15DE0459002AEB355F105360D6AA30810D52125A2DD61552DC05614388064D14500357915556541CE1AB007449E42581A48706C599009150245491711557C0612E840544355"
        .to_owned()));

        let doc1 = Document::new(
            "cvqlmd,cpqlzec;)à\"ç!(àb\"(!uyiuegfbnsoc,)az\"à(!ç".to_owned(),
            HashMap::new(),
            meta1,
        );
        let doc2 = doc1.clone();

        let mut src = NamedTempFile::new().expect("not able to open temp file");
        let src_path = src.into_temp_path();
        let doc = [doc1, doc2];
        {
            let mut src_w = Writer::new(&src_path, lang_tag1.into(), None)
                .expect("unable to write to temp file");
            src_w.write(doc.to_vec()).unwrap();
        }

        let dst = NamedTempFile::new().unwrap();
        let dst_path = dst.into_temp_path();

        let temp = TempDir::new("en").expect("unable to");
        let file_path = temp.path().join(src_path);
        
        dbg!(file_path);

        //let deduplicate = DedupDoc::default().dedup(file_path, &dst_path).unwrap();
        //dbg!(&deduplicate);
        todo!()
    }
}
