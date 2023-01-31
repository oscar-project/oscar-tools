/* The goal is to dedup on document level using tlsh */
//use oscar tool 
//runiq and write it out 
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf
};
use oscar_io::v3::{Reader, Writer, Document, WriterTrait};
use oxilangtag::LanguageTag;
use serde_json::de::Read;

use crate::{ops::Dedup, error::Error};
use runiq::filters::{DigestFilter, Filter};

pub struct DedupDoc{
    filter: Box<dyn Filter>,
}

impl DedupDoc{
    fn new(filter:Box<dyn Filter>) -> Self{
        Self{ filter }
    }
}

impl Dedup for DedupDoc{
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
            let lang = LanguageTag::parse(lang).unwrap();

            let bw = BufWriter::new(dest_file);
            let w = Writer::new(dst, lang, None)?;
            for document in r{
                let doc = document?;
                if let Some(tlsh) = doc.metadata().tlsh()
                {
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
#[cfg(test)]
mod test{
    use std::{
        collections::{HashMap, HashSet},
    };
    use clap::builder;
    use itertools::Itertools;
    use tlsh::{Tlsh, Version, BucketKind, ChecksumKind, TlshBuilder};

    use oscar_io::{
        common::Identification,
        lang::Lang,
    };
    use super::DedupDoc;

    #[test]
    fn test_simple(){
       todo!() 
    }
}