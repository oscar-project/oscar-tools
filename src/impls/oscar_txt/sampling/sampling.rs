/*! The goal is to sample from& files until it reach certain file size reached limits */
//read the files [for every new line separated senteces is sentences ]

use crate::cli::Command;
use crate::error::{self, Error};
use crate::impls::oscar_txt::sampling::indexed_reader::IndexedReader;
use crate::impls::oscar_txt::sampling::indexer::Indexer;
use crate::ops::SampleText;
use clap::arg;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::Index;
use std::path::{Path, PathBuf};



impl Command for SampleDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("sample")
            .about("sample form corpus")
            .arg(arg!([SOURCE] "Corpus source file."))
            .arg(arg!([DESTINATION] "Corpus destination file. Should not exist."))
            .arg(arg!([SIZE] "size of the sample MB"))
    }

    fn run(matches: &clap::ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        let src: PathBuf = matches.value_of("SOURCE").unwrap().into();
        let dst: PathBuf = matches.value_of("DESTINATION").unwrap().into();
        let size: usize = matches.value_of("SIZE").unwrap().parse::<usize>().unwrap() * 10usize.pow(6);

        Self::sample(&src, &dst, size)?;
        Ok(())
    }
}

// do not know what is dyn is
pub struct SampleDoc;
impl SampleDoc {
    fn indexing(src: &Path) -> Result<HashMap<u64, usize>, error::Error> {
        println!("indexing the corpus...");
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let mut collection: HashMap<usize, usize> = HashMap::new();
        let mut indexer = Indexer::new(corpus_buf);
        // let ret : std::io::Result<HashMap<_, _>>= indexer.collect();
        let ret : std::io::Result<_>= indexer.collect();
        Ok(ret?)
    }
    fn sample_(collection: &HashMap<u64, usize>, max_size: usize) -> Result<Vec<u64>, Error> {
        println!("sampling doc indices...");
        let mut rng = thread_rng();
        let mut size = 0;
        let mut idx = Vec::with_capacity(collection.len());
        idx.extend(collection.keys());
        // let idx = Vec::from_iter(collection.keys());
        let mut random_idx = Vec::new();
        loop {
            let sample = idx
                .choose(&mut rng)
                .ok_or_else(|| Error::Custom("no document to sample from".to_string()))?;
            let doc_length = collection
                .get(sample)
                .ok_or_else(|| Error::Custom("no document to sample from".to_string()))?;

            if doc_length > &max_size {
                continue;
                // return Err(Error::Custom(
                //     "documents is larger than max size".to_string(),
                // ));
            }
            if size + doc_length > max_size {
                break;
            }
            random_idx.push(*sample);
            size += doc_length;
        }
        random_idx.sort_unstable();
        random_idx.dedup();
        if random_idx.is_empty() {
            return Err(Error::Custom("no sample is selected".to_string()));
        }
        println!("done");

        Ok(random_idx)
    }
    fn get_sample(src: &Path, dst: &Path, sample_idx: &Vec<u64>) -> Result<(), Error> {
        println!("reading corpus and sampling...");
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let dst_file = File::create(dst)?;
        let mut dst_buf = BufWriter::new(dst_file);

        let it = IndexedReader::new(corpus_buf, sample_idx.into_iter().copied());
        for line in it {
            let line = line?;
            dst_buf.write(line.as_bytes())?;
        }
        dst_buf.flush()?;
        println!("done");
        Ok(())
    }
}
impl SampleText for SampleDoc {
    fn sample(src: &Path, dst: &Path, sample_size: usize) -> Result<(), Error> {
        let indices = Self::indexing(src)?;
        let indices = Self::sample_(&indices, sample_size)?;
        Self::get_sample(src, dst, &indices)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use itertools::{Itertools, Position};
    use std::io::{Read, Write};
    use std::{collections::HashMap, ops::Index};
    use tempfile::NamedTempFile;

    use crate::impls::oscar_txt::SampleDoc;

    use super::SampleText;

    #[test]
    fn test_index() {
        let text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        let path = file.into_temp_path();
        let testmap: HashMap<u64, usize> = HashMap::from([(27, 49), (76, 269), (0, 27)]);
        assert_eq!(SampleDoc::indexing(&path).unwrap(), testmap);
    }
    #[test]
    fn test_sample_sampling() {
        let testmap: HashMap<u64, usize> = HashMap::from([(1, 48), (2, 269), (0, 26)]);
        //test the sampling
        let max_size = 80;
        let sample = SampleDoc::sample_(&testmap, max_size).unwrap();
        let iter: Vec<usize> = sample.iter().map(|x| *testmap.get(x).unwrap()).collect();
        //this will give me the index I need to sum the values corrsponding to the indecie

        let _sum: usize = iter.iter().sum();
        assert!(_sum <= max_size);
    }
    #[test]
    fn test_sample_sorting() {
        let testmap: HashMap<u64, usize> = HashMap::from([(1, 48), (2, 269), (0, 26)]);
        let max_size = 80;
        let sample = SampleDoc::sample_(&testmap, max_size).unwrap();
        let iter: Vec<usize> = sample.iter().map(|x| *testmap.get(x).unwrap()).collect();

        let _sort: Vec<usize> = sample
            .iter()
            .map(|x| *testmap.get(x).unwrap())
            .sorted_unstable()
            .collect();
        //test the sorting
        assert!(_sort == iter);
    }
    #[test]
    fn test_get_sample() {
        //the function should write the sampled documents into files
        let text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
        let text = "foo
bar
baz
quux
the quick brown fox 
jumps over the lazy dog
rust is hard";
        let mut src = NamedTempFile::new().unwrap();
        src.write_all(text.as_bytes()).unwrap();
        let src_path = src.into_temp_path();

        let dst = NamedTempFile::new().unwrap();
        let dst_path = dst.into_temp_path();

        let testmap: HashMap<u64, usize> = SampleDoc::indexing(&src_path).unwrap();
        let max_size = 80;
        let sampled_offsets = SampleDoc::sample_(&testmap, max_size).unwrap();

        SampleDoc::get_sample(&src_path, &dst_path, &sampled_offsets).unwrap();
        let sampled_file = std::fs::read_to_string(&dst_path).unwrap();
        println!("{sampled_file:?}");
        // check size is correct
        assert!(sampled_file.len() < max_size);

        let corpus_lines : Vec<&str> = text.lines().collect();
        let mut positions_in_corpus = Vec::new();
        for line in sampled_file.lines() {
            let pos_in_corpus = corpus_lines.iter().position(|x| *x==line);
            assert!(pos_in_corpus.is_some());
            positions_in_corpus.push(pos_in_corpus.unwrap());
        }

        let mut expected = positions_in_corpus.clone();
        expected.sort();
        assert_eq!(positions_in_corpus, expected);
    }
    #[test]
    #[ignore]
    fn test_get_sample_on_scale() {
        todo!()
    }
}
