/*! The goal is to sample from& files until it reach certain file size reached limits */
//read the files [for every new line separated senteces is sentences ]

use crate::cli::Command;
use crate::error::{self, Error};
use crate::impls::oscar_txt::sampling::indexed_reader::IndexedReader;
use crate::impls::oscar_txt::sampling::indexer::Indexer;
use crate::ops::SampleText;
use crate::ops::SamplingKind;
use clap::arg;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

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
        let size: usize =
            matches.value_of("SIZE").unwrap().parse::<usize>().unwrap() * 10usize.pow(6);

        Self::sample(&src, &dst, size, SamplingKind::WithoutReplacement)?;
        Ok(())
    }
}

// do not know what is dyn is
pub struct SampleDoc;
impl SampleDoc {
    fn build_index(src: &Path) -> Result<HashMap<u64, usize>, error::Error> {
        info!("indexing the corpus...");
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let indexer = Indexer::new(corpus_buf);
        let ret: std::io::Result<_> = indexer.collect();
        Ok(ret?)
    }
    fn sample_indices(
        collection: &HashMap<u64, usize>,
        max_size: usize,
    ) -> Result<Vec<u64>, Error> {
        info!("sampling doc indices...");
        let mut rng = thread_rng();
        let mut size = 0;
        let mut offsets = Vec::with_capacity(collection.len());
        offsets.extend(collection.keys());
        let mut sampled_offsets = Vec::new();
        loop {
            let chosen_offset = offsets
                .choose(&mut rng)
                .ok_or_else(|| Error::Custom("no document to sample from".to_string()))?;
            let doc_length = collection
                .get(chosen_offset)
                .ok_or_else(|| Error::Custom("no document to sample from".to_string()))?;

            if doc_length > &max_size {
                continue;
            }
            if size + doc_length > max_size {
                break;
            }
            sampled_offsets.push(*chosen_offset);
            size += doc_length;
        }
        sampled_offsets.sort_unstable();
        let size_before = sampled_offsets.len();
        sampled_offsets.dedup();
        if sampled_offsets.is_empty() {
            return Err(Error::Custom("no sample is selected".to_string()));
        }

        let dup_pctg = (sampled_offsets.len() as f64 / size_before as f64) * 100.0;
        info!("Dedup: keeping {:.1}% of sampled data", dup_pctg);

        Ok(sampled_offsets)
    }

    fn sample_indices_discard(
        collection: &HashMap<u64, usize>,
        max_size: usize,
    ) -> Result<Vec<u64>, Error> {
        // let doc_sizes: Vec<usize> = vec![100, 20, 120, 5, 133, 40, 894, 12, 496];
        let mut offsets = collection.keys().collect_vec();
        // let mut doc_indices = (0..400_000_000).collect_vec();
        let mut rng = rand::thread_rng();
        offsets.shuffle(&mut rng);

        let mut cur_size = 0;
        let mut sample_indices: Vec<u64> = Vec::new();
        while let Some(idx) = offsets.pop() {
            let doc_size = collection.get(idx).unwrap();

            // if doc fits, add it
            if cur_size + doc_size < max_size {
                sample_indices.push(*idx);
                cur_size += doc_size;
            // if doc is larger than max size, skip it
            } else if doc_size > &max_size {
                continue;

            // if doc doesn't fit, then we're done
            } else {
                break;
            }
        }

        sample_indices.sort_unstable();
        Ok(sample_indices)
    }

    fn write_samples(src: &Path, dst: &Path, sample_idx: &[u64]) -> Result<(), Error> {
        info!("reading corpus and writing samples...");
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let dst_file = File::create(dst)?;
        let mut dst_buf = BufWriter::new(dst_file);

        let ir = IndexedReader::new(corpus_buf, sample_idx.iter().copied());
        for line in ir {
            let line = line?;
            dst_buf.write(line.as_bytes())?;
        }
        dst_buf.flush()?;
        Ok(())
    }
}

impl SampleText for SampleDoc {
    fn sample(
        src: &Path,
        dst: &Path,
        sample_size: usize,
        sampling: SamplingKind,
    ) -> Result<(), Error> {
        //check that sample size < file size.
        let src_size = src.metadata()?.len();
        if sample_size > src_size as usize {
            return Err(Error::Custom(format!("Requested sample size is too big for the source corpus (corpus is {}MB, sample size is  {}MB)", src_size/10u64.pow(6), sample_size/10usize.pow(6))));
        }
        let indices = Self::build_index(src)?;

        let indices = match sampling {
            SamplingKind::WithReplacement => Self::sample_indices(&indices, sample_size)?,
            SamplingKind::WithoutReplacement => {
                Self::sample_indices_discard(&indices, sample_size)?
            }
        };
        // let indices = Self::sample_(&indices, sample_size)?;
        // let indices = Self::sample_discard(&indices, sample_size)?;
        Self::write_samples(src, dst, &indices)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use std::collections::HashMap;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use crate::impls::oscar_txt::SampleDoc;

    #[test]
    fn test_index() {
        let text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        let path = file.into_temp_path();
        let testmap: HashMap<u64, usize> = HashMap::from([(27, 49), (76, 269), (0, 27)]);
        assert_eq!(SampleDoc::build_index(&path).unwrap(), testmap);
    }
    #[test]
    fn test_sample_sampling() {
        let testmap: HashMap<u64, usize> = HashMap::from([(1, 48), (2, 269), (0, 26)]);
        //test the sampling
        let max_size = 80;
        let sample = SampleDoc::sample_indices(&testmap, max_size).unwrap();
        let iter: Vec<usize> = sample.iter().map(|x| *testmap.get(x).unwrap()).collect();
        //this will give me the index I need to sum the values corrsponding to the indecie

        let _sum: usize = iter.iter().sum();
        assert!(_sum <= max_size);
    }
    #[test]
    fn test_sample_sorting() {
        let testmap: HashMap<u64, usize> = HashMap::from([(1, 48), (2, 269), (0, 26)]);
        let max_size = 80;
        let sample = SampleDoc::sample_indices(&testmap, max_size).unwrap();
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
        let _text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
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

        let testmap: HashMap<u64, usize> = SampleDoc::build_index(&src_path).unwrap();
        let max_size = 80;
        let sampled_offsets = SampleDoc::sample_indices_discard(&testmap, max_size).unwrap();

        SampleDoc::write_samples(&src_path, &dst_path, &sampled_offsets).unwrap();
        let sampled_file = std::fs::read_to_string(&dst_path).unwrap();
        println!("{sampled_file:?}");
        // check size is correct
        assert!(sampled_file.len() < max_size);

        let corpus_lines: Vec<&str> = text.lines().collect();
        let mut positions_in_corpus = Vec::new();
        for line in sampled_file.lines() {
            let pos_in_corpus = corpus_lines.iter().position(|x| *x == line);
            assert!(pos_in_corpus.is_some());
            positions_in_corpus.push(pos_in_corpus.unwrap());
        }

        let mut expected = positions_in_corpus.clone();
        expected.sort_unstable();
        assert_eq!(positions_in_corpus, expected);
    }
    #[test]
    #[ignore]
    fn test_get_sample_on_scale() {
        todo!()
    }
}
