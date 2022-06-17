/*! The goal is to sample from& files until it reach certain file size reached limits */
//read the files [for every new line separated senteces is sentences ]

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::collections::HashMap;
use std::path::Path;
use itertools::Itertools;
use rand::{thread_rng}; 
use rand::seq::SliceRandom;
use crate::error::{Error, self};


trait SampleText{
    fn indexing(src: &std::path::Path)->Result<HashMap<usize, usize>, Error>;
    fn sample_(collection: &HashMap<usize, usize>, max_size: usize) -> Result<Vec<usize>, Error>;
    fn get_sample(src:&Path, dst:&Path,sample_idx: &Vec<usize>) -> Result<(), Error>;
}
// do not know what is dyn is 
pub struct SampleDoc;
impl SampleText for SampleDoc{
    fn indexing(src: &Path) -> Result<HashMap<usize, usize>, error::Error>{
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let mut collection : HashMap<usize, usize> = HashMap::new();
        for (index, doc) in corpus_buf.lines().enumerate(){
            let doc = doc?;
            let doc_size = doc.len();
            collection.insert(index, doc_size);
        }
        Ok(collection)
    }

    fn sample_(collection: &HashMap<usize, usize>, max_size: usize) -> Result<Vec<usize>, Error> {
        let mut rng = thread_rng();
        let mut size = 0;
        let idx = Vec::from_iter(collection.keys());
        let mut random_idx = Vec::<usize>::new();
        loop{
            let sample= idx.choose(&mut rng).ok_or(Error::Custom("no document to sample from".to_string()))?;
            let map_length = collection.get(sample).ok_or(Error::Custom("no document to sample from".to_string()))?;

            if map_length>&max_size {
                return Err(Error::Custom("documents is larger than max size".to_string()));
            }
            if size+map_length > max_size {
                break;
            }
            random_idx.push(**sample);
            size += map_length;
        };
        random_idx.sort_unstable();
        random_idx.dedup();
        if random_idx.is_empty(){return Err(Error::Custom("no sample is selected".to_string()));}
        
        Ok(random_idx)
    }
    fn get_sample(src:&Path, dst:&Path, sample_idx: &Vec<usize>) -> Result<(), Error> {
        let corpus = File::open(&src)?;
        let corpus_buf= BufReader::new(corpus);
        let dst_file = File::create(dst)?;
        let mut dst_buf = BufWriter::new(dst_file);
        for (index, line) in corpus_buf.lines().enumerate()
        {    
            let line = line?+"\n";
            if sample_idx.iter().any(|idx| idx == &index){
               dst_buf.write_all(line.as_bytes())?;
            }
        }
     Ok(())
    }
    
}
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ops::Index};
    use itertools::{Itertools, Position};
    use tempfile::NamedTempFile;
    use std::io::{Write, Read};
    use crate::{error::{Error, self}, ops::sampling::SampleDoc};


    use super::SampleText;

    #[test]
    fn test_index(){
        let text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        let path = file.into_temp_path();
        let testmap:HashMap<usize, usize>= HashMap::from([
            (1, 48),
            (2, 269),
            (0, 26),
        ]);
        assert_eq!(SampleDoc::indexing(&path).unwrap(), testmap);
    }
    #[test]
    fn test_sample_sampling(){
        let testmap:HashMap<usize, usize>= HashMap::from([
            (1, 48),
            (2, 269),
            (0, 26),
        ]);
        //test the sampling 
        let max_size = 80;
        let sample =  SampleDoc::sample_(&testmap, max_size).unwrap();
        let iter: Vec<usize>= sample.iter().map(|x| *testmap.get(x).unwrap()).collect();
        //this will give me the index I need to sum the values corrsponding to the indecie
      
        let _sum:usize = iter.iter().sum();
        assert!(_sum <= max_size);
        }
    #[test]
    fn test_sample_sorting(){
        let testmap:HashMap<usize, usize>= HashMap::from([
            (1, 48),
            (2, 269),
            (0, 26),
        ]);
        let max_size = 80;
        let sample =  SampleDoc::sample_(&testmap, max_size).unwrap();
        let iter: Vec<usize>= sample.iter().map(|x| *testmap.get(x).unwrap()).collect();

        let _sort: Vec<usize> = sample.iter().map(|x| *testmap.get(x).unwrap()).sorted_unstable().collect();
        //test the sorting 
        assert!(_sort == iter);
    }
    #[test]
    fn test_get_sample(){
        //the function should write the sampled documents into files 
        let text = "Text messaging or texting \n or may also be sent via an Internet connection \n is the act of composing and sending electronic messages, typically consisting of alphabetic and numeric characters, between two or more users of mobile devices, desktops/laptops, or another type of compatible computer. Text messages may be sent over a cellular network";
        let mut src = NamedTempFile::new().unwrap();
        src.write_all(text.as_bytes()).unwrap();
        let src_path = src.into_temp_path();

        let dst = NamedTempFile::new().unwrap();
        let dst_path = dst.into_temp_path();

        let testmap:HashMap<usize, usize>= HashMap::from([
            (1, 48),
            (2, 269),
            (0, 26),
        ]);
        let max_size = 80;
        let sample =  SampleDoc::sample_(&testmap, max_size).unwrap();

        let _get = SampleDoc::get_sample(&src_path, &dst_path, &sample);
        let sampled_file = std::fs::read_to_string(&dst_path).unwrap();
        println!("{:?}", sampled_file);
        let get_index_text:Option<Vec<usize>> = sampled_file.lines().map(|x| text.lines().position(|y| y == x)).collect();

        println!("{:?} sample is ", sample);
        println!("{:?} get_index_text is ", get_index_text);
        assert_eq!(sample, get_index_text.unwrap());

    }
    #[test]
    fn test_get_sample_on_scale(){
        todo!()
    }
    }


    




   