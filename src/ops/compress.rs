use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use flate2::{write::GzEncoder, Compression};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::error::Error;

pub trait Compress {
    fn compress_file(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error> {
        if !src.is_file() {
            warn!("{:?} is not a file: ignoring", src);
            return Ok(());
        }
        let src_file = File::open(src)?;
        let mut b = BufReader::new(src_file);

        // gen filename
        let filename = src.file_name().unwrap();
        let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
        let extension = String::from(dst.extension().unwrap().to_str().unwrap());
        dst.set_extension(extension + ".gz");

        info!("compressing {:?} to {:?}", src, dst);

        let dest_file = File::create(dst)?;
        let mut enc = GzEncoder::new(dest_file, Compression::default());

        let mut length = 1;
        while length > 0 {
            let buffer = b.fill_buf()?;
            enc.write_all(buffer)?;
            length = buffer.len();
            b.consume(length);
        }

        enc.try_finish()?;

        if del_src {
            std::fs::remove_file(src)?;
        }

        Ok(())
    }

    fn compress_folder(
        src: &Path,
        dst: &Path,
        del_src: bool,
        num_threads: usize,
    ) -> Result<(), Error> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()?;
        debug!("Built rayon threadpool with num_threads={num_threads}");
        // There should be an easier way to do that.
        let files_to_compress: Result<Vec<_>, std::io::Error> = std::fs::read_dir(src)?.collect();
        let files_to_compress: Vec<PathBuf> =
            files_to_compress?.into_iter().map(|x| x.path()).collect();
        let files_to_compress = files_to_compress.into_par_iter();

        // construct vector of errors
        let errors: Vec<Error> = files_to_compress
            .filter_map(|filepath| Self::compress_file(&filepath, dst, del_src).err())
            .collect();

        if !errors.is_empty() {
            for error in &errors {
                error!("{:?}", error);
            }
        };

        Ok(())
    }
}
