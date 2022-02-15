/*! Compression operation, using gzip !*/
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

use flate2::{write::GzEncoder, Compression};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::error::Error;

pub trait Compress {
    /// Compress a file located at `src` to `dst`.
    /// If `del_src` is set to `true`, removes the file at `src` upon compression completion.
    ///
    /// `src` has to exist and be a file, and `dst` should not exist.
    fn compress_file(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error> {
        if !src.is_file() {
            warn!("{:?} is not a file: ignoring", src);
            return Ok(());
        }
        let src_file = File::open(src)?;

        // gen filename
        let filename = src.file_name().unwrap();
        let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
        let extension = String::from(dst.extension().unwrap().to_str().unwrap());
        dst.set_extension(extension + ".gz");

        info!("compressing {:?} to {:?}", src, dst);

        if dst.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{:?}", dst),
            )
            .into());
        }
        let mut dest_file = File::create(dst)?;
        compress(&mut dest_file, src_file)?;

        if del_src {
            std::fs::remove_file(src)?;
        }

        Ok(())
    }

    /// Compress files in provided folder.
    /// If `del_src` is set to `true`, removes the compressed files at `src` upon compression completion.
    /// The compression is only done at depth=1.
    /// `src` has to exist and be a file, and `dst` should not exist.
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

/// Compress a reader into a writer.
/// Consumes the whole reader.
// TODO: should it be inside the compress trait?
fn compress<T: Read>(dest_file: &mut impl Write, r: T) -> Result<(), Error> {
    let mut b = BufReader::new(r);
    let mut enc = GzEncoder::new(dest_file, Compression::default());
    let mut length = 1;
    while length > 0 {
        let buffer = b.fill_buf()?;
        enc.write_all(buffer)?;
        length = buffer.len();
        b.consume(length);
    }
    enc.try_finish()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::io::Read;

    use super::compress;

    #[test]
    fn test_compress() {
        // create content and compress
        let content = "foo";
        let mut compressed = Vec::new();
        compress(&mut compressed, content.as_bytes()).unwrap();

        let mut reader = flate2::read::GzDecoder::new(&*compressed);
        let mut decompressed = String::with_capacity(content.len());
        reader.read_to_string(&mut decompressed).unwrap();
        assert_eq!(content, decompressed);
    }
}
