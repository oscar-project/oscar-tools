/*! Compression operation, using gzip in default implementatino !*/
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

use flate2::{write::GzEncoder, Compression};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::error::Error;

const COMPRESSED_FILE_EXTS: [&'static str; 2] = ["gz", "zst"];

pub trait Compress {
    /// Compress a file located at `src` to `dst`.
    /// If `del_src` is set to `true`, removes the file at `src` upon compression completion.
    ///
    /// `src` has to exist and be a file, and `dst` should not exist.
    fn compress_file(
        src: &Path,
        dst: &Path,
        del_src: bool,
        compression: &str,
    ) -> Result<(), Error> {
        if !src.is_file() {
            warn!("{:?} is not a file: ignoring", src);
            return Ok(());
        }
        let src_file = File::open(src)?;

        // gen filename
        let filename = src.file_name().unwrap();
        let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
        if let Some(ext) = dst.extension() {
            //TODO remove unwrapping here
            let extension = String::from(ext.to_str().unwrap());
            if COMPRESSED_FILE_EXTS.contains(&extension.as_str()) {
                warn!("{:?} is already compressed! Skipping.", dst);
                return Ok(());
            }

            match compression {
                "gzip" => dst.set_extension(extension + ".gz"),
                #[cfg(feature = "zstd")]
                "zstd" => dst.set_extension(extension + ".zst"),
                _ => {
                    return Err(Error::Custom(format!(
                        "Compression {compression} not supported."
                    )))
                }
            };
        } else {
            warn!(
                "File {:?} has no extension! Fallback to {0:?}.txt.{compression}",
                dst
            );
            let extension = format!("txt.{compression}");
            dst.set_extension(extension);
        }

        info!("compressing {:?} to {:?}", src, dst);

        if dst.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{:?}", dst),
            )
            .into());
        }
        let mut dest_file = File::create(dst)?;
        compress(&mut dest_file, src_file, compression)?;

        if del_src {
            info!("removing {:?}", src);
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
        compression: &str,
    ) -> Result<(), Error> {
        //TODO: read dir
        // if file, error+ignore
        // if dir, read dir
        //     if file, compress
        //     if dir, error+ignore
        // There should be an easier way to do that.

        let files_to_compress: Result<Vec<_>, std::io::Error> = std::fs::read_dir(src)?.collect();
        let files_to_compress: Vec<PathBuf> =
            files_to_compress?.into_iter().map(|x| x.path()).collect();
        let files_to_compress = files_to_compress.into_par_iter();

        if !dst.exists() {
            debug!("Creating {:?}", dst);
            std::fs::create_dir(&dst)?;
        }
        // construct vector of errors
        let errors: Vec<Error> = files_to_compress
            .filter_map(|filepath| Self::compress_file(&filepath, dst, del_src, compression).err())
            .collect();

        if !errors.is_empty() {
            for error in &errors {
                error!("{:?}", error);
            }
        };

        Ok(())
    }

    fn compress_corpus(
        src: &Path,
        dst: &Path,
        del_src: bool,
        compression: &str,
        num_threads: usize,
    ) -> Result<(), Error> {
        if num_threads != 1 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build_global()?;
            debug!("Built rayon threadpool with num_threads={num_threads}");
        }

        if !dst.exists() {
            std::fs::create_dir(dst)?;
        }

        let language_directories: Result<Vec<_>, std::io::Error> =
            std::fs::read_dir(src)?.collect();
        let language_directories: Vec<PathBuf> = language_directories?
            .into_iter()
            .map(|x| x.path())
            .collect();
        let languages_to_compress = language_directories.into_par_iter();
        let results: Vec<Result<_, Error>> = languages_to_compress
            .map(|language_dir| {
                let file_stem = language_dir.file_name().ok_or_else(|| {
                    Error::Custom(format!("Bad file name {:?}", language_dir.file_name()))
                })?;
                let dst_folder = dst.clone().join(file_stem);
                debug!("compressing {:?} into{:?}", &language_dir, &dst_folder);

                // transform source + language
                // into dest + language
                Self::compress_folder(&language_dir, &dst_folder, del_src, compression)
            })
            .collect();
        for result in results.into_iter().filter(|r| r.is_err()) {
            error!("{:?}", result);
        }
        Ok(())
    }
}

/// Compress a reader into a writer.
/// Consumes the whole reader.
// TODO: should it be inside the compress trait?
// TODO: merge compress_gzip and compress_zstd?
fn compress<T: Read>(dest_file: &mut impl Write, r: T, compression: &str) -> Result<(), Error> {
    match compression {
        "gzip" => compress_gzip(dest_file, r)?,
        #[cfg(feature="zstd")]
        "zstd" => compress_zstd(dest_file, r)?,
        _ => panic!("Unsupported compression method. If you have selected `zstd`, be sure to have enabled the feature."),
    };
    Ok(())
}

/// compress using GZip
fn compress_gzip<T: Read>(dest_file: &mut impl Write, r: T) -> Result<(), Error> {
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

/// compress using zstd
#[cfg(feature = "zstd")]
fn compress_zstd<T: Read>(dest_file: &mut impl Write, r: T) -> Result<(), Error> {
    let mut b = BufReader::new(r);
    let mut enc = zstd::Encoder::new(dest_file, 0)?;
    let mut length = 1;
    while length > 0 {
        let buffer = b.fill_buf()?;
        enc.write_all(buffer)?;
        length = buffer.len();
        b.consume(length);
    }
    enc.do_finish()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::Write,
        io::{Cursor, Read},
    };

    use tempfile::tempdir;

    use crate::ops::{compress::compress_zstd, Compress};

    use super::compress;

    #[test]
    fn test_compress() {
        // create content and compress
        let content = "foo";
        let mut compressed = Vec::new();
        compress(&mut compressed, content.as_bytes(), "gzip").unwrap();

        let mut reader = flate2::read::GzDecoder::new(&*compressed);
        let mut decompressed = String::with_capacity(content.len());
        reader.read_to_string(&mut decompressed).unwrap();
        assert_eq!(content, decompressed);
    }

    #[test]
    fn test_compress_ztd() {
        // create content and compress
        let content = "foo";
        let mut compressed = Vec::new();
        compress(&mut compressed, content.as_bytes(), "zstd").unwrap();

        let compressed_cursor = Cursor::new(compressed);
        let decompressed = zstd::decode_all(compressed_cursor).unwrap();
        let decompressed = String::from_utf8(decompressed).unwrap();
        assert_eq!(content, decompressed);
    }
    #[test]
    fn test_dst_not_directory() {
        struct Dummy;
        impl Compress for Dummy {}

        let src = tempfile::NamedTempFile::new().unwrap();
        let dst = tempfile::NamedTempFile::new().unwrap();

        match Dummy::compress_file(src.path(), dst.path(), false, "gzip").err() {
            None => panic!("Should fail!"),
            Some(error) => match error {
                crate::error::Error::Io(_) => {
                    //when #86442 is merged
                    // assert_eq!(error.kind(), std::io::ErrorKind::NotADirectory)
                    assert!(true)
                }
                _ => panic!("wrong error type!"),
            },
        }
    }

    #[test]
    fn test_dst_exists() {
        struct Dummy;
        impl Compress for Dummy {}

        let dir = tempdir().unwrap();

        let src = dir.path().join("test.txt");
        let mut file = File::create(&src).unwrap();
        writeln!(file, "Brian was here. Briefly.").unwrap();
        let mut dst = src.clone();
        dst.set_extension("txt.gz");
        File::create(&dst).unwrap();

        match Dummy::compress_file(&src, dir.path(), false, "gzip").err() {
            None => panic!("Should fail!"),
            Some(error) => match error {
                crate::error::Error::Io(error) => {
                    assert_eq!(
                        error.kind(),
                        std::io::ErrorKind::AlreadyExists,
                        "{:?}",
                        error
                    )
                }
                _ => panic!("wrong error type!"),
            },
        }
    }
}
