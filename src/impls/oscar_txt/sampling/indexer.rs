use std::io::{BufRead, Seek, Sink};
/// Goes through the corpus, counting byte offsets for each line.
/// It implements iterator over usizes, that are the byte offsets.
/// For convinience, also has something that implements iterator over (usize, usize) with offset and size
pub struct Indexer<R: BufRead + Seek> {
    inner: R,
    prev_offset: usize,
}

impl<R: BufRead + Seek> Indexer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            prev_offset: 0,
        }
    }
}

/// yields `(pos, size)`
impl<R: BufRead + Seek> Iterator for Indexer<R> {
    type Item = std::io::Result<(u64, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.inner.stream_position();
        match (self.inner.read_line(&mut String::new()), pos) {
            (_, Err(e)) => Some(Err(e)),
            (Err(e), _) => Some(Err(e)), //propagate error
            (Ok(0), Ok(_)) => None,      //EOF
            (Ok(num_read), Ok(pos)) => Some(Ok((pos, num_read))), //we continue
        }
    }
}

#[cfg(test)]
mod test {
    use super::Indexer;
    use std::io::BufRead;
    use std::io::Cursor;
    use std::io::Seek;
    use std::io::SeekFrom;
    #[test]
    fn test_indexer() {
        let lines = vec!["foo\n", "bar ij\n", "baz  i\n", "quux"];
        let text = lines.join("");

        let mut c = Cursor::new(text);
        let it = Indexer::new(&mut c);
        let offsets: std::io::Result<Vec<_>> = it.collect();
        let offsets = offsets.unwrap();
        let mut line_buf = String::new();
        for (idx, (offset, size)) in offsets.iter().enumerate() {
            // using offset from iterator, get content from file
            c.seek(SeekFrom::Start(*offset)).unwrap();
            c.read_line(&mut line_buf).unwrap();

            assert_eq!(line_buf, lines[idx]);
            assert_eq!(size, &lines[idx].len());
            line_buf.clear();
        }
    }
}
