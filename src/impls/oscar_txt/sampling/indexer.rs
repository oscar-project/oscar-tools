
use std::io::{BufRead, Seek, Sink};
/// Goes through the corpus, counting byte offsets for each line. 
/// It implements iterator over usizes, that are the byte offsets.
/// For convinience, also has something that implements iterator over (usize, usize) with offset and size
struct Indexer<R: BufRead + Seek> {
    inner: R,
}

impl<R: BufRead + Seek> Indexer<R> {
    pub fn new(inner: R) -> Self {
        Self {inner}
    }
}

impl<R: BufRead+Seek> Iterator for Indexer<R>  {
    type Item = std::io::Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.inner.stream_position();
        match self.inner.read_line(&mut String::new()) {
            Ok(0) => None, //EOF
            Ok(_) => Some(pos), //we continue
            Err(e) => Some(Err(e)), //propagate error
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use std::io::SeekFrom;
    use std::io::Seek;
    use std::io::BufRead;
    use super::Indexer;
    #[test]
    fn test_indexer() {
        let lines = vec![
            "foo",
            "bar ij",
            r#"baz  \ni"#,
            "quux"
        ];
        let text = lines.join("\n");

    let mut c = Cursor::new(text);
    let it = Indexer::new(&mut c);
    let offsets: std::io::Result<Vec<_>> = it.collect();
    let offsets = offsets.unwrap();
    let mut line_buf = String::new();
    for (idx, offset) in offsets.iter().enumerate() {
        c.seek(SeekFrom::Start(*offset)).unwrap();
        c.read_line(&mut line_buf).unwrap();
        assert_eq!(line_buf.trim(), lines[idx]);
        line_buf.clear();
    }

    }
}