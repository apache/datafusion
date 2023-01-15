use std::ops::Deref;
use std::rc::Rc;
use std::result;

use crate::antlr::presto::prestolexer::PrestoLexer;
use crate::antlr::presto::prestoparser::{PrestoParser, SingleStatementContextAll};
use antlr_rust::char_stream::{CharStream, InputData};
use antlr_rust::common_token_stream::CommonTokenStream;
use antlr_rust::errors::ANTLRError;
use antlr_rust::int_stream::{self, IntStream};
use antlr_rust::token_factory::ArenaCommonFactory;

#[derive(Debug)]
pub struct CaseInsensitiveInputStream<Data: Deref> {
    name: String,
    data_raw: Data,
    index: isize,
}

better_any::tid! {impl<'a, T: 'static> TidAble<'a> for CaseInsensitiveInputStream<&'a T> where T: ?Sized}
better_any::tid! {impl<'a, T: 'static> TidAble<'a> for CaseInsensitiveInputStream<Box<T>> where T: ?Sized}

impl<'a, T: From<&'a str>> CharStream<T> for CaseInsensitiveInputStream<&'a str> {
    #[inline]
    fn get_text(&self, start: isize, stop: isize) -> T {
        self.get_text_inner(start, stop).into()
    }
}

impl<'a, Data> CaseInsensitiveInputStream<&'a Data>
where
    Data: ?Sized + InputData,
{
    fn get_text_inner(&self, start: isize, stop: isize) -> &'a Data {
        // println!("get text {}..{} of {:?}",start,stop,self.data_raw.to_display());
        let start = start as usize;
        let stop = self.data_raw.offset(stop, 1).unwrap_or(stop) as usize;
        // println!("justed range {}..{} ",start,stop);
        // let start = self.data_raw.offset(0,start).unwrap() as usize;
        // let stop = self.data_raw.offset(0,stop + 1).unwrap() as usize;

        if stop < self.data_raw.len() {
            &self.data_raw[start..stop]
        } else {
            &self.data_raw[start..]
        }
    }

    /// Creates new `InputStream` over borrowed data
    pub fn new(data_raw: &'a Data) -> Self {
        // let data_raw = data_raw.as_ref();
        // let data = data_raw.to_indexed_vec();
        Self {
            name: "<empty>".to_string(),
            data_raw,
            index: 0,
            // phantom: Default::default(),
        }
    }
}
impl<'a, Data: Deref> IntStream for CaseInsensitiveInputStream<Data>
where
    Data::Target: InputData,
{
    #[inline]
    fn consume(&mut self) {
        if let Some(index) = self.data_raw.offset(self.index, 1) {
            self.index = index;
            // self.current = self.data_raw.deref().item(index).unwrap_or(TOKEN_EOF);
            // Ok(())
        } else {
            panic!("cannot consume EOF");
        }
    }

    #[inline]
    fn la(&mut self, mut offset: isize) -> isize {
        if offset == 1 {
            return match self.data_raw.item(self.index) {
                Some(v) => match v {
                    97..=122 => v - 33,
                    _ => v,
                },
                None => int_stream::EOF,
            };
        }
        if offset == 0 {
            panic!("should not be called with offset 0");
        }
        if offset < 0 {
            offset += 1; // e.g., translate LA(-1) to use offset i=0; then data[p+0-1]
        }

        match self
            .data_raw
            .offset(self.index, offset - 1)
            .and_then(|index| self.data_raw.item(index))
        {
            Some(v) => match v {
                97..=122 => v - 33,
                _ => v,
            },
            None => int_stream::EOF,
        }
    }

    #[inline]
    fn mark(&mut self) -> isize {
        -1
    }

    #[inline]
    fn release(&mut self, _marker: isize) {}

    #[inline]
    fn index(&self) -> isize {
        self.index
    }

    #[inline]
    fn seek(&mut self, index: isize) {
        self.index = index
    }

    #[inline]
    fn size(&self) -> isize {
        self.data_raw.len() as isize
    }

    fn get_source_name(&self) -> String {
        self.name.clone()
    }
}

pub fn parse<'input>(
    sql: &'input str,
    tf: &'input ArenaCommonFactory<'input>,
) -> result::Result<Rc<SingleStatementContextAll<'input>>, ANTLRError> {
    let mut _lexer: PrestoLexer<'input, CaseInsensitiveInputStream<&'input str>> =
        PrestoLexer::new_with_token_factory(CaseInsensitiveInputStream::new(&sql), &tf);
    let token_source = CommonTokenStream::new(_lexer);
    let mut parser = PrestoParser::new(token_source);
    parser.singleStatement()
}
