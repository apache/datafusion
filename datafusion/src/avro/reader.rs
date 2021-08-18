use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::arrow::record_batch::RecordBatch;
use crate::avro::arrow_array_reader::AvroArrowArrayReader;
use crate::error::Result;
use arrow::error::Result as ArrowResult;
use avro_rs::Reader as AvroReader;
use std::io::{BufReader, Read, Seek};
use std::sync::Arc;

/// Avro file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the JSON file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the JSON structure.
    schema: Option<SchemaRef>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring JSON parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate avro_rs;
    ///
    /// use std::fs::File;
    ///
    /// fn example() -> avro_rs::Reader<File> {
    ///     let file = File::open("test/data/basic.avro").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = crate::datafusion::avro::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build::<File>(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Avro file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the Avro reader to infer the schema of the file
    pub fn infer_schema(mut self) -> Self {
        // remove any schema that is set
        self.schema = None;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<'a, R>(self, source: R) -> Result<Reader<'a, BufReader<R>>>
    where
        R: Read + Seek,
    {
        let mut buf_reader = BufReader::new(source);

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => Arc::new(infer_avro_schema_from_reader(&mut buf_reader)?),
        };
        Reader::try_new(buf_reader, schema, self.batch_size, self.projection)
    }
}

/// Avro file record  reader
pub struct Reader<'a, R: Read> {
    array_reader: AvroArrowArrayReader<'a, R>,
    schema: SchemaRef,
    batch_size: usize,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Create a new Avro Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        Ok(Self {
            array_reader: AvroArrowArrayReader::try_new(
                AvroReader::new(reader)?,
                schema.clone(),
                projection,
            )?,
            schema,
            batch_size,
        })
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> ArrowResult<Option<RecordBatch>> {
        self.array_reader.next_batch(self.batch_size)
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

/// Infer Avro schema given a reader
pub fn infer_avro_schema_from_reader<R: Read + Seek>(reader: &mut R) -> Result<Schema> {
    let avro_reader = avro_rs::Reader::new(reader)?;
    let schema = avro_reader.writer_schema();
    super::to_arrow_schema(schema)
}
