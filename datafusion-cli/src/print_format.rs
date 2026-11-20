// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Print format variants

use std::str::FromStr;

use crate::print_options::MaxRows;

use arrow::csv::writer::WriterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use datafusion::common::format::ExplainFormat;
use datafusion::config::FormatOptions;
use datafusion::error::Result;
use tabled::builder::Builder;
use tabled::grid::{config::Position, records::RecordsMut, util::string::get_char_width};
use tabled::settings::{
    Span, Style, Width, object::Cell, peaker::Priority, style::HorizontalLine,
    width::Wrap,
};

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone, Copy)]
pub enum PrintFormat {
    Csv,
    Tsv,
    Table,
    Json,
    NdJson,
    Automatic,
}

#[derive(Debug, Clone)]
pub(crate) struct TableOptions {
    pub maxrows: MaxRows,
    pub width: Option<u16>,
    pub explain_format: Option<ExplainFormat>,
}

#[derive(Clone, Copy)]
struct PreserveIndent {
    column: usize,
    continuation_indent: usize,
}

const MIN_COLUMN_CONTENT_WIDTH: usize = 2;
const COLUMN_PADDING_WIDTH: usize = 2;
const TABLE_BORDER_WIDTH: usize = 1;

fn minimum_table_width(columns: usize) -> usize {
    columns
        .saturating_mul(
            MIN_COLUMN_CONTENT_WIDTH + COLUMN_PADDING_WIDTH + TABLE_BORDER_WIDTH,
        )
        .saturating_add(TABLE_BORDER_WIDTH)
}

impl FromStr for PrintFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

macro_rules! batches_to_json {
    ($WRITER: ident, $writer: expr, $batches: expr) => {{
        {
            if !$batches.is_empty() {
                let mut json_writer = $WRITER::new(&mut *$writer);
                for batch in $batches {
                    json_writer.write(batch)?;
                }
                json_writer.finish()?;
                json_finish!($WRITER, $writer);
            }
        }
        Ok(()) as Result<()>
    }};
}

macro_rules! json_finish {
    (ArrayWriter, $writer: expr) => {{
        writeln!($writer)?;
    }};
    (LineDelimitedWriter, $writer: expr) => {{}};
}

fn print_batches_with_sep<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    delimiter: u8,
    with_header: bool,
) -> Result<()> {
    let builder = WriterBuilder::new()
        .with_header(with_header)
        .with_delimiter(delimiter);
    let mut csv_writer = builder.build(writer);

    for batch in batches {
        csv_writer.write(batch)?;
    }

    Ok(())
}

fn print_table_batches<W: std::io::Write>(
    writer: &mut W,
    schema: SchemaRef,
    batches: &[RecordBatch],
    maxrows: MaxRows,
    width: Option<u16>,
    format_options: &FormatOptions,
    preserve_indent: Option<PreserveIndent>,
) -> Result<()> {
    let schema = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .map_or(schema, RecordBatch::schema);
    if schema.fields().is_empty() || maxrows == MaxRows::Limited(0) {
        return Ok(());
    }

    let headers: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if format_options.types_info {
                format!("{}\n{}", field.name(), field.data_type())
            } else {
                field.name().to_owned()
            }
        })
        .collect();
    let options = format_options.try_into()?;
    let limit = match maxrows {
        MaxRows::Limited(limit) => limit,
        MaxRows::Unlimited => usize::MAX,
    };
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut over_limit = false;
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|array| ArrayFormatter::try_new(array.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            if rows.len() == limit {
                over_limit = true;
                break;
            }
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).try_to_string())
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
        }
        if over_limit {
            break;
        }
    }

    if let Some(width) = width
        && usize::from(width) < minimum_table_width(schema.fields().len().max(2))
    {
        let empty = rows.is_empty();
        if empty {
            rows.push(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.data_type().to_string())
                    .collect(),
            );
        }
        let visible_rows = rows.len();
        for (index, row) in rows.into_iter().enumerate() {
            if index > 0 {
                writeln!(writer)?;
            }
            if usize::from(width) < minimum_table_width(2) {
                for (column, (header, value)) in headers.iter().zip(row).enumerate() {
                    print_wrapped(writer, header, width, None)?;
                    print_wrapped(
                        writer,
                        &value,
                        width,
                        preserve_indent
                            .filter(|option| option.column == column)
                            .map(|option| option.continuation_indent),
                    )?;
                }
                if over_limit && index + 1 == visible_rows {
                    write!(writer, ".\n.\n.\n")?;
                }
                continue;
            }
            let vertical_rows: Vec<_> = headers
                .iter()
                .zip(row)
                .map(|(header, value)| vec![header.clone(), value])
                .collect();
            let vertical_header = [
                "Column".to_owned(),
                if empty { "Type" } else { "Value" }.to_owned(),
            ];
            print_table(
                writer,
                &vertical_header,
                &vertical_rows,
                Some(width),
                over_limit && index + 1 == visible_rows,
                None,
            )?;
        }
    } else {
        print_table(writer, &headers, &rows, width, over_limit, preserve_indent)?;
    }
    Ok(())
}

fn print_table<W: std::io::Write>(
    writer: &mut W,
    headers: &[String],
    rows: &[Vec<String>],
    width: Option<u16>,
    over_limit: bool,
    preserve_indent: Option<PreserveIndent>,
) -> Result<()> {
    let mut builder = Builder::default();
    for row in std::iter::once(headers).chain(rows.iter().map(Vec::as_slice)) {
        builder.push_record(row);
    }
    let marker_start = rows.len() + 1;
    let column_count = builder.count_columns();
    if column_count == 0 {
        return Ok(());
    }
    if over_limit {
        for _ in 0..3 {
            builder.push_record(
                std::iter::once(".").chain(std::iter::repeat_n("", column_count - 1)),
            );
        }
    }
    let mut table = builder.build();
    table.with(
        Style::ascii()
            .remove_horizontal()
            .horizontals([(1, HorizontalLine::full('-', '+', '+', '+'))]),
    );
    if over_limit {
        for row in marker_start..marker_start + 3 {
            table.modify(Cell::new(row, 0), Span::column(0));
        }
    }
    if let Some(width) = width {
        table.with(
            Width::wrap(usize::from(width))
                .keep_words(true)
                .priority(Priority::max(false)),
        );
        if let Some(option) = preserve_indent {
            let widths = table
                .get_dimension()
                .get_widths()
                .expect("Width::wrap estimates column widths")
                .to_vec();
            for (row_index, row) in rows.iter().enumerate() {
                let text = wrap_indented(
                    &row[option.column],
                    widths[option.column].saturating_sub(COLUMN_PADDING_WIDTH),
                    option.continuation_indent,
                );
                table
                    .get_records_mut()
                    .set(Position::new(row_index + 1, option.column), text);
            }
            table.get_dimension_mut().clear_height();
        }
    }
    writeln!(writer, "{table}")?;
    Ok(())
}

fn print_tree_batches<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    format_options: &FormatOptions,
) -> Result<()> {
    let options = format_options.try_into()?;
    for batch in batches {
        let kind = ArrayFormatter::try_new(batch.column(0).as_ref(), &options)?;
        let plan = ArrayFormatter::try_new(batch.column(1).as_ref(), &options)?;
        for row in 0..batch.num_rows() {
            writeln!(writer, "{}", kind.value(row).try_to_string()?)?;
            writeln!(writer, "{}", plan.value(row).try_to_string()?)?;
        }
    }
    Ok(())
}

fn print_wrapped<W: std::io::Write>(
    writer: &mut W,
    text: &str,
    width: u16,
    continuation_indent: Option<usize>,
) -> Result<()> {
    let width = usize::from(width);
    if continuation_indent.is_none()
        && text
            .lines()
            .all(|line| textwrap::core::display_width(line) <= width)
    {
        writeln!(writer, "{text}")?;
        return Ok(());
    }
    let text = match continuation_indent {
        Some(indent) => wrap_indented(text, width, indent),
        None => {
            // Keep an indivisible character intact even if it exceeds the target width.
            let width = text.chars().map(get_char_width).fold(width, usize::max);
            Wrap::wrap(text, width, true)
        }
    };
    writeln!(writer, "{text}")?;
    Ok(())
}

fn wrap_indented(text: &str, width: usize, continuation_indent: usize) -> String {
    let mut output = String::new();
    for (index, line) in text.trim_end_matches('\n').split('\n').enumerate() {
        if index > 0 {
            output.push('\n');
        }
        if textwrap::core::display_width(line) <= width {
            output.push_str(line);
        } else {
            wrap_indented_line(&mut output, line, width, continuation_indent);
        }
    }
    output
}

fn wrap_indented_line(
    output: &mut String,
    line: &str,
    width: usize,
    continuation_indent: usize,
) {
    let content = line.trim_start_matches(' ');
    if content.is_empty() {
        return;
    }
    let content_width = if content.is_ascii() { 1 } else { 2 };
    let max_indent = width.saturating_sub(content_width);
    let indent = (line.len() - content.len()).min(max_indent);
    let initial_indent = " ".repeat(indent);
    let subsequent_indent = " ".repeat((indent + continuation_indent).min(max_indent));
    let options = textwrap::Options::new(width)
        .initial_indent(&initial_indent)
        .subsequent_indent(&subsequent_indent)
        .word_separator(textwrap::WordSeparator::AsciiSpace)
        .word_splitter(textwrap::WordSplitter::NoHyphenation)
        .wrap_algorithm(textwrap::WrapAlgorithm::FirstFit);
    output.push_str(&textwrap::fill(content, options));
}

impl PrintFormat {
    /// Print the batches to a writer using the specified format
    pub fn print_batches<W: std::io::Write>(
        &self,
        writer: &mut W,
        schema: SchemaRef,
        batches: &[RecordBatch],
        maxrows: MaxRows,
        with_header: bool,
        format_options: &FormatOptions,
    ) -> Result<()> {
        self.print_batches_with_options(
            writer,
            schema,
            batches,
            TableOptions {
                maxrows,
                width: None,
                explain_format: None,
            },
            with_header,
            format_options,
        )
    }

    pub(crate) fn print_batches_with_options<W: std::io::Write>(
        &self,
        writer: &mut W,
        schema: SchemaRef,
        batches: &[RecordBatch],
        table_options: TableOptions,
        with_header: bool,
        format_options: &FormatOptions,
    ) -> Result<()> {
        let TableOptions {
            maxrows,
            width,
            explain_format,
        } = table_options;
        if *self == Self::Table {
            if explain_format == Some(ExplainFormat::Tree) && width.is_some() {
                return print_tree_batches(writer, batches, format_options);
            }
            let continuation_indent = match explain_format {
                Some(ExplainFormat::Indent) => Some(4),
                Some(ExplainFormat::PostgresJSON) => Some(0),
                _ => None,
            };
            return print_table_batches(
                writer,
                schema,
                batches,
                maxrows,
                width,
                format_options,
                continuation_indent.map(|continuation_indent| PreserveIndent {
                    column: 1,
                    continuation_indent,
                }),
            );
        }
        // filter out any empty batches
        let batches: Vec<_> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if batches.is_empty() {
            return Ok(());
        }

        match self {
            Self::Csv | Self::Automatic => {
                print_batches_with_sep(writer, &batches, b',', with_header)
            }
            Self::Tsv => print_batches_with_sep(writer, &batches, b'\t', with_header),
            Self::Table => unreachable!("table output is handled above"),
            Self::Json => batches_to_json!(ArrayWriter, writer, &batches),
            Self::NdJson => batches_to_json!(LineDelimitedWriter, writer, &batches),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray, Time64NanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use insta::{allow_duplicates, assert_snapshot};

    #[test]
    fn print_empty() {
        for format in [
            PrintFormat::Csv,
            PrintFormat::Tsv,
            PrintFormat::Json,
            PrintFormat::NdJson,
            PrintFormat::Automatic,
        ] {
            // no output for empty batches, even with header set
            let output = PrintBatchesTest::new()
                .with_format(format)
                .with_schema(three_column_schema())
                .with_batches(vec![])
                .run();
            assert_eq!(output, "")
        }

        // output column headers for empty batches when format is Table
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(three_column_schema())
            .with_batches(vec![])
            .run();
        assert_snapshot!(output, @r"
        +---+---+---+
        | a | b | c |
        +---+---+---+
        ");
    }

    #[test]
    fn print_csv_no_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::No)
            .run();
        assert_snapshot!(output, @r"
        1,4,7
        2,5,8
        3,6,9
        ");
    }

    #[test]
    fn print_csv_with_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Yes)
            .run();
        assert_snapshot!(output, @r"
        a,b,c
        1,4,7
        2,5,8
        3,6,9
        ");
    }

    #[test]
    fn print_tsv_no_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::No)
            .run();
        assert_snapshot!(output, @r"
        1	4	7
        2	5	8
        3	6	9
        ")
    }

    #[test]
    fn print_tsv_with_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Yes)
            .run();
        assert_snapshot!(output, @r"
        a	b	c
        1	4	7
        2	5	8
        3	6	9
        ");
    }

    #[test]
    fn print_table() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Ignored)
            .run();
        assert_snapshot!(output, @r"
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | 4 | 7 |
        | 2 | 5 | 8 |
        | 3 | 6 | 9 |
        +---+---+---+
        ");
    }
    #[test]
    fn print_json() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Json)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Ignored)
            .run();
        assert_snapshot!(output, @r#"[{"a":1,"b":4,"c":7},{"a":2,"b":5,"c":8},{"a":3,"b":6,"c":9}]"#);
    }

    #[test]
    fn print_ndjson() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::NdJson)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Ignored)
            .run();
        assert_snapshot!(output, @r#"
        {"a":1,"b":4,"c":7}
        {"a":2,"b":5,"c":8}
        {"a":3,"b":6,"c":9}
        "#);
    }

    #[test]
    fn print_automatic_no_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::No)
            .run();
        assert_snapshot!(output, @r"
        1,4,7
        2,5,8
        3,6,9
        ");
    }
    #[test]
    fn print_automatic_with_header() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(&three_column_batch()))
            .with_header(WithHeader::Yes)
            .run();
        assert_snapshot!(output, @r"
        a,b,c
        1,4,7
        2,5,8
        3,6,9
        ");
    }

    #[test]
    fn print_maxrows_unlimited() {
        // should print out entire output with no truncation if unlimited or
        // limit greater than number of batches or equal to the number of batches
        for max_rows in [MaxRows::Unlimited, MaxRows::Limited(5), MaxRows::Limited(3)] {
            let output = PrintBatchesTest::new()
                .with_format(PrintFormat::Table)
                .with_schema(one_column_schema())
                .with_batches(vec![one_column_batch()])
                .with_maxrows(max_rows)
                .run();
            allow_duplicates! {
                assert_snapshot!(output, @r"
                +---+
                | a |
                +---+
                | 1 |
                | 2 |
                | 3 |
                +---+
                ");
            }
        }
    }

    #[test]
    fn print_maxrows_limited_one_batch() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![one_column_batch()])
            .with_maxrows(MaxRows::Limited(1))
            .run();
        assert_snapshot!(output, @r"
        +---+
        | a |
        +---+
        | 1 |
        | . |
        | . |
        | . |
        +---+
        ");
    }

    #[test]
    fn print_maxrows_limited_multi_batched() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![
                one_column_batch(),
                one_column_batch(),
                one_column_batch(),
            ])
            .with_maxrows(MaxRows::Limited(5))
            .run();
        assert_snapshot!(output, @r"
        +---+
        | a |
        +---+
        | 1 |
        | 2 |
        | 3 |
        | 1 |
        | 2 |
        | . |
        | . |
        | . |
        +---+
        ");
    }

    #[test]
    fn test_print_batches_empty_batches() {
        let batch = one_column_batch();
        let empty_batch = RecordBatch::new_empty(batch.schema());

        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![empty_batch.clone(), batch, empty_batch])
            .run();
        assert_snapshot!(output, @r"
        +---+
        | a |
        +---+
        | 1 |
        | 2 |
        | 3 |
        +---+
        ");
    }

    #[test]
    fn test_print_batches_empty_batch() {
        let empty_batch = RecordBatch::new_empty(one_column_batch().schema());

        // Print column headers for empty batch when format is Table
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(one_column_schema())
            .with_batches(vec![empty_batch])
            .with_header(WithHeader::Yes)
            .run();
        assert_snapshot!(output, @r"
        +---+
        | a |
        +---+
        ");

        // No output for empty batch when schema contains no columns
        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(Arc::new(Schema::empty()))
            .with_batches(vec![empty_batch])
            .with_header(WithHeader::Yes)
            .run();
        assert_eq!(output, "")
    }

    #[test]
    fn narrow_tables_preserve_wide_characters() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        for (width, value, expected) in [
            (1, "中", "s\n中\n"),
            (1, "ab", "s\na\nb\n"),
            (1, "a中b", "s\na\n中\nb\n"),
            (1, "🙂", "s\n🙂\n"),
            (2, "中", "s\n中\n"),
        ] {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![value]))],
            )?;
            let mut output = Vec::new();
            PrintFormat::Table.print_batches_with_options(
                &mut output,
                Arc::clone(&schema),
                &[batch],
                TableOptions {
                    maxrows: MaxRows::Unlimited,
                    width: Some(width),
                    explain_format: None,
                },
                true,
                &FormatOptions::default(),
            )?;
            assert_eq!(
                String::from_utf8(output).unwrap(),
                expected,
                "width {width}, value {value:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn invalid_temporal_format_errors_before_table_output() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Time64NanosecondArray::from(vec![1_000, 2_000]))],
        )
        .unwrap();
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let mut format_options = FormatOptions::default();
        format_options.time_format = Some("%Y".to_owned());

        for batches in [vec![batch], vec![empty_batch]] {
            let mut output = Vec::new();
            let error = PrintFormat::Table
                .print_batches(
                    &mut output,
                    schema.clone(),
                    &batches,
                    MaxRows::Limited(1),
                    true,
                    &format_options,
                )
                .expect_err("incompatible time formats must fail before writing output");

            assert!(
                error.to_string().contains("datafusion.format.time_format"),
                "unexpected error: {error}"
            );
            assert!(
                output.is_empty(),
                "format errors must not write partial output"
            );
        }
    }

    #[derive(Debug)]
    struct PrintBatchesTest {
        format: PrintFormat,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        maxrows: MaxRows,
        with_header: WithHeader,
    }

    /// How to test with_header
    #[derive(Debug, Clone)]
    enum WithHeader {
        Yes,
        No,
        /// output should be the same with or without header
        Ignored,
    }

    impl PrintBatchesTest {
        fn new() -> Self {
            Self {
                format: PrintFormat::Table,
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                maxrows: MaxRows::Unlimited,
                with_header: WithHeader::Ignored,
            }
        }

        /// set the format
        fn with_format(mut self, format: PrintFormat) -> Self {
            self.format = format;
            self
        }

        // set the schema
        fn with_schema(mut self, schema: SchemaRef) -> Self {
            self.schema = schema;
            self
        }

        /// set the batches to convert
        fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
            self.batches = batches;
            self
        }

        /// set maxrows
        fn with_maxrows(mut self, maxrows: MaxRows) -> Self {
            self.maxrows = maxrows;
            self
        }

        /// set with_header
        fn with_header(mut self, with_header: WithHeader) -> Self {
            self.with_header = with_header;
            self
        }

        /// run the test
        /// formats batches using parameters and returns the resulting output
        fn run(self) -> String {
            match self.with_header {
                WithHeader::Yes => self.output_with_header(true),
                WithHeader::No => self.output_with_header(false),
                WithHeader::Ignored => {
                    let output = self.output_with_header(true);
                    // ensure the output is the same without header
                    let output_without_header = self.output_with_header(false);
                    assert_eq!(
                        output, output_without_header,
                        "Expected output to be the same with or without header"
                    );
                    output
                }
            }
        }

        fn output_with_header(&self, with_header: bool) -> String {
            let mut buffer: Vec<u8> = vec![];
            self.format
                .print_batches(
                    &mut buffer,
                    self.schema.clone(),
                    &self.batches,
                    self.maxrows,
                    with_header,
                    &FormatOptions::default(),
                )
                .unwrap();
            String::from_utf8(buffer).unwrap()
        }
    }

    /// Return a schema with three columns
    fn three_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]))
    }

    /// Return a batch with three columns and three rows
    fn three_column_batch() -> RecordBatch {
        RecordBatch::try_new(
            three_column_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap()
    }

    /// Return a schema with one column
    fn one_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// return a batch with one column and three rows
    fn one_column_batch() -> RecordBatch {
        RecordBatch::try_new(
            one_column_schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[test]
    fn print_maxrows_limited_wide_table() {
        let output = PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![wide_column_batch()])
            .with_maxrows(MaxRows::Limited(1))
            .run();
        assert_snapshot!(output, @r"
        +----+----+----+----+----+----+----+----+----+----+
        | c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 |
        +----+----+----+----+----+----+----+----+----+----+
        | 0  | 0  | 0  | 0  | 0  | 0  | 0  | 0  | 0  | 0  |
        | .                                               |
        | .                                               |
        | .                                               |
        +----+----+----+----+----+----+----+----+----+----+
        ");
    }

    /// return a schema with many columns (to exercise wide table formatting)
    fn wide_column_schema() -> SchemaRef {
        let fields: Vec<Field> = (0..10)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// return a batch with many columns and three rows
    fn wide_column_batch() -> RecordBatch {
        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..10)
            .map(|_| Arc::new(Int32Array::from(vec![0, 1, 2])) as _)
            .collect();
        RecordBatch::try_new(wide_column_schema(), arrays).unwrap()
    }

    /// Slice the record batch into 2 batches
    fn split_batch(batch: &RecordBatch) -> Vec<RecordBatch> {
        assert!(batch.num_rows() > 1);
        let split = batch.num_rows() / 2;
        vec![
            batch.slice(0, split),
            batch.slice(split, batch.num_rows() - split),
        ]
    }
}
