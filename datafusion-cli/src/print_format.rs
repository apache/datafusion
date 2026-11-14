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
use arrow::util::pretty::pretty_format_batches_with_options;
use comfy_table::presets::ASCII_FULL_CONDENSED;
use comfy_table::{
    ColumnConstraint, ContentArrangement, LineStyle, Table, TableStyle, Width,
};
use datafusion::config::FormatOptions;
use datafusion::error::Result;

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct TableOptions {
    pub maxrows: MaxRows,
    pub width: Option<u16>,
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

fn keep_only_maxrows(s: &str, maxrows: usize) -> String {
    let lines: Vec<String> = s.lines().map(String::from).collect();

    assert!(lines.len() >= maxrows + 4); // 4 lines for top and bottom border

    let last_line = &lines[lines.len() - 1]; // bottom border line

    let spaces = last_line.len().saturating_sub(4);
    let dotted_line = format!("| .{}|", " ".repeat(spaces));

    let mut result = lines[0..(maxrows + 3)].to_vec(); // Keep top border and `maxrows` lines
    result.extend(vec![dotted_line; 3]); // Append ... lines
    result.push(last_line.clone());

    result.join("\n")
}

fn format_batches_with_maxrows<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    maxrows: MaxRows,
    format_options: &FormatOptions,
) -> Result<()> {
    let options: arrow::util::display::FormatOptions = format_options.try_into()?;

    match maxrows {
        MaxRows::Limited(maxrows) => {
            // Filter batches to meet the maxrows condition
            let mut filtered_batches = Vec::new();
            let mut row_count: usize = 0;
            let mut over_limit = false;
            for batch in batches {
                if row_count + batch.num_rows() > maxrows {
                    // If adding this batch exceeds maxrows, slice the batch
                    let limit = maxrows - row_count;
                    let sliced_batch = batch.slice(0, limit);
                    filtered_batches.push(sliced_batch);
                    over_limit = true;
                    break;
                } else {
                    filtered_batches.push(batch.clone());
                    row_count += batch.num_rows();
                }
            }

            let formatted =
                pretty_format_batches_with_options(&filtered_batches, &options)?;
            if over_limit {
                let mut formatted_str = format!("{formatted}");
                formatted_str = keep_only_maxrows(&formatted_str, maxrows);
                writeln!(writer, "{formatted_str}")?;
            } else {
                writeln!(writer, "{formatted}")?;
            }
        }
        MaxRows::Unlimited => {
            let formatted = pretty_format_batches_with_options(batches, &options)?;
            writeln!(writer, "{formatted}")?;
        }
    }

    Ok(())
}

fn format_batches_with_width<W: std::io::Write>(
    writer: &mut W,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    maxrows: MaxRows,
    width: u16,
    format_options: &FormatOptions,
) -> Result<()> {
    if schema.fields().is_empty() || maxrows == MaxRows::Limited(0) {
        return Ok(());
    }

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

    let new_table = || {
        let mut table = Table::new();
        table
            .load_style(if width < 11 {
                TableStyle::new()
            } else {
                ASCII_FULL_CONDENSED.header_separator(LineStyle::new('+', '-', '+', '+'))
            })
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_width(width);
        table
    };

    // Reserve two content columns per cell so wide Unicode characters fit too.
    if width < 11
        || schema.fields().len().saturating_mul(5).saturating_add(1) > usize::from(width)
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
        for (index, row) in rows.into_iter().enumerate() {
            let mut table = new_table();
            if width < 11 {
                for (field, value) in schema.fields().iter().zip(row) {
                    table.add_row([field.name().as_str()]);
                    table.add_row([&value]);
                }
                for column in table.column_iter_mut() {
                    column.set_padding((0, 0));
                }
            } else {
                table.set_header(["Column", if empty { "Type" } else { "Value" }]);
                for (field, value) in schema.fields().iter().zip(row) {
                    table.add_row([field.name().as_str(), &value]);
                }
                table.set_constraints(
                    [ColumnConstraint::LowerBoundary(Width::Fixed(4)); 2],
                );
            }
            if index > 0 {
                writeln!(writer)?;
            }
            writeln!(writer, "{table}")?;
        }
        if over_limit {
            writeln!(writer, "...")?;
        }
    } else {
        let mut table = new_table();
        table.set_header(schema.fields().iter().map(|field| field.name()));
        table.set_constraints(std::iter::repeat_n(
            ColumnConstraint::LowerBoundary(Width::Fixed(4)),
            schema.fields().len(),
        ));
        table.add_rows(rows);
        if over_limit {
            table.add_row(vec!["..."; schema.fields().len()]);
        }
        writeln!(writer, "{table}")?;
    }
    Ok(())
}

pub(crate) fn print_plan_batches<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    format_options: &FormatOptions,
    width: Option<u16>,
) -> Result<()> {
    let options = format_options.try_into()?;
    let mut rows = Vec::new();
    let mut group = None;
    let mut grouped = false;
    for batch in batches {
        let kind = ArrayFormatter::try_new(batch.column(0).as_ref(), &options)?;
        let plan = ArrayFormatter::try_new(batch.column(1).as_ref(), &options)?;
        for row in 0..batch.num_rows() {
            let kind = kind.value(row).try_to_string()?;
            let (next_group, stage) = plan_group_and_stage(&kind);
            if next_group != group {
                if let Some(title) = next_group {
                    rows.push((title.to_owned(), String::new()));
                }
                group = next_group;
            }
            grouped |= next_group.is_some();
            rows.push((stage.to_owned(), plan.value(row).try_to_string()?));
        }
    }
    let type_header = if grouped { "stage" } else { "plan_type" };
    let mut table = Table::new();
    // Wrapping cells here prevents the table formatter from stripping node indentation.
    table.set_content_arrangement(ContentArrangement::Disabled);
    if let Some(width) = width.filter(|width| *width < 11) {
        table.load_style(TableStyle::new());
        let width = usize::from(width);
        for (kind, plan) in rows {
            table.add_row([format!(
                "{}\n{}\n{}\n{}",
                textwrap::fill(type_header, width),
                textwrap::fill(&kind, width),
                textwrap::fill("plan", width),
                wrap_plan(&plan, width),
            )]);
        }
        for column in table.column_iter_mut() {
            column.set_padding((0, 0));
        }
    } else if let Some(width) = width {
        let type_width = rows
            .iter()
            .map(|(kind, _)| textwrap::core::display_width(kind))
            .max()
            .unwrap_or(0)
            .max(type_header.len())
            .min(usize::from(width) - 9);
        let plan_width = usize::from(width) - type_width - 7;
        table.load_style(
            ASCII_FULL_CONDENSED.header_separator(LineStyle::new('+', '-', '+', '+')),
        );
        table.set_header([
            textwrap::fill(type_header, type_width),
            textwrap::fill("plan", plan_width),
        ]);
        for (kind, plan) in rows {
            table.add_row([
                textwrap::fill(&kind, type_width),
                wrap_plan(&plan, plan_width),
            ]);
        }
        table.set_constraints([ColumnConstraint::Absolute(Width::Fixed(
            type_width as u16 + 2,
        ))]);
    } else {
        table.load_style(
            ASCII_FULL_CONDENSED.header_separator(LineStyle::new('+', '-', '+', '+')),
        );
        table.set_header([type_header, "plan"]);
        table.add_rows(rows.into_iter().map(|(kind, plan)| [kind, plan]));
    }
    writeln!(writer, "{table}")?;
    Ok(())
}

fn plan_group_and_stage(kind: &str) -> (Option<&'static str>, &str) {
    let (group, stage) = match kind {
        "initial_logical_plan" => ("logical plan", "initial"),
        "analyzed_logical_plan" => ("logical plan", "analyzed"),
        "logical_plan" => ("logical plan", "final"),
        "initial_physical_plan" => ("physical plan", "initial"),
        "initial_physical_plan_with_stats" => ("physical plan", "initial_with_stats"),
        "initial_physical_plan_with_schema" => ("physical plan", "initial_with_schema"),
        "physical_plan" => ("physical plan", "final"),
        "physical_plan_with_stats" => ("physical plan", "final_with_stats"),
        "physical_plan_with_schema" => ("physical plan", "final_with_schema"),
        "physical_plan_error" => ("physical plan", "error"),
        _ => {
            if let Some(stage) = kind.strip_prefix("logical_plan after ") {
                ("logical plan", stage)
            } else if let Some(stage) = kind.strip_prefix("physical_plan after ") {
                ("physical plan", stage)
            } else {
                return (None, kind);
            }
        }
    };
    (Some(group), stage)
}

pub(crate) fn print_tree_batches<W: std::io::Write>(
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

fn wrap_plan(plan: &str, width: usize) -> String {
    let mut output = String::new();
    for (index, line) in plan.trim_end_matches('\n').lines().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        wrap_plan_line(&mut output, line, width);
    }
    output
}

fn wrap_plan_line(output: &mut String, line: &str, width: usize) {
    let content = line.trim_start_matches(' ');
    if content.is_empty() {
        return;
    }
    // Leave room for a wide character even when the terminal is very narrow.
    let max_indent = width.saturating_sub(2);
    let indent = (line.len() - content.len()).min(max_indent);
    let initial_indent = " ".repeat(indent);
    let subsequent_indent = " ".repeat((indent + 4).min(max_indent));
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
        let TableOptions { maxrows, width } = table_options;
        if *self == Self::Table
            && let Some(width) = width
        {
            return format_batches_with_width(
                writer,
                &schema,
                batches,
                maxrows,
                width,
                format_options,
            );
        }
        // filter out any empty batches
        let batches: Vec<_> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if batches.is_empty() {
            return self.print_empty(writer, schema, format_options);
        }

        match self {
            Self::Csv | Self::Automatic => {
                print_batches_with_sep(writer, &batches, b',', with_header)
            }
            Self::Tsv => print_batches_with_sep(writer, &batches, b'\t', with_header),
            Self::Table => {
                if maxrows == MaxRows::Limited(0) {
                    return Ok(());
                }
                format_batches_with_maxrows(writer, &batches, maxrows, format_options)
            }
            Self::Json => batches_to_json!(ArrayWriter, writer, &batches),
            Self::NdJson => batches_to_json!(LineDelimitedWriter, writer, &batches),
        }
    }

    /// Print when the result batches contain no rows
    fn print_empty<W: std::io::Write>(
        &self,
        writer: &mut W,
        schema: SchemaRef,
        format_options: &FormatOptions,
    ) -> Result<()> {
        match self {
            // Print column headers for Table format
            Self::Table if !schema.fields().is_empty() => {
                let format_options: arrow::util::display::FormatOptions =
                    format_options.try_into()?;

                let empty_batch = RecordBatch::new_empty(schema);
                let formatted =
                    pretty_format_batches_with_options(&[empty_batch], &format_options)?;
                writeln!(writer, "{formatted}")?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Time64NanosecondArray};
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
        let format_options = FormatOptions {
            time_format: Some("%Y".to_owned()),
            ..Default::default()
        };

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
