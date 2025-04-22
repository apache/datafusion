// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Options related to how files should be written

pub mod arrow_writer;
pub mod avro_writer;
pub mod csv_writer;
pub mod file_type;
pub mod json_writer;
#[cfg(feature = "parquet")]
pub mod parquet_writer;

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use std::collections::HashMap;

    use crate::{
        Result,
        config::{ConfigFileType, TableOptions},
        file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
        parsers::CompressionTypeVariant,
    };

    use parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::properties::{EnabledStatistics, WriterPropertiesBuilder, WriterVersion},
        schema::types::ColumnPath,
    };

    #[test]
    fn test_writeroptions_parquet_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("format.max_row_group_size".to_owned(), "123".to_owned());
        option_map.insert("format.data_pagesize_limit".to_owned(), "123".to_owned());
        option_map.insert("format.write_batch_size".to_owned(), "123".to_owned());
        option_map.insert("format.writer_version".to_owned(), "2.0".to_owned());
        option_map.insert(
            "format.dictionary_page_size_limit".to_owned(),
            "123".to_owned(),
        );
        option_map.insert(
            "format.created_by".to_owned(),
            "df write unit test".to_owned(),
        );
        option_map.insert(
            "format.column_index_truncate_length".to_owned(),
            "123".to_owned(),
        );
        option_map.insert(
            "format.data_page_row_count_limit".to_owned(),
            "123".to_owned(),
        );
        option_map.insert("format.bloom_filter_on_write".to_owned(), "true".to_owned());
        option_map.insert("format.encoding".to_owned(), "plain".to_owned());
        option_map.insert("format.dictionary_enabled".to_owned(), "true".to_owned());
        option_map.insert("format.compression".to_owned(), "zstd(4)".to_owned());
        option_map.insert("format.statistics_enabled".to_owned(), "page".to_owned());
        option_map.insert("format.bloom_filter_fpp".to_owned(), "0.123".to_owned());
        option_map.insert("format.bloom_filter_ndv".to_owned(), "123".to_owned());

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.alter_with_string_hash_map(&option_map)?;

        let properties = WriterPropertiesBuilder::try_from(
            &table_config.parquet.with_skip_arrow_metadata(true),
        )?
        .build();

        // Verify the expected options propagated down to parquet crate WriterProperties struct
        assert_eq!(properties.max_row_group_size(), 123);
        assert_eq!(properties.data_page_size_limit(), 123);
        assert_eq!(properties.write_batch_size(), 123);
        assert_eq!(properties.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(properties.dictionary_page_size_limit(), 123);
        assert_eq!(properties.created_by(), "df write unit test");
        assert_eq!(properties.column_index_truncate_length(), Some(123));
        assert_eq!(properties.data_page_row_count_limit(), 123);
        properties
            .bloom_filter_properties(&ColumnPath::from(""))
            .expect("expected bloom filter enabled");
        assert_eq!(
            properties
                .encoding(&ColumnPath::from(""))
                .expect("expected default encoding"),
            Encoding::PLAIN
        );
        assert!(properties.dictionary_enabled(&ColumnPath::from("")));
        assert_eq!(
            properties.compression(&ColumnPath::from("")),
            Compression::ZSTD(ZstdLevel::try_new(4_i32)?)
        );
        assert_eq!(
            properties.statistics_enabled(&ColumnPath::from("")),
            EnabledStatistics::Page
        );
        assert_eq!(
            properties
                .bloom_filter_properties(&ColumnPath::from(""))
                .expect("expected bloom properties!")
                .fpp,
            0.123
        );
        assert_eq!(
            properties
                .bloom_filter_properties(&ColumnPath::from(""))
                .expect("expected bloom properties!")
                .ndv,
            123
        );

        // properties which remain as default on WriterProperties
        assert_eq!(properties.key_value_metadata(), None);
        assert_eq!(properties.sorting_columns(), None);

        Ok(())
    }

    #[test]
    fn test_writeroptions_parquet_column_specific() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();

        option_map.insert(
            "format.bloom_filter_enabled::col1".to_owned(),
            "true".to_owned(),
        );
        option_map.insert(
            "format.bloom_filter_enabled::col2.nested".to_owned(),
            "true".to_owned(),
        );
        option_map.insert("format.encoding::col1".to_owned(), "plain".to_owned());
        option_map.insert("format.encoding::col2.nested".to_owned(), "rle".to_owned());
        option_map.insert(
            "format.dictionary_enabled::col1".to_owned(),
            "true".to_owned(),
        );
        option_map.insert(
            "format.dictionary_enabled::col2.nested".to_owned(),
            "true".to_owned(),
        );
        option_map.insert("format.compression::col1".to_owned(), "zstd(4)".to_owned());
        option_map.insert(
            "format.compression::col2.nested".to_owned(),
            "zstd(10)".to_owned(),
        );
        option_map.insert(
            "format.statistics_enabled::col1".to_owned(),
            "page".to_owned(),
        );
        option_map.insert(
            "format.statistics_enabled::col2.nested".to_owned(),
            "none".to_owned(),
        );
        option_map.insert(
            "format.bloom_filter_fpp::col1".to_owned(),
            "0.123".to_owned(),
        );
        option_map.insert(
            "format.bloom_filter_fpp::col2.nested".to_owned(),
            "0.456".to_owned(),
        );
        option_map.insert("format.bloom_filter_ndv::col1".to_owned(), "123".to_owned());
        option_map.insert(
            "format.bloom_filter_ndv::col2.nested".to_owned(),
            "456".to_owned(),
        );

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.alter_with_string_hash_map(&option_map)?;

        let properties = WriterPropertiesBuilder::try_from(
            &table_config.parquet.with_skip_arrow_metadata(true),
        )?
        .build();

        let col1 = ColumnPath::from(vec!["col1".to_owned()]);
        let col2_nested = ColumnPath::from(vec!["col2".to_owned(), "nested".to_owned()]);

        // Verify the expected options propagated down to parquet crate WriterProperties struct

        properties
            .bloom_filter_properties(&col1)
            .expect("expected bloom filter enabled for col1");

        properties
            .bloom_filter_properties(&col2_nested)
            .expect("expected bloom filter enabled cor col2_nested");

        assert_eq!(
            properties.encoding(&col1).expect("expected encoding"),
            Encoding::PLAIN
        );

        assert_eq!(
            properties
                .encoding(&col2_nested)
                .expect("expected encoding"),
            Encoding::RLE
        );

        assert!(properties.dictionary_enabled(&col1));
        assert!(properties.dictionary_enabled(&col2_nested));

        assert_eq!(
            properties.compression(&col1),
            Compression::ZSTD(ZstdLevel::try_new(4_i32)?)
        );

        assert_eq!(
            properties.compression(&col2_nested),
            Compression::ZSTD(ZstdLevel::try_new(10_i32)?)
        );

        assert_eq!(
            properties.statistics_enabled(&col1),
            EnabledStatistics::Page
        );

        assert_eq!(
            properties.statistics_enabled(&col2_nested),
            EnabledStatistics::None
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col1)
                .expect("expected bloom properties!")
                .fpp,
            0.123
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col2_nested)
                .expect("expected bloom properties!")
                .fpp,
            0.456
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col1)
                .expect("expected bloom properties!")
                .ndv,
            123
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col2_nested)
                .expect("expected bloom properties!")
                .ndv,
            456
        );

        Ok(())
    }

    #[test]
    // for StatementOptions
    fn test_writeroptions_csv_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("format.has_header".to_owned(), "true".to_owned());
        option_map.insert("format.date_format".to_owned(), "123".to_owned());
        option_map.insert("format.datetime_format".to_owned(), "123".to_owned());
        option_map.insert("format.timestamp_format".to_owned(), "2.0".to_owned());
        option_map.insert("format.time_format".to_owned(), "123".to_owned());
        option_map.insert("format.null_value".to_owned(), "123".to_owned());
        option_map.insert("format.compression".to_owned(), "gzip".to_owned());
        option_map.insert("format.delimiter".to_owned(), ";".to_owned());

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::CSV);
        table_config.alter_with_string_hash_map(&option_map)?;

        let csv_options = CsvWriterOptions::try_from(&table_config.csv)?;

        let builder = csv_options.writer_options;
        assert!(builder.header());
        let buff = Vec::new();
        let _properties = builder.build(buff);
        assert_eq!(csv_options.compression, CompressionTypeVariant::GZIP);
        // TODO expand unit test if csv::WriterBuilder allows public read access to properties

        Ok(())
    }

    #[test]
    // for StatementOptions
    fn test_writeroptions_json_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("format.compression".to_owned(), "gzip".to_owned());

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::JSON);
        table_config.alter_with_string_hash_map(&option_map)?;

        let json_options = JsonWriterOptions::try_from(&table_config.json)?;
        assert_eq!(json_options.compression, CompressionTypeVariant::GZIP);

        Ok(())
    }
}
