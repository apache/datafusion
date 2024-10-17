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

//! Benchmark derived from IMDB dataset.

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::plan_err,
    error::Result,
};
mod convert;
pub use convert::ConvertOpt;

use std::fs;
mod run;
pub use run::RunOpt;

// we have 21 tables in the IMDB dataset
pub const IMDB_TABLES: &[&str] = &[
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "role_type",
    "title",
    "movie_info",
    "person_info",
];

/// Get the schema for the IMDB dataset tables
/// see benchmarks/data/imdb/schematext.sql
pub fn get_imdb_table_schema(table: &str) -> Schema {
    match table {
        "aka_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("name_pcode_cf", DataType::Utf8, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "aka_title" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("kind_id", DataType::Int32, false),
            Field::new("production_year", DataType::Int32, true),
            Field::new("phonetic_code", DataType::Utf8, true),
            Field::new("episode_of_id", DataType::Int32, true),
            Field::new("season_nr", DataType::Int32, true),
            Field::new("episode_nr", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "cast_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("person_role_id", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("nr_order", DataType::Int32, true),
            Field::new("role_id", DataType::Int32, false),
        ]),
        "char_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "comp_cast_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, false),
        ]),
        "company_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("country_code", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("name_pcode_sf", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "company_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, true),
        ]),
        "complete_cast" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, true),
            Field::new("subject_id", DataType::Int32, false),
            Field::new("status_id", DataType::Int32, false),
        ]),
        "info_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("info", DataType::Utf8, false),
        ]),
        "keyword" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("keyword", DataType::Utf8, false),
            Field::new("phonetic_code", DataType::Utf8, true),
        ]),
        "kind_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, true),
        ]),
        "link_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("link", DataType::Utf8, false),
        ]),
        "movie_companies" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("company_id", DataType::Int32, false),
            Field::new("company_type_id", DataType::Int32, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "movie_info_idx" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "movie_keyword" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("keyword_id", DataType::Int32, false),
        ]),
        "movie_link" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("linked_movie_id", DataType::Int32, false),
            Field::new("link_type_id", DataType::Int32, false),
        ]),
        "name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("gender", DataType::Utf8, true),
            Field::new("name_pcode_cf", DataType::Utf8, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "role_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("role", DataType::Utf8, false),
        ]),
        "title" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("kind_id", DataType::Int32, false),
            Field::new("production_year", DataType::Int32, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("phonetic_code", DataType::Utf8, true),
            Field::new("episode_of_id", DataType::Int32, true),
            Field::new("season_nr", DataType::Int32, true),
            Field::new("episode_nr", DataType::Int32, true),
            Field::new("series_years", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "movie_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "person_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        _ => unimplemented!("Schema for table {} is not implemented", table),
    }
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: &str) -> Result<Vec<String>> {
    let possibilities = vec![
        format!("queries/imdb/{query}.sql"),
        format!("benchmarks/queries/imdb/{query}.sql"),
    ];
    let mut errors = vec![];
    for filename in possibilities {
        match fs::read_to_string(&filename) {
            Ok(contents) => {
                return Ok(contents
                    .split(';')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect());
            }
            Err(e) => errors.push(format!("{filename}: {e}")),
        };
    }
    plan_err!("invalid query. Could not find query: {:?}", errors)
}
