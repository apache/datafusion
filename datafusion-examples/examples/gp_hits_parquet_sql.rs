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

//use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::options::ReadOptions;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::error::Error;
use std::fs::File;
//use std::process::exit;
use std::string::String;
use url::Url;

use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Builder;
//mod mysub;
//mod mylog;
/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
fn main() -> Result<()> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(12)
        .thread_name("my-gp_parquet")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_time()
        .enable_io()
        .build()
        .unwrap();
    println!("calling run_queries here");
    runtime.block_on(run_queries());
    Ok(())
}

fn setup_s3(ctx: &SessionContext, bucket_name: &str) {
    let region = "us-east-1";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_access_key_id(
            env::var("AWS_ACCESS_KEY_ID").expect("expect aws access key id"),
        )
        .with_secret_access_key(
            env::var("AWS_SECRET_ACCESS_KEY").expect("expect aws secret access key"),
        )
        .build();
    match s3 {
        Ok(s3) => {
            let path = format!("s3://{bucket_name}");
            let s3_url = Url::parse(&path).unwrap();
            ctx.runtime_env()
                .register_object_store(&s3_url, Arc::new(s3));
        }
        Err(e) => {
            eprintln!("could not build S3Builder error {e}")
        }
    };
}

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct QueryRecord {
    id: i32,
    query: String,
}

fn setup_queries_from_file() -> HashMap<i32, String> {
    let file_path = get_first_arg().expect("pass in path for query file");
    let file = File::open(file_path).expect("could not open file");
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .double_quote(true)
        //        .escape(Some(b'\\'))
        .delimiter(b';')
        .from_reader(file);
    let mut queries = HashMap::new();
    for result in rdr.deserialize() {
        let record: QueryRecord =
            result.expect("could not deserialize record from query file");
        //println!("got query {} {}", record.id, record.query.clone());
        queries.insert(record.id, record.query);
    }
    queries
}

async fn run_queries() -> Result<()> {
    let bucket_name = "gayathri-sam-s3-trial";
    let config = SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);
    let ctx = SessionContext::new_with_config(config);
    setup_s3(&ctx, &bucket_name);
    let testdata = datafusion::test_util::parquet_test_data();
    let table_name = "hits";
    //let mydatadir = "/home/gayathri/gitrepo/timescaledb-osm/test/data";
    let dbprefix = "clickbench_hits/1";
    let mydatadir = format!("s3://{bucket_name}/{dbprefix}");

    // Step 1: create a bunch of ListingTableUrl for the parquet files. Associate them with a single table
    let mut myfvec = Vec::<ListingTableUrl>::new();
    //chunk names ordered by timestamp in asc
    let hits_userid_compress_vec = vec!["2", "1", "3", "4", "5"];
    let filestr_vec = hits_userid_compress_vec;
    for fil in filestr_vec {
        let myf = ListingTableUrl::parse(&format!(
            "{mydatadir}/_timescaledb_internal._hyper_1_{fil}_chunk.parquet"
        ))
        .expect("valid file name");
        //println!("added {}", myf);
        myfvec.push(myf);
    }

    //schema for the files
    // see logical parquet to arrow type mapping here:
    // https://arrow.apache.org/docs/cpp/parquet.html#types
    //For rust parquet to arrow (primitive type mapping): https://github.com/apache/arrow-rs/blob/0d031cc8aa81296cb1bdfedea7a7cb4ec6aa54ea/parquet/src/arrow/schema/primitive.rs#L86
    //TODO instead of Utf8 for string, try Utf8View /StringView etc.
    let tbl_schema = Schema::new(vec![
Field::new("watchid", DataType::Int64  , true),
Field::new("javaenable", DataType::Int16  , true),
Field::new("title", DataType::Utf8   , true),
Field::new("goodevent", DataType::Int16  , true),
Field::new("eventtime", DataType::Int64  , false),
Field::new("eventdate", DataType::UInt16 , true),
Field::new("counterid", DataType::Int32  , true),
Field::new("clientip", DataType::Int32  , true),
Field::new("regionid", DataType::Int32  , true),
Field::new("userid", DataType::Int64  , true),
Field::new("counterclass", DataType::Int16  , true),
Field::new("os", DataType::Int16  , true),
Field::new("useragent", DataType::Int16  , true),
Field::new("url", DataType::Utf8   , true),
Field::new("referer", DataType::Utf8   , true),
Field::new("isrefresh", DataType::Int16  , true),
Field::new("referercategoryid", DataType::Int16  , true),
Field::new("refererregionid", DataType::Int32  , true),
Field::new("urlcategoryid", DataType::Int16  , true),
Field::new("urlregionid", DataType::Int32  , true),
Field::new("resolutionwidth", DataType::Int16  , true),
Field::new("resolutionheight", DataType::Int16  , true),
Field::new("resolutiondepth", DataType::Int16  , true),
Field::new("flashmajor", DataType::Int16  , true),
Field::new("flashminor", DataType::Int16  , true),
Field::new("flashminor2", DataType::Utf8   , true),
Field::new("netmajor", DataType::Int16  , true),
Field::new("netminor", DataType::Int16  , true),
Field::new("useragentmajor", DataType::Int16  , true),
Field::new("useragentminor", DataType::Utf8   , true),
Field::new("cookieenable", DataType::Int16  , true),
Field::new("javascriptenable", DataType::Int16  , true),
Field::new("ismobile", DataType::Int16  , true),
Field::new("mobilephone", DataType::Int16  , true),
Field::new("mobilephonemodel", DataType::Utf8   , true),
Field::new("params", DataType::Utf8   , true),
Field::new("ipnetworkid", DataType::Int32  , true),
Field::new("traficsourceid", DataType::Int16  , true),
Field::new("searchengineid", DataType::Int16  , true),
Field::new("searchphrase", DataType::Utf8   , true),
Field::new("advengineid", DataType::Int16  , true),
Field::new("isartifical", DataType::Int16  , true),
Field::new("windowclientwidth", DataType::Int16  , true),
Field::new("windowclientheight", DataType::Int16  , true),
Field::new("clienttimezone", DataType::Int16  , true),
Field::new("clienteventtime", DataType::Int64  , true),
Field::new("silverlightversion1", DataType::Int16  , true),
Field::new("silverlightversion2", DataType::Int16  , true),
Field::new("silverlightversion3", DataType::Int32  , true),
Field::new("silverlightversion4", DataType::Int16  , true),
Field::new("pagecharset", DataType::Utf8   , true),
Field::new("codeversion", DataType::Int32  , true),
Field::new("islink", DataType::Int16  , true),
Field::new("isdownload", DataType::Int16  , true),
Field::new("isnotbounce", DataType::Int16  , true),
Field::new("funiqid", DataType::Int64  , true),
Field::new("originalurl", DataType::Utf8   , true),
Field::new("hid", DataType::Int32  , true),
Field::new("isoldcounter", DataType::Int16  , true),
Field::new("isevent", DataType::Int16  , true),
Field::new("isparameter", DataType::Int16  , true),
Field::new("dontcounthits", DataType::Int16  , true),
Field::new("withhash", DataType::Int16  , true),
Field::new("hitcolor", DataType::Utf8   , true),
Field::new("localeventtime", DataType::Int64  , true),
Field::new("age", DataType::Int16  , true),
Field::new("sex", DataType::Int16  , true),
Field::new("income", DataType::Int16  , true),
Field::new("interests", DataType::Int16  , true),
Field::new("robotness", DataType::Int16  , true),
Field::new("remoteip", DataType::Int32  , true),
Field::new("windowname", DataType::Int32  , true),
Field::new("openername", DataType::Int32  , true),
Field::new("historylength", DataType::Int16  , true),
Field::new("browserlanguage", DataType::Utf8   , true),
Field::new("browsercountry", DataType::Utf8   , true),
Field::new("socialnetwork", DataType::Utf8   , true),
Field::new("socialaction", DataType::Utf8   , true),
Field::new("httperror", DataType::Int16  , true),
Field::new("sendtiming", DataType::Int32  , true),
Field::new("dnstiming", DataType::Int32  , true),
Field::new("connecttiming", DataType::Int32  , true),
Field::new("responsestarttiming", DataType::Int32  , true),
Field::new("responseendtiming", DataType::Int32  , true),
Field::new("fetchtiming", DataType::Int32  , true),
Field::new("socialsourcenetworkid", DataType::Int16  , true),
Field::new("socialsourcepage", DataType::Utf8   , true),
Field::new("paramprice", DataType::Int64  , true),
Field::new("paramorderid", DataType::Utf8   , true),
Field::new("paramcurrency", DataType::Utf8   , true),
Field::new("paramcurrencyid", DataType::Int16  , true),
Field::new("openstatservicename", DataType::Utf8   , true),
Field::new("openstatcampaignid", DataType::Utf8   , true),
Field::new("openstatadid", DataType::Utf8   , true),
Field::new("openstatsourceid", DataType::Utf8   , true),
Field::new("utmsource", DataType::Utf8   , true),
Field::new("utmmedium", DataType::Utf8   , true),
Field::new("utmcampaign", DataType::Utf8   , true),
Field::new("utmcontent", DataType::Utf8   , true),
Field::new("utmterm", DataType::Utf8   , true),
Field::new("fromtag", DataType::Utf8   , true),
Field::new("hasgclid", DataType::Int16  , true),
Field::new("refererhash", DataType::Int64  , true),
Field::new("urlhash", DataType::Int64  , true),
Field::new("clid", DataType::Int32  , true),
    ]);

    let parquet_file_opt = ParquetReadOptions::default(); //have to eb converted to generic listing options before register_table
    let myconfig = ListingTableConfig::new_with_multi_paths(myfvec)
        .with_listing_options(
            parquet_file_opt
                .to_listing_options(&ctx.copied_config(), ctx.copied_table_options()),
        )
        .with_schema(tbl_schema.into());
    // based on register_listing_table
    let table = ListingTable::try_new(myconfig)?; //.with_definition(sql_definition);
    let tp = ctx.register_table(
        TableReference::Bare {
            table: table_name.into(),
        },
        Arc::new(table),
    );
    //println!("registered table");

    //Add schema and metadata information. we should be able to populate
    //directly?
    /*
        let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_m
    etadata.key_value_metadata(),
        )?;
        Ok(schema)
        */

    let queries = setup_queries_from_file();
    // execute the query
    for (idx, query_sql) in queries {
        println!("RUNNING query id={}", idx);
        let start = Instant::now();
        let df = ctx.sql(&query_sql).await;
        match df {
            Ok(df) => {
                // print the results

                let a = df.show().await;
                if a.is_err() {
                    println!("id {} failed with err {}", idx, a.unwrap_err());
                }
                let elapsed = start.elapsed().as_millis();
                println!("id {} elapsed is {} ms", idx, elapsed);
            }
            Err(e) => {
                println!("query id {} failed: {e}", idx);
            }
        };
    }
    println!("DONE *************");
    Ok(())
}
