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

//! DataFusion Configuration Options

use crate::{DataFusionError, Result};
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Display;

/// A macro that wraps a configuration struct and automatically derives
/// [`Default`] and [`ConfigField`] for it, allowing it to be used
/// in the [`ConfigOptions`] configuration tree
///
/// For example,
///
/// ```ignore
/// config_namespace! {
///    /// Amazing config
///    pub struct MyConfig {
///        /// Field 1 doc
///        field1: String, default = "".to_string()
///
///        /// Field 2 doc
///        field2: usize, default = 232
///
///        /// Field 3 doc
///        field3: Option<usize>, default = None
///    }
///}
/// ```
///
/// Will generate
///
/// ```ignore
/// /// Amazing config
/// #[derive(Debug, Clone)]
/// #[non_exhaustive]
/// pub struct MyConfig {
///     /// Field 1 doc
///     field1: String,
///     /// Field 2 doc
///     field2: usize,
///     /// Field 3 doc
///     field3: Option<usize>,
/// }
/// impl ConfigField for MyConfig {
///     fn set(&mut self, key: &str, value: &str) -> Result<()> {
///         let (key, rem) = key.split_once('.').unwrap_or((key, ""));
///         match key {
///             "field1" => self.field1.set(rem, value),
///             "field2" => self.field2.set(rem, value),
///             "field3" => self.field3.set(rem, value),
///             _ => Err(DataFusionError::Internal(format!(
///                 "Config value \"{}\" not found on MyConfig",
///                 key
///             ))),
///         }
///     }
///
///     fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
///         let key = format!("{}.field1", key_prefix);
///         let desc = "Field 1 doc";
///         self.field1.visit(v, key.as_str(), desc);
///         let key = format!("{}.field2", key_prefix);
///         let desc = "Field 2 doc";
///         self.field2.visit(v, key.as_str(), desc);
///         let key = format!("{}.field3", key_prefix);
///         let desc = "Field 3 doc";
///         self.field3.visit(v, key.as_str(), desc);
///     }
/// }
///
/// impl Default for MyConfig {
///     fn default() -> Self {
///         Self {
///             field1: "".to_string(),
///             field2: 232,
///             field3: None,
///         }
///     }
/// }
/// ```
///
/// NB: Misplaced commas may result in nonsensical errors
///
macro_rules! config_namespace {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {

        $(#[doc = $struct_d])*
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                       stringify!($field_name) => self.$field_name.set(rem, value),
                    )*
                    _ => Err(DataFusionError::Internal(
                        format!(concat!("Config value \"{}\" not found on ", stringify!($struct_name)), key)
                    ))
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                let desc = concat!($($d),*).trim();
                self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }
    }
}

config_namespace! {
    /// Options related to catalog and directory scanning
    pub struct CatalogOptions {
        /// Number of partitions for query execution. Increasing partitions can increase
        /// concurrency. Defaults to the number of cpu cores on the system.
        pub create_default_catalog_and_schema: bool, default = true

        /// The default catalog name - this impacts what SQL queries use if not specified
        pub default_catalog: String, default = "datafusion".to_string()

        /// The default schema name - this impacts what SQL queries use if not specified
        pub default_schema: String, default = "public".to_string()

        /// Should DataFusion provide access to `information_schema`
        /// virtual tables for displaying schema information
        pub information_schema: bool, default = false

        /// Location scanned to load tables for `default` schema
        pub location: Option<String>, default = None

        /// Type of `TableProvider` to use when loading `default` schema
        pub format: Option<String>, default = None

        /// If the file has a header
        pub has_header: bool, default = false
    }
}

config_namespace! {
    /// Options related to query execution
    pub struct ExecutionOptions {
        /// Default batch size while creating new batches, it's especially useful for
        /// buffer-in-memory batches since creating tiny batches would results in too much
        /// metadata memory consumption
        pub batch_size: usize, default = 8192

        /// When set to true, record batches will be examined between each operator and
        /// small batches will be coalesced into larger batches. This is helpful when there
        /// are highly selective filters or joins that could produce tiny output batches. The
        /// target batch size is determined by the configuration setting
        pub coalesce_batches: bool, default = true

        /// Should DataFusion collect statistics after listing files
        pub collect_statistics: bool, default = false

        /// Number of partitions for query execution. Increasing partitions can increase
        /// concurrency. Defaults to the number of cpu cores on the system
        pub target_partitions: usize, default = num_cpus::get()

        /// The default time zone
        ///
        /// Some functions, e.g. EXTRACT(HOUR from SOME_TIME), shift the underlying datetime
        /// according to this time zone, and then extract the hour
        pub time_zone: Option<String>, default = Some("+00:00".into())

        /// Parquet options
        pub parquet: ParquetOptions, default = Default::default()
    }
}

config_namespace! {
    /// Options related to reading of parquet files
    pub struct ParquetOptions {
        /// If true, uses parquet data page level metadata (Page Index) statistics
        /// to reduce the number of rows decoded.
        pub enable_page_index: bool, default = false

        /// If true, the parquet reader attempts to skip entire row groups based
        /// on the predicate in the query and the metadata (min/max values) stored in
        /// the parquet file
        pub pruning: bool, default = true

        /// If true, the parquet reader skip the optional embedded metadata that may be in
        /// the file Schema. This setting can help avoid schema conflicts when querying
        /// multiple parquet files with schemas containing compatible types but different metadata
        pub skip_metadata: bool, default = true

        /// If specified, the parquet reader will try and fetch the last `size_hint`
        /// bytes of the parquet file optimistically. If not specified, two read are required:
        /// One read to fetch the 8-byte parquet footer and
        /// another to fetch the metadata length encoded in the footer
        pub metadata_size_hint: Option<usize>, default = None

        /// If true, filter expressions are be applied during the parquet decoding operation to
        /// reduce the number of rows decoded
        pub pushdown_filters: bool, default = false

        /// If true, filter expressions evaluated during the parquet decoding operation
        /// will be reordered heuristically to minimize the cost of evaluation. If false,
        /// the filters are applied in the same order as written in the query
        pub reorder_filters: bool, default = false
    }
}

config_namespace! {
    /// Options related to query optimization
    pub struct OptimizerOptions {
        /// When set to true, the physical plan optimizer will try to add round robin
        /// repartition to increase parallelism to leverage more CPU cores
        pub enable_round_robin_repartition: bool, default = true

        /// When set to true, the optimizer will insert filters before a join between
        /// a nullable and non-nullable column to filter out nulls on the nullable side. This
        /// filter can add additional overhead when the file format does not fully support
        /// predicate push down.
        pub filter_null_join_keys: bool, default = false

        /// Should DataFusion repartition data using the aggregate keys to execute aggregates
        /// in parallel using the provided `target_partitions` level"
        pub repartition_aggregations: bool, default = true

        /// Should DataFusion repartition data using the join keys to execute joins in parallel
        /// using the provided `target_partitions` level"
        pub repartition_joins: bool, default = true

        /// Should DataFusion repartition data using the partitions keys to execute window
        /// functions in parallel using the provided `target_partitions` level"
        pub repartition_windows: bool, default = true

        /// When set to true, the logical plan optimizer will produce warning
        /// messages if any optimization rules produce errors and then proceed to the next
        /// rule. When set to false, any rules that produce errors will cause the query to fail
        pub skip_failed_rules: bool, default = true

        /// Number of times that the optimizer will attempt to optimize the plan
        pub max_passes: usize, default = 3

        /// When set to true, the physical plan optimizer will run a top down
        /// process to reorder the join keys
        pub top_down_join_key_reordering: bool, default = true

        /// When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin.
        /// HashJoin can work more efficiently than SortMergeJoin but consumes more memory
        pub prefer_hash_join: bool, default = true

        /// The maximum estimated size in bytes for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold: usize, default = 1024 * 1024
    }
}

config_namespace! {
    /// Options controlling explain output
    pub struct ExplainOptions {
        /// When set to true, the explain statement will only print logical plans
        pub logical_plan_only: bool, default = false

        /// When set to true, the explain statement will only print physical plans
        pub physical_plan_only: bool, default = false
    }
}

/// A key value pair, with a corresponding description
#[derive(Debug)]
pub struct ConfigEntry {
    /// A unique string to identify this config value
    pub key: String,

    /// The value if any
    pub value: Option<String>,

    /// A description of this configuration entry
    pub description: &'static str,
}

/// Configuration options struct, able to store both built-in configuration and custom options
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ConfigOptions {
    /// Catalog options
    pub catalog: CatalogOptions,
    /// Execution options
    pub execution: ExecutionOptions,
    /// Explain options
    pub optimizer: OptimizerOptions,
    /// Explain options
    pub explain: ExplainOptions,
    /// Optional extensions registered using [`Extensions::insert`]
    pub extensions: Extensions,
}

impl ConfigField for ConfigOptions {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Extensions are handled in the public `ConfigOptions::set`
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "catalog" => self.catalog.set(rem, value),
            "execution" => self.execution.set(rem, value),
            "optimizer" => self.optimizer.set(rem, value),
            "explain" => self.explain.set(rem, value),
            _ => Err(DataFusionError::Internal(format!(
                "Config value \"{}\" not found on ConfigOptions",
                key
            ))),
        }
    }

    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        self.catalog.visit(v, "datafusion.catalog", "");
        self.execution.visit(v, "datafusion.execution", "");
        self.optimizer.visit(v, "datafusion.optimizer", "");
        self.explain.visit(v, "datafusion.explain", "");
    }
}

impl ConfigOptions {
    /// Creates a new [`ConfigOptions`] with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a configuration option
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (prefix, key) = key.split_once('.').ok_or_else(|| {
            DataFusionError::External(
                format!("could not find config namespace for key \"{key}\"",).into(),
            )
        })?;

        if prefix == "datafusion" {
            return ConfigField::set(self, key, value);
        }

        let e = self.extensions.0.get_mut(prefix);
        let e = e.ok_or_else(|| {
            DataFusionError::External(
                format!("Could not find config namespace \"{prefix}\"",).into(),
            )
        })?;
        e.0.set(key, value)
    }

    /// Create new ConfigOptions struct, taking values from
    /// environment variables where possible.
    ///
    /// For example, setting `DATAFUSION_EXECUTION_BATCH_SIZE` will
    /// control `datafusion.execution.batch_size`.
    pub fn from_env() -> Result<Self> {
        struct Visitor(Vec<String>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, _: V, _: &'static str) {
                self.0.push(key.to_string())
            }

            fn none(&mut self, key: &str, _: &'static str) {
                self.0.push(key.to_string())
            }
        }

        // Extract the names of all fields and then look up the corresponding
        // environment variables. This isn't hugely efficient but avoids
        // ambiguity between `a.b` and `a_b` which would both correspond
        // to an environment variable of `A_B`

        let mut keys = Visitor(vec![]);
        let mut ret = Self::default();
        ret.visit(&mut keys, "datafusion", "");

        for key in keys.0 {
            let env = key.to_uppercase().replace('.', "_");
            if let Some(var) = std::env::var_os(env) {
                ret.set(&key, var.to_string_lossy().as_ref())?;
            }
        }

        Ok(ret)
    }

    /// Returns the [`ConfigEntry`] stored within this [`ConfigOptions`]
    pub fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, "datafusion", "");

        v.0.extend(self.extensions.0.values().flat_map(|e| e.0.entries()));
        v.0
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;

        let mut s = Self::default();
        s.execution.target_partitions = 0; // Normalize for display

        let mut docs = "| key | default | description |\n".to_string();
        docs += "|-----|---------|-------------|\n";
        let mut entries = s.entries();
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        for entry in s.entries() {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} |",
                entry.key,
                entry.value.as_deref().unwrap_or("NULL"),
                entry.description
            );
        }
        docs
    }
}

/// [`ConfigExtension`] provides a mechanism to store third-party configuration within DataFusion
///
/// Unfortunately associated constants are not currently object-safe, and so this
/// extends the object-safe [`ExtensionOptions`]
pub trait ConfigExtension: ExtensionOptions {
    /// Configuration namespace prefix to use
    ///
    /// All values under this will be prefixed with `$PREFIX + "."`
    const PREFIX: &'static str;
}

/// An object-safe API for storing arbitrary configuration
pub trait ExtensionOptions: Send + Sync + std::fmt::Debug + 'static {
    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilised
    fn as_any(&self) -> &dyn Any;

    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilised
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Return a deep clone of this [`ExtensionOptions`]
    ///
    /// It is important this does not share mutable state to avoid consistency issues
    /// with configuration changing whilst queries are executing
    fn cloned(&self) -> Box<dyn ExtensionOptions>;

    /// Set the given `key`, `value` pair
    fn set(&mut self, key: &str, value: &str) -> Result<()>;

    /// Returns the [`ConfigEntry`] stored in this [`ExtensionOptions`]
    fn entries(&self) -> Vec<ConfigEntry>;
}

/// A type-safe container for [`ConfigExtension`]
#[derive(Debug, Default, Clone)]
pub struct Extensions(BTreeMap<&'static str, ExtensionBox>);

impl Extensions {
    /// Registers a [`ConfigExtension`] with this [`ConfigOptions`]
    pub fn insert<T: ConfigExtension>(&mut self, extension: T) {
        assert_ne!(T::PREFIX, "datafusion");
        let e = ExtensionBox(Box::new(extension));
        self.0.insert(T::PREFIX, e);
    }

    /// Retrieves the extension of the given type if any
    pub fn get<T: ConfigExtension>(&self) -> Option<&T> {
        self.0.get(T::PREFIX)?.0.as_any().downcast_ref()
    }

    /// Retrieves the extension of the given type if any
    pub fn get_mut<T: ConfigExtension>(&mut self) -> Option<&mut T> {
        let e = self.0.get_mut(T::PREFIX)?;
        e.0.as_any_mut().downcast_mut()
    }
}

#[derive(Debug)]
struct ExtensionBox(Box<dyn ExtensionOptions>);

impl Clone for ExtensionBox {
    fn clone(&self) -> Self {
        Self(self.0.cloned())
    }
}

/// A trait implemented by `config_namespace` and for field types that provides
/// the ability to walk and mutate the configuration tree
trait ConfigField {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str);

    fn set(&mut self, key: &str, value: &str) -> Result<()>;
}

impl<F: ConfigField + Default> ConfigField for Option<F> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(s) => s.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.get_or_insert_with(Default::default).set(key, value)
    }
}

macro_rules! config_field {
    ($t:ty) => {
        impl ConfigField for $t {
            fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
                v.some(key, self, description)
            }

            fn set(&mut self, _: &str, value: &str) -> Result<()> {
                *self = value.parse().map_err(|e| {
                    DataFusionError::Context(
                        format!(concat!("Error parsing {} as ", stringify!($t),), value),
                        Box::new(DataFusionError::External(Box::new(e))),
                    )
                })?;
                Ok(())
            }
        }
    };
}

config_field!(String);
config_field!(bool);
config_field!(usize);

/// An implementation trait used to recursively walk configuration
trait Visit {
    fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str);

    fn none(&mut self, key: &str, description: &'static str);
}
