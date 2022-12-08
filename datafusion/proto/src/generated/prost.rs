#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnRelation {
    #[prost(string, tag = "1")]
    pub relation: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Column {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub relation: ::core::option::Option<ColumnRelation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfField {
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
    #[prost(message, optional, tag = "2")]
    pub qualifier: ::core::option::Option<ColumnRelation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfSchema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<DfField>,
    #[prost(map = "string, string", tag = "2")]
    pub metadata: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// logical plan
/// LogicalPlan is a nested type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalPlanNode {
    #[prost(
        oneof = "logical_plan_node::LogicalPlanType",
        tags = "1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26"
    )]
    pub logical_plan_type: ::core::option::Option<logical_plan_node::LogicalPlanType>,
}
/// Nested message and enum types in `LogicalPlanNode`.
pub mod logical_plan_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum LogicalPlanType {
        #[prost(message, tag = "1")]
        ListingScan(super::ListingTableScanNode),
        #[prost(message, tag = "3")]
        Projection(::prost::alloc::boxed::Box<super::ProjectionNode>),
        #[prost(message, tag = "4")]
        Selection(::prost::alloc::boxed::Box<super::SelectionNode>),
        #[prost(message, tag = "5")]
        Limit(::prost::alloc::boxed::Box<super::LimitNode>),
        #[prost(message, tag = "6")]
        Aggregate(::prost::alloc::boxed::Box<super::AggregateNode>),
        #[prost(message, tag = "7")]
        Join(::prost::alloc::boxed::Box<super::JoinNode>),
        #[prost(message, tag = "8")]
        Sort(::prost::alloc::boxed::Box<super::SortNode>),
        #[prost(message, tag = "9")]
        Repartition(::prost::alloc::boxed::Box<super::RepartitionNode>),
        #[prost(message, tag = "10")]
        EmptyRelation(super::EmptyRelationNode),
        #[prost(message, tag = "11")]
        CreateExternalTable(super::CreateExternalTableNode),
        #[prost(message, tag = "12")]
        Explain(::prost::alloc::boxed::Box<super::ExplainNode>),
        #[prost(message, tag = "13")]
        Window(::prost::alloc::boxed::Box<super::WindowNode>),
        #[prost(message, tag = "14")]
        Analyze(::prost::alloc::boxed::Box<super::AnalyzeNode>),
        #[prost(message, tag = "15")]
        CrossJoin(::prost::alloc::boxed::Box<super::CrossJoinNode>),
        #[prost(message, tag = "16")]
        Values(super::ValuesNode),
        #[prost(message, tag = "17")]
        Extension(super::LogicalExtensionNode),
        #[prost(message, tag = "18")]
        CreateCatalogSchema(super::CreateCatalogSchemaNode),
        #[prost(message, tag = "19")]
        Union(super::UnionNode),
        #[prost(message, tag = "20")]
        CreateCatalog(super::CreateCatalogNode),
        #[prost(message, tag = "21")]
        SubqueryAlias(::prost::alloc::boxed::Box<super::SubqueryAliasNode>),
        #[prost(message, tag = "22")]
        CreateView(::prost::alloc::boxed::Box<super::CreateViewNode>),
        #[prost(message, tag = "23")]
        Distinct(::prost::alloc::boxed::Box<super::DistinctNode>),
        #[prost(message, tag = "24")]
        ViewScan(::prost::alloc::boxed::Box<super::ViewTableScanNode>),
        #[prost(message, tag = "25")]
        CustomScan(super::CustomTableScanNode),
        #[prost(message, tag = "26")]
        Prepare(::prost::alloc::boxed::Box<super::PrepareNode>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExtensionNode {
    #[prost(bytes = "vec", tag = "1")]
    pub node: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub inputs: ::prost::alloc::vec::Vec<LogicalPlanNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionColumns {
    #[prost(string, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CsvFormat {
    #[prost(bool, tag = "1")]
    pub has_header: bool,
    #[prost(string, tag = "2")]
    pub delimiter: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetFormat {
    #[prost(bool, tag = "1")]
    pub enable_pruning: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvroFormat {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListingTableScanNode {
    #[prost(string, tag = "1")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "3")]
    pub file_extension: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag = "5")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "6")]
    pub filters: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(string, repeated, tag = "7")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "8")]
    pub collect_stat: bool,
    #[prost(uint32, tag = "9")]
    pub target_partitions: u32,
    #[prost(message, repeated, tag = "13")]
    pub file_sort_order: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(oneof = "listing_table_scan_node::FileFormatType", tags = "10, 11, 12")]
    pub file_format_type: ::core::option::Option<
        listing_table_scan_node::FileFormatType,
    >,
}
/// Nested message and enum types in `ListingTableScanNode`.
pub mod listing_table_scan_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FileFormatType {
        #[prost(message, tag = "10")]
        Csv(super::CsvFormat),
        #[prost(message, tag = "11")]
        Parquet(super::ParquetFormat),
        #[prost(message, tag = "12")]
        Avro(super::AvroFormat),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ViewTableScanNode {
    #[prost(string, tag = "1")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag = "2")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, optional, tag = "4")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(string, tag = "5")]
    pub definition: ::prost::alloc::string::String,
}
/// Logical Plan to Scan a CustomTableProvider registered at runtime
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CustomTableScanNode {
    #[prost(string, tag = "1")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "4")]
    pub filters: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bytes = "vec", tag = "5")]
    pub custom_table_data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(oneof = "projection_node::OptionalAlias", tags = "3")]
    pub optional_alias: ::core::option::Option<projection_node::OptionalAlias>,
}
/// Nested message and enum types in `ProjectionNode`.
pub mod projection_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OptionalAlias {
        #[prost(string, tag = "3")]
        Alias(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::core::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    /// Maximum number of highest/lowest rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepartitionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(oneof = "repartition_node::PartitionMethod", tags = "2, 3")]
    pub partition_method: ::core::option::Option<repartition_node::PartitionMethod>,
}
/// Nested message and enum types in `RepartitionNode`.
pub mod repartition_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PartitionMethod {
        #[prost(uint64, tag = "2")]
        RoundRobin(u64),
        #[prost(message, tag = "3")]
        Hash(super::HashRepartition),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashRepartition {
    #[prost(message, repeated, tag = "1")]
    pub hash_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(uint64, tag = "2")]
    pub partition_count: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyRelationNode {
    #[prost(bool, tag = "1")]
    pub produce_one_row: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExternalTableNode {
    #[prost(message, optional, tag = "12")]
    pub name: ::core::option::Option<OwnedTableReference>,
    #[prost(string, tag = "2")]
    pub location: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub file_type: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub has_header: bool,
    #[prost(message, optional, tag = "5")]
    pub schema: ::core::option::Option<DfSchema>,
    #[prost(string, repeated, tag = "6")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "7")]
    pub if_not_exists: bool,
    #[prost(string, tag = "8")]
    pub delimiter: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub definition: ::prost::alloc::string::String,
    #[prost(string, tag = "10")]
    pub file_compression_type: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "11")]
    pub options: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub data_types: ::prost::alloc::vec::Vec<ArrowType>,
    #[prost(message, optional, boxed, tag = "3")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogSchemaNode {
    #[prost(string, tag = "1")]
    pub schema_name: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<DfSchema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogNode {
    #[prost(string, tag = "1")]
    pub catalog_name: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<DfSchema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateViewNode {
    #[prost(message, optional, tag = "5")]
    pub name: ::core::option::Option<OwnedTableReference>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "3")]
    pub or_replace: bool,
    #[prost(string, tag = "4")]
    pub definition: ::prost::alloc::string::String,
}
/// a node containing data for defining values list. unlike in SQL where it's two dimensional, here
/// the list is flattened, and with the field n_cols it can be parsed and partitioned into rows
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValuesNode {
    #[prost(uint64, tag = "1")]
    pub n_cols: u64,
    #[prost(message, repeated, tag = "2")]
    pub values_list: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "2")]
    pub verbose: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "2")]
    pub verbose: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub group_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag = "3")]
    pub aggr_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub window_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(enumeration = "JoinType", tag = "3")]
    pub join_type: i32,
    #[prost(enumeration = "JoinConstraint", tag = "4")]
    pub join_constraint: i32,
    #[prost(message, repeated, tag = "5")]
    pub left_join_column: ::prost::alloc::vec::Vec<Column>,
    #[prost(message, repeated, tag = "6")]
    pub right_join_column: ::prost::alloc::vec::Vec<Column>,
    #[prost(bool, tag = "7")]
    pub null_equals_null: bool,
    #[prost(message, optional, tag = "8")]
    pub filter: ::core::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistinctNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnionNode {
    #[prost(message, repeated, tag = "1")]
    pub inputs: ::prost::alloc::vec::Vec<LogicalPlanNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrossJoinNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LimitNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    /// The number of rows to skip before fetch; non-positive means don't skip any
    #[prost(int64, tag = "2")]
    pub skip: i64,
    /// Maximum number of rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionExecNode {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubqueryAliasNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
/// logical expressions
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprNode {
    #[prost(
        oneof = "logical_expr_node::ExprType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34"
    )]
    pub expr_type: ::core::option::Option<logical_expr_node::ExprType>,
}
/// Nested message and enum types in `LogicalExprNode`.
pub mod logical_expr_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprType {
        /// column references
        #[prost(message, tag = "1")]
        Column(super::Column),
        /// alias
        #[prost(message, tag = "2")]
        Alias(::prost::alloc::boxed::Box<super::AliasNode>),
        #[prost(message, tag = "3")]
        Literal(super::ScalarValue),
        /// binary expressions
        #[prost(message, tag = "4")]
        BinaryExpr(super::BinaryExprNode),
        /// aggregate expressions
        #[prost(message, tag = "5")]
        AggregateExpr(::prost::alloc::boxed::Box<super::AggregateExprNode>),
        /// null checks
        #[prost(message, tag = "6")]
        IsNullExpr(::prost::alloc::boxed::Box<super::IsNull>),
        #[prost(message, tag = "7")]
        IsNotNullExpr(::prost::alloc::boxed::Box<super::IsNotNull>),
        #[prost(message, tag = "8")]
        NotExpr(::prost::alloc::boxed::Box<super::Not>),
        #[prost(message, tag = "9")]
        Between(::prost::alloc::boxed::Box<super::BetweenNode>),
        #[prost(message, tag = "10")]
        Case(::prost::alloc::boxed::Box<super::CaseNode>),
        #[prost(message, tag = "11")]
        Cast(::prost::alloc::boxed::Box<super::CastNode>),
        #[prost(message, tag = "12")]
        Sort(::prost::alloc::boxed::Box<super::SortExprNode>),
        #[prost(message, tag = "13")]
        Negative(::prost::alloc::boxed::Box<super::NegativeNode>),
        #[prost(message, tag = "14")]
        InList(::prost::alloc::boxed::Box<super::InListNode>),
        #[prost(bool, tag = "15")]
        Wildcard(bool),
        #[prost(message, tag = "16")]
        ScalarFunction(super::ScalarFunctionNode),
        #[prost(message, tag = "17")]
        TryCast(::prost::alloc::boxed::Box<super::TryCastNode>),
        /// window expressions
        #[prost(message, tag = "18")]
        WindowExpr(::prost::alloc::boxed::Box<super::WindowExprNode>),
        /// AggregateUDF expressions
        #[prost(message, tag = "19")]
        AggregateUdfExpr(::prost::alloc::boxed::Box<super::AggregateUdfExprNode>),
        /// Scalar UDF expressions
        #[prost(message, tag = "20")]
        ScalarUdfExpr(super::ScalarUdfExprNode),
        #[prost(message, tag = "21")]
        GetIndexedField(::prost::alloc::boxed::Box<super::GetIndexedField>),
        #[prost(message, tag = "22")]
        GroupingSet(super::GroupingSetNode),
        #[prost(message, tag = "23")]
        Cube(super::CubeNode),
        #[prost(message, tag = "24")]
        Rollup(super::RollupNode),
        #[prost(message, tag = "25")]
        IsTrue(::prost::alloc::boxed::Box<super::IsTrue>),
        #[prost(message, tag = "26")]
        IsFalse(::prost::alloc::boxed::Box<super::IsFalse>),
        #[prost(message, tag = "27")]
        IsUnknown(::prost::alloc::boxed::Box<super::IsUnknown>),
        #[prost(message, tag = "28")]
        IsNotTrue(::prost::alloc::boxed::Box<super::IsNotTrue>),
        #[prost(message, tag = "29")]
        IsNotFalse(::prost::alloc::boxed::Box<super::IsNotFalse>),
        #[prost(message, tag = "30")]
        IsNotUnknown(::prost::alloc::boxed::Box<super::IsNotUnknown>),
        #[prost(message, tag = "31")]
        Like(::prost::alloc::boxed::Box<super::LikeNode>),
        #[prost(message, tag = "32")]
        Ilike(::prost::alloc::boxed::Box<super::ILikeNode>),
        #[prost(message, tag = "33")]
        SimilarTo(::prost::alloc::boxed::Box<super::SimilarToNode>),
        #[prost(message, tag = "34")]
        Placeholder(super::PlaceholderNode),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PlaceholderNode {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub data_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprList {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupingSetNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprList>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CubeNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollupNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexedField {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub key: ::core::option::Option<ScalarValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsTrue {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsFalse {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsUnknown {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotTrue {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotFalse {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotUnknown {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Not {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryExprNode {
    /// Represents the operands from the left inner most expression
    /// to the right outer most expression where each of them are chained
    /// with the operator 'op'.
    #[prost(message, repeated, tag = "1")]
    pub operands: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NegativeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InListNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub list: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag = "3")]
    pub negated: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarFunctionNode {
    #[prost(enumeration = "ScalarFunction", tag = "1")]
    pub fun: i32,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateExprNode {
    #[prost(enumeration = "AggregateFunction", tag = "1")]
    pub aggr_function: i32,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag = "3")]
    pub distinct: bool,
    #[prost(message, optional, boxed, tag = "4")]
    pub filter: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateUdfExprNode {
    #[prost(string, tag = "1")]
    pub fun_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, optional, boxed, tag = "3")]
    pub filter: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarUdfExprNode {
    #[prost(string, tag = "1")]
    pub fun_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowExprNode {
    #[prost(message, optional, boxed, tag = "4")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "5")]
    pub partition_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag = "6")]
    pub order_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
    /// repeated LogicalExprNode filter = 7;
    #[prost(message, optional, tag = "8")]
    pub window_frame: ::core::option::Option<WindowFrame>,
    #[prost(oneof = "window_expr_node::WindowFunction", tags = "1, 2")]
    pub window_function: ::core::option::Option<window_expr_node::WindowFunction>,
}
/// Nested message and enum types in `WindowExprNode`.
pub mod window_expr_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WindowFunction {
        #[prost(enumeration = "super::AggregateFunction", tag = "1")]
        AggrFunction(i32),
        /// udaf = 3
        #[prost(enumeration = "super::BuiltInWindowFunction", tag = "2")]
        BuiltInFunction(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BetweenNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "3")]
    pub low: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub high: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LikeNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ILikeNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimilarToNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaseNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub when_then_expr: ::prost::alloc::vec::Vec<WhenThen>,
    #[prost(message, optional, boxed, tag = "3")]
    pub else_expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhenThen {
    #[prost(message, optional, tag = "1")]
    pub when_expr: ::core::option::Option<LogicalExprNode>,
    #[prost(message, optional, tag = "2")]
    pub then_expr: ::core::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TryCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub asc: bool,
    #[prost(bool, tag = "3")]
    pub nulls_first: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrame {
    #[prost(enumeration = "WindowFrameUnits", tag = "1")]
    pub window_frame_units: i32,
    #[prost(message, optional, tag = "2")]
    pub start_bound: ::core::option::Option<WindowFrameBound>,
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[prost(oneof = "window_frame::EndBound", tags = "3")]
    pub end_bound: ::core::option::Option<window_frame::EndBound>,
}
/// Nested message and enum types in `WindowFrame`.
pub mod window_frame {
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum EndBound {
        #[prost(message, tag = "3")]
        Bound(super::WindowFrameBound),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrameBound {
    #[prost(enumeration = "WindowFrameBoundType", tag = "1")]
    pub window_frame_bound_type: i32,
    #[prost(message, optional, tag = "2")]
    pub bound_value: ::core::option::Option<ScalarValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    /// name of the field
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag = "2")]
    pub arrow_type: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(bool, tag = "3")]
    pub nullable: bool,
    /// for complex data types like structs, unions
    #[prost(message, repeated, tag = "4")]
    pub children: ::prost::alloc::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeBinary {
    #[prost(int32, tag = "1")]
    pub length: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(enumeration = "TimeUnit", tag = "1")]
    pub time_unit: i32,
    #[prost(string, tag = "2")]
    pub timezone: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal {
    #[prost(uint32, tag = "3")]
    pub precision: u32,
    #[prost(int32, tag = "4")]
    pub scale: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct List {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeList {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
    #[prost(int32, tag = "2")]
    pub list_size: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dictionary {
    #[prost(message, optional, boxed, tag = "1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Struct {
    #[prost(message, repeated, tag = "1")]
    pub sub_field_types: ::prost::alloc::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Union {
    #[prost(message, repeated, tag = "1")]
    pub union_types: ::prost::alloc::vec::Vec<Field>,
    #[prost(enumeration = "UnionMode", tag = "2")]
    pub union_mode: i32,
    #[prost(int32, repeated, tag = "3")]
    pub type_ids: ::prost::alloc::vec::Vec<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarListValue {
    /// encode null explicitly to distinguish a list with a null value
    /// from a list with no values)
    #[prost(bool, tag = "3")]
    pub is_null: bool,
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
    #[prost(message, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<ScalarValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime32Value {
    #[prost(oneof = "scalar_time32_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time32_value::Value>,
}
/// Nested message and enum types in `ScalarTime32Value`.
pub mod scalar_time32_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int32, tag = "1")]
        Time32SecondValue(i32),
        #[prost(int32, tag = "2")]
        Time32MillisecondValue(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime64Value {
    #[prost(oneof = "scalar_time64_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time64_value::Value>,
}
/// Nested message and enum types in `ScalarTime64Value`.
pub mod scalar_time64_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        Time64MicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        Time64NanosecondValue(i64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTimestampValue {
    #[prost(string, tag = "5")]
    pub timezone: ::prost::alloc::string::String,
    #[prost(oneof = "scalar_timestamp_value::Value", tags = "1, 2, 3, 4")]
    pub value: ::core::option::Option<scalar_timestamp_value::Value>,
}
/// Nested message and enum types in `ScalarTimestampValue`.
pub mod scalar_timestamp_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        TimeMicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        TimeNanosecondValue(i64),
        #[prost(int64, tag = "3")]
        TimeSecondValue(i64),
        #[prost(int64, tag = "4")]
        TimeMillisecondValue(i64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarDictionaryValue {
    #[prost(message, optional, tag = "1")]
    pub index_type: ::core::option::Option<ArrowType>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ScalarValue>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntervalMonthDayNanoValue {
    #[prost(int32, tag = "1")]
    pub months: i32,
    #[prost(int32, tag = "2")]
    pub days: i32,
    #[prost(int64, tag = "3")]
    pub nanos: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructValue {
    /// Note that a null struct value must have one or more fields, so we
    /// encode a null StructValue as one witth an empty field_values
    /// list.
    #[prost(message, repeated, tag = "2")]
    pub field_values: ::prost::alloc::vec::Vec<ScalarValue>,
    #[prost(message, repeated, tag = "3")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarFixedSizeBinary {
    #[prost(bytes = "vec", tag = "1")]
    pub values: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "2")]
    pub length: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarValue {
    #[prost(
        oneof = "scalar_value::Value",
        tags = "33, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34"
    )]
    pub value: ::core::option::Option<scalar_value::Value>,
}
/// Nested message and enum types in `ScalarValue`.
pub mod scalar_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        /// was PrimitiveScalarType null_value = 19;
        /// Null value of any type
        #[prost(message, tag = "33")]
        NullValue(super::ArrowType),
        #[prost(bool, tag = "1")]
        BoolValue(bool),
        #[prost(string, tag = "2")]
        Utf8Value(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        LargeUtf8Value(::prost::alloc::string::String),
        #[prost(int32, tag = "4")]
        Int8Value(i32),
        #[prost(int32, tag = "5")]
        Int16Value(i32),
        #[prost(int32, tag = "6")]
        Int32Value(i32),
        #[prost(int64, tag = "7")]
        Int64Value(i64),
        #[prost(uint32, tag = "8")]
        Uint8Value(u32),
        #[prost(uint32, tag = "9")]
        Uint16Value(u32),
        #[prost(uint32, tag = "10")]
        Uint32Value(u32),
        #[prost(uint64, tag = "11")]
        Uint64Value(u64),
        #[prost(float, tag = "12")]
        Float32Value(f32),
        #[prost(double, tag = "13")]
        Float64Value(f64),
        /// Literal Date32 value always has a unit of day
        #[prost(int32, tag = "14")]
        Date32Value(i32),
        #[prost(message, tag = "15")]
        Time32Value(super::ScalarTime32Value),
        /// WAS: ScalarType null_list_value = 18;
        #[prost(message, tag = "17")]
        ListValue(super::ScalarListValue),
        #[prost(message, tag = "20")]
        Decimal128Value(super::Decimal128),
        #[prost(int64, tag = "21")]
        Date64Value(i64),
        #[prost(int32, tag = "24")]
        IntervalYearmonthValue(i32),
        #[prost(int64, tag = "25")]
        IntervalDaytimeValue(i64),
        #[prost(message, tag = "26")]
        TimestampValue(super::ScalarTimestampValue),
        #[prost(message, tag = "27")]
        DictionaryValue(::prost::alloc::boxed::Box<super::ScalarDictionaryValue>),
        #[prost(bytes, tag = "28")]
        BinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "29")]
        LargeBinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(message, tag = "30")]
        Time64Value(super::ScalarTime64Value),
        #[prost(message, tag = "31")]
        IntervalMonthDayNano(super::IntervalMonthDayNanoValue),
        #[prost(message, tag = "32")]
        StructValue(super::StructValue),
        #[prost(message, tag = "34")]
        FixedSizeBinaryValue(super::ScalarFixedSizeBinary),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal128 {
    #[prost(bytes = "vec", tag = "1")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(int64, tag = "2")]
    pub p: i64,
    #[prost(int64, tag = "3")]
    pub s: i64,
}
/// Serialized data type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowType {
    #[prost(
        oneof = "arrow_type::ArrowTypeEnum",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 32, 15, 16, 31, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30"
    )]
    pub arrow_type_enum: ::core::option::Option<arrow_type::ArrowTypeEnum>,
}
/// Nested message and enum types in `ArrowType`.
pub mod arrow_type {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ArrowTypeEnum {
        /// arrow::Type::NA
        #[prost(message, tag = "1")]
        None(super::EmptyMessage),
        /// arrow::Type::BOOL
        #[prost(message, tag = "2")]
        Bool(super::EmptyMessage),
        /// arrow::Type::UINT8
        #[prost(message, tag = "3")]
        Uint8(super::EmptyMessage),
        /// arrow::Type::INT8
        #[prost(message, tag = "4")]
        Int8(super::EmptyMessage),
        /// represents arrow::Type fields in src/arrow/type.h
        #[prost(message, tag = "5")]
        Uint16(super::EmptyMessage),
        #[prost(message, tag = "6")]
        Int16(super::EmptyMessage),
        #[prost(message, tag = "7")]
        Uint32(super::EmptyMessage),
        #[prost(message, tag = "8")]
        Int32(super::EmptyMessage),
        #[prost(message, tag = "9")]
        Uint64(super::EmptyMessage),
        #[prost(message, tag = "10")]
        Int64(super::EmptyMessage),
        #[prost(message, tag = "11")]
        Float16(super::EmptyMessage),
        #[prost(message, tag = "12")]
        Float32(super::EmptyMessage),
        #[prost(message, tag = "13")]
        Float64(super::EmptyMessage),
        #[prost(message, tag = "14")]
        Utf8(super::EmptyMessage),
        #[prost(message, tag = "32")]
        LargeUtf8(super::EmptyMessage),
        #[prost(message, tag = "15")]
        Binary(super::EmptyMessage),
        #[prost(int32, tag = "16")]
        FixedSizeBinary(i32),
        #[prost(message, tag = "31")]
        LargeBinary(super::EmptyMessage),
        #[prost(message, tag = "17")]
        Date32(super::EmptyMessage),
        #[prost(message, tag = "18")]
        Date64(super::EmptyMessage),
        #[prost(enumeration = "super::TimeUnit", tag = "19")]
        Duration(i32),
        #[prost(message, tag = "20")]
        Timestamp(super::Timestamp),
        #[prost(enumeration = "super::TimeUnit", tag = "21")]
        Time32(i32),
        #[prost(enumeration = "super::TimeUnit", tag = "22")]
        Time64(i32),
        #[prost(enumeration = "super::IntervalUnit", tag = "23")]
        Interval(i32),
        #[prost(message, tag = "24")]
        Decimal(super::Decimal),
        #[prost(message, tag = "25")]
        List(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "26")]
        LargeList(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "27")]
        FixedSizeList(::prost::alloc::boxed::Box<super::FixedSizeList>),
        #[prost(message, tag = "28")]
        Struct(super::Struct),
        #[prost(message, tag = "29")]
        Union(super::Union),
        #[prost(message, tag = "30")]
        Dictionary(::prost::alloc::boxed::Box<super::Dictionary>),
    }
}
/// Useful for representing an empty enum variant in rust
/// E.G. enum example{One, Two(i32)}
/// maps to
/// message example{
///     oneof{
///         EmptyMessage One = 1;
///         i32 Two = 2;
///    }
/// }
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyMessage {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizedLogicalPlanType {
    #[prost(string, tag = "1")]
    pub optimizer_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizedPhysicalPlanType {
    #[prost(string, tag = "1")]
    pub optimizer_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PlanType {
    #[prost(oneof = "plan_type::PlanTypeEnum", tags = "1, 2, 3, 4, 5, 6")]
    pub plan_type_enum: ::core::option::Option<plan_type::PlanTypeEnum>,
}
/// Nested message and enum types in `PlanType`.
pub mod plan_type {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PlanTypeEnum {
        #[prost(message, tag = "1")]
        InitialLogicalPlan(super::EmptyMessage),
        #[prost(message, tag = "2")]
        OptimizedLogicalPlan(super::OptimizedLogicalPlanType),
        #[prost(message, tag = "3")]
        FinalLogicalPlan(super::EmptyMessage),
        #[prost(message, tag = "4")]
        InitialPhysicalPlan(super::EmptyMessage),
        #[prost(message, tag = "5")]
        OptimizedPhysicalPlan(super::OptimizedPhysicalPlanType),
        #[prost(message, tag = "6")]
        FinalPhysicalPlan(super::EmptyMessage),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringifiedPlan {
    #[prost(message, optional, tag = "1")]
    pub plan_type: ::core::option::Option<PlanType>,
    #[prost(string, tag = "2")]
    pub plan: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BareTableReference {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartialTableReference {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FullTableReference {
    #[prost(string, tag = "1")]
    pub catalog: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OwnedTableReference {
    #[prost(oneof = "owned_table_reference::TableReferenceEnum", tags = "1, 2, 3")]
    pub table_reference_enum: ::core::option::Option<
        owned_table_reference::TableReferenceEnum,
    >,
}
/// Nested message and enum types in `OwnedTableReference`.
pub mod owned_table_reference {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TableReferenceEnum {
        #[prost(message, tag = "1")]
        Bare(super::BareTableReference),
        #[prost(message, tag = "2")]
        Partial(super::PartialTableReference),
        #[prost(message, tag = "3")]
        Full(super::FullTableReference),
    }
}
/// PhysicalPlanNode is a nested type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalPlanNode {
    #[prost(
        oneof = "physical_plan_node::PhysicalPlanType",
        tags = "1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21"
    )]
    pub physical_plan_type: ::core::option::Option<physical_plan_node::PhysicalPlanType>,
}
/// Nested message and enum types in `PhysicalPlanNode`.
pub mod physical_plan_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PhysicalPlanType {
        #[prost(message, tag = "1")]
        ParquetScan(super::ParquetScanExecNode),
        #[prost(message, tag = "2")]
        CsvScan(super::CsvScanExecNode),
        #[prost(message, tag = "3")]
        Empty(super::EmptyExecNode),
        #[prost(message, tag = "4")]
        Projection(::prost::alloc::boxed::Box<super::ProjectionExecNode>),
        #[prost(message, tag = "6")]
        GlobalLimit(::prost::alloc::boxed::Box<super::GlobalLimitExecNode>),
        #[prost(message, tag = "7")]
        LocalLimit(::prost::alloc::boxed::Box<super::LocalLimitExecNode>),
        #[prost(message, tag = "8")]
        Aggregate(::prost::alloc::boxed::Box<super::AggregateExecNode>),
        #[prost(message, tag = "9")]
        HashJoin(::prost::alloc::boxed::Box<super::HashJoinExecNode>),
        #[prost(message, tag = "10")]
        Sort(::prost::alloc::boxed::Box<super::SortExecNode>),
        #[prost(message, tag = "11")]
        CoalesceBatches(::prost::alloc::boxed::Box<super::CoalesceBatchesExecNode>),
        #[prost(message, tag = "12")]
        Filter(::prost::alloc::boxed::Box<super::FilterExecNode>),
        #[prost(message, tag = "13")]
        Merge(::prost::alloc::boxed::Box<super::CoalescePartitionsExecNode>),
        #[prost(message, tag = "14")]
        Repartition(::prost::alloc::boxed::Box<super::RepartitionExecNode>),
        #[prost(message, tag = "15")]
        Window(::prost::alloc::boxed::Box<super::WindowAggExecNode>),
        #[prost(message, tag = "16")]
        CrossJoin(::prost::alloc::boxed::Box<super::CrossJoinExecNode>),
        #[prost(message, tag = "17")]
        AvroScan(super::AvroScanExecNode),
        #[prost(message, tag = "18")]
        Extension(super::PhysicalExtensionNode),
        #[prost(message, tag = "19")]
        Union(super::UnionExecNode),
        #[prost(message, tag = "20")]
        Explain(super::ExplainExecNode),
        #[prost(message, tag = "21")]
        SortPreservingMerge(
            ::prost::alloc::boxed::Box<super::SortPreservingMergeExecNode>,
        ),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalExtensionNode {
    #[prost(bytes = "vec", tag = "1")]
    pub node: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub inputs: ::prost::alloc::vec::Vec<PhysicalPlanNode>,
}
/// physical expressions
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalExprNode {
    #[prost(
        oneof = "physical_expr_node::ExprType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17"
    )]
    pub expr_type: ::core::option::Option<physical_expr_node::ExprType>,
}
/// Nested message and enum types in `PhysicalExprNode`.
pub mod physical_expr_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprType {
        /// column references
        #[prost(message, tag = "1")]
        Column(super::PhysicalColumn),
        #[prost(message, tag = "2")]
        Literal(super::ScalarValue),
        /// binary expressions
        #[prost(message, tag = "3")]
        BinaryExpr(::prost::alloc::boxed::Box<super::PhysicalBinaryExprNode>),
        /// aggregate expressions
        #[prost(message, tag = "4")]
        AggregateExpr(super::PhysicalAggregateExprNode),
        /// null checks
        #[prost(message, tag = "5")]
        IsNullExpr(::prost::alloc::boxed::Box<super::PhysicalIsNull>),
        #[prost(message, tag = "6")]
        IsNotNullExpr(::prost::alloc::boxed::Box<super::PhysicalIsNotNull>),
        #[prost(message, tag = "7")]
        NotExpr(::prost::alloc::boxed::Box<super::PhysicalNot>),
        #[prost(message, tag = "8")]
        Case(::prost::alloc::boxed::Box<super::PhysicalCaseNode>),
        #[prost(message, tag = "9")]
        Cast(::prost::alloc::boxed::Box<super::PhysicalCastNode>),
        #[prost(message, tag = "10")]
        Sort(::prost::alloc::boxed::Box<super::PhysicalSortExprNode>),
        #[prost(message, tag = "11")]
        Negative(::prost::alloc::boxed::Box<super::PhysicalNegativeNode>),
        #[prost(message, tag = "12")]
        InList(::prost::alloc::boxed::Box<super::PhysicalInListNode>),
        #[prost(message, tag = "13")]
        ScalarFunction(super::PhysicalScalarFunctionNode),
        #[prost(message, tag = "14")]
        TryCast(::prost::alloc::boxed::Box<super::PhysicalTryCastNode>),
        /// window expressions
        #[prost(message, tag = "15")]
        WindowExpr(::prost::alloc::boxed::Box<super::PhysicalWindowExprNode>),
        #[prost(message, tag = "16")]
        ScalarUdf(super::PhysicalScalarUdfNode),
        #[prost(message, tag = "17")]
        DateTimeIntervalExpr(
            ::prost::alloc::boxed::Box<super::PhysicalDateTimeIntervalExprNode>,
        ),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalScalarUdfNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, optional, tag = "4")]
    pub return_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalAggregateExprNode {
    #[prost(enumeration = "AggregateFunction", tag = "1")]
    pub aggr_function: i32,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, tag = "3")]
    pub distinct: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalWindowExprNode {
    #[prost(message, optional, boxed, tag = "4")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(oneof = "physical_window_expr_node::WindowFunction", tags = "1, 2")]
    pub window_function: ::core::option::Option<
        physical_window_expr_node::WindowFunction,
    >,
}
/// Nested message and enum types in `PhysicalWindowExprNode`.
pub mod physical_window_expr_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WindowFunction {
        #[prost(enumeration = "super::AggregateFunction", tag = "1")]
        AggrFunction(i32),
        /// udaf = 3
        #[prost(enumeration = "super::BuiltInWindowFunction", tag = "2")]
        BuiltInFunction(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalIsNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalIsNotNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalNot {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalAliasNode {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<PhysicalExprNode>,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalBinaryExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub l: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub r: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalDateTimeIntervalExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub l: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub r: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalSortExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub asc: bool,
    #[prost(bool, tag = "3")]
    pub nulls_first: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalWhenThen {
    #[prost(message, optional, tag = "1")]
    pub when_expr: ::core::option::Option<PhysicalExprNode>,
    #[prost(message, optional, tag = "2")]
    pub then_expr: ::core::option::Option<PhysicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalInListNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub list: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, tag = "3")]
    pub negated: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalCaseNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub when_then_expr: ::prost::alloc::vec::Vec<PhysicalWhenThen>,
    #[prost(message, optional, boxed, tag = "3")]
    pub else_expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalScalarFunctionNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "ScalarFunction", tag = "2")]
    pub fun: i32,
    #[prost(message, repeated, tag = "3")]
    pub args: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, optional, tag = "4")]
    pub return_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalTryCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalNegativeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::core::option::Option<PhysicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileGroup {
    #[prost(message, repeated, tag = "1")]
    pub files: ::prost::alloc::vec::Vec<PartitionedFile>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScanLimit {
    /// wrap into a message to make it optional
    #[prost(uint32, tag = "1")]
    pub limit: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileScanExecConf {
    #[prost(message, repeated, tag = "1")]
    pub file_groups: ::prost::alloc::vec::Vec<FileGroup>,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(uint32, repeated, tag = "4")]
    pub projection: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, tag = "5")]
    pub limit: ::core::option::Option<ScanLimit>,
    #[prost(message, optional, tag = "6")]
    pub statistics: ::core::option::Option<Statistics>,
    #[prost(string, repeated, tag = "7")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "8")]
    pub object_store_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
    #[prost(message, optional, tag = "2")]
    pub pruning_predicate: ::core::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CsvScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
    #[prost(bool, tag = "2")]
    pub has_header: bool,
    #[prost(string, tag = "3")]
    pub delimiter: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvroScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashJoinExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "3")]
    pub on: ::prost::alloc::vec::Vec<JoinOn>,
    #[prost(enumeration = "JoinType", tag = "4")]
    pub join_type: i32,
    #[prost(enumeration = "PartitionMode", tag = "6")]
    pub partition_mode: i32,
    #[prost(bool, tag = "7")]
    pub null_equals_null: bool,
    #[prost(message, optional, tag = "8")]
    pub filter: ::core::option::Option<JoinFilter>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnionExecNode {
    #[prost(message, repeated, tag = "1")]
    pub inputs: ::prost::alloc::vec::Vec<PhysicalPlanNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainExecNode {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "2")]
    pub stringified_plans: ::prost::alloc::vec::Vec<StringifiedPlan>,
    #[prost(bool, tag = "3")]
    pub verbose: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrossJoinExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalColumn {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub index: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinOn {
    #[prost(message, optional, tag = "1")]
    pub left: ::core::option::Option<PhysicalColumn>,
    #[prost(message, optional, tag = "2")]
    pub right: ::core::option::Option<PhysicalColumn>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyExecNode {
    #[prost(bool, tag = "1")]
    pub produce_one_row: bool,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<Schema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(string, repeated, tag = "3")]
    pub expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowAggExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub window_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(string, repeated, tag = "3")]
    pub window_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub input_schema: ::core::option::Option<Schema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateExecNode {
    #[prost(message, repeated, tag = "1")]
    pub group_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, repeated, tag = "2")]
    pub aggr_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(enumeration = "AggregateMode", tag = "3")]
    pub mode: i32,
    #[prost(message, optional, boxed, tag = "4")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(string, repeated, tag = "5")]
    pub group_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "6")]
    pub aggr_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// we need the input schema to the partial aggregate to pass to the final aggregate
    #[prost(message, optional, tag = "7")]
    pub input_schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "8")]
    pub null_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, repeated, tag = "9")]
    pub groups: ::prost::alloc::vec::Vec<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GlobalLimitExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    /// The number of rows to skip before fetch
    #[prost(uint32, tag = "2")]
    pub skip: u32,
    /// Maximum number of rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalLimitExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(uint32, tag = "2")]
    pub fetch: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    /// Maximum number of highest/lowest rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortPreservingMergeExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoalesceBatchesExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(uint32, tag = "2")]
    pub target_batch_size: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoalescePartitionsExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalHashRepartition {
    #[prost(message, repeated, tag = "1")]
    pub hash_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(uint64, tag = "2")]
    pub partition_count: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepartitionExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(oneof = "repartition_exec_node::PartitionMethod", tags = "2, 3, 4")]
    pub partition_method: ::core::option::Option<repartition_exec_node::PartitionMethod>,
}
/// Nested message and enum types in `RepartitionExecNode`.
pub mod repartition_exec_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PartitionMethod {
        #[prost(uint64, tag = "2")]
        RoundRobin(u64),
        #[prost(message, tag = "3")]
        Hash(super::PhysicalHashRepartition),
        #[prost(uint64, tag = "4")]
        Unknown(u64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinFilter {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<PhysicalExprNode>,
    #[prost(message, repeated, tag = "2")]
    pub column_indices: ::prost::alloc::vec::Vec<ColumnIndex>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnIndex {
    #[prost(uint32, tag = "1")]
    pub index: u32,
    #[prost(enumeration = "JoinSide", tag = "2")]
    pub side: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionedFile {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub size: u64,
    #[prost(uint64, tag = "3")]
    pub last_modified_ns: u64,
    #[prost(message, repeated, tag = "4")]
    pub partition_values: ::prost::alloc::vec::Vec<ScalarValue>,
    #[prost(message, optional, tag = "5")]
    pub range: ::core::option::Option<FileRange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileRange {
    #[prost(int64, tag = "1")]
    pub start: i64,
    #[prost(int64, tag = "2")]
    pub end: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStats {
    #[prost(int64, tag = "1")]
    pub num_rows: i64,
    #[prost(int64, tag = "2")]
    pub num_batches: i64,
    #[prost(int64, tag = "3")]
    pub num_bytes: i64,
    #[prost(message, repeated, tag = "4")]
    pub column_stats: ::prost::alloc::vec::Vec<ColumnStats>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Statistics {
    #[prost(int64, tag = "1")]
    pub num_rows: i64,
    #[prost(int64, tag = "2")]
    pub total_byte_size: i64,
    #[prost(message, repeated, tag = "3")]
    pub column_stats: ::prost::alloc::vec::Vec<ColumnStats>,
    #[prost(bool, tag = "4")]
    pub is_exact: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnStats {
    #[prost(message, optional, tag = "1")]
    pub min_value: ::core::option::Option<ScalarValue>,
    #[prost(message, optional, tag = "2")]
    pub max_value: ::core::option::Option<ScalarValue>,
    #[prost(uint32, tag = "3")]
    pub null_count: u32,
    #[prost(uint32, tag = "4")]
    pub distinct_count: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionLocation {
    /// partition_id of the map stage who produces the shuffle.
    #[prost(uint32, tag = "1")]
    pub map_partition_id: u32,
    /// partition_id of the shuffle, a composition of(job_id + map_stage_id + partition_id).
    #[prost(message, optional, tag = "2")]
    pub partition_id: ::core::option::Option<PartitionId>,
    #[prost(message, optional, tag = "3")]
    pub executor_meta: ::core::option::Option<ExecutorMetadata>,
    #[prost(message, optional, tag = "4")]
    pub partition_stats: ::core::option::Option<PartitionStats>,
    #[prost(string, tag = "5")]
    pub path: ::prost::alloc::string::String,
}
/// Unique identifier for a materialized partition of data
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionId {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(uint32, tag = "4")]
    pub partition_id: u32,
}
/// Used by scheduler
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorMetadata {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub port: u32,
    #[prost(uint32, tag = "4")]
    pub grpc_port: u32,
    #[prost(message, optional, tag = "5")]
    pub specification: ::core::option::Option<ExecutorSpecification>,
}
/// Used by grpc
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorRegistration {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub port: u32,
    #[prost(uint32, tag = "4")]
    pub grpc_port: u32,
    #[prost(message, optional, tag = "5")]
    pub specification: ::core::option::Option<ExecutorSpecification>,
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[prost(oneof = "executor_registration::OptionalHost", tags = "2")]
    pub optional_host: ::core::option::Option<executor_registration::OptionalHost>,
}
/// Nested message and enum types in `ExecutorRegistration`.
pub mod executor_registration {
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OptionalHost {
        #[prost(string, tag = "2")]
        Host(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorHeartbeat {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// Unix epoch-based timestamp in seconds
    #[prost(uint64, tag = "2")]
    pub timestamp: u64,
    #[prost(message, repeated, tag = "3")]
    pub metrics: ::prost::alloc::vec::Vec<ExecutorMetric>,
    #[prost(message, optional, tag = "4")]
    pub status: ::core::option::Option<ExecutorStatus>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorSpecification {
    #[prost(message, repeated, tag = "1")]
    pub resources: ::prost::alloc::vec::Vec<ExecutorResource>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorResource {
    /// TODO add more resources
    #[prost(oneof = "executor_resource::Resource", tags = "1")]
    pub resource: ::core::option::Option<executor_resource::Resource>,
}
/// Nested message and enum types in `ExecutorResource`.
pub mod executor_resource {
    /// TODO add more resources
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Resource {
        #[prost(uint32, tag = "1")]
        TaskSlots(u32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorMetric {
    /// TODO add more metrics
    #[prost(oneof = "executor_metric::Metric", tags = "1")]
    pub metric: ::core::option::Option<executor_metric::Metric>,
}
/// Nested message and enum types in `ExecutorMetric`.
pub mod executor_metric {
    /// TODO add more metrics
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Metric {
        #[prost(uint64, tag = "1")]
        AvailableMemory(u64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorStatus {
    #[prost(oneof = "executor_status::Status", tags = "1, 2, 3")]
    pub status: ::core::option::Option<executor_status::Status>,
}
/// Nested message and enum types in `ExecutorStatus`.
pub mod executor_status {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(string, tag = "1")]
        Active(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        Dead(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        Unknown(::prost::alloc::string::String),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinType {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3,
    Leftsemi = 4,
    Leftanti = 5,
    Rightsemi = 6,
    Rightanti = 7,
}
impl JoinType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
            JoinType::Leftsemi => "LEFTSEMI",
            JoinType::Leftanti => "LEFTANTI",
            JoinType::Rightsemi => "RIGHTSEMI",
            JoinType::Rightanti => "RIGHTANTI",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinConstraint {
    On = 0,
    Using = 1,
}
impl JoinConstraint {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinConstraint::On => "ON",
            JoinConstraint::Using => "USING",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ScalarFunction {
    Abs = 0,
    Acos = 1,
    Asin = 2,
    Atan = 3,
    Ascii = 4,
    Ceil = 5,
    Cos = 6,
    Digest = 7,
    Exp = 8,
    Floor = 9,
    Ln = 10,
    Log = 11,
    Log10 = 12,
    Log2 = 13,
    Round = 14,
    Signum = 15,
    Sin = 16,
    Sqrt = 17,
    Tan = 18,
    Trunc = 19,
    Array = 20,
    RegexpMatch = 21,
    BitLength = 22,
    Btrim = 23,
    CharacterLength = 24,
    Chr = 25,
    Concat = 26,
    ConcatWithSeparator = 27,
    DatePart = 28,
    DateTrunc = 29,
    InitCap = 30,
    Left = 31,
    Lpad = 32,
    Lower = 33,
    Ltrim = 34,
    Md5 = 35,
    NullIf = 36,
    OctetLength = 37,
    Random = 38,
    RegexpReplace = 39,
    Repeat = 40,
    Replace = 41,
    Reverse = 42,
    Right = 43,
    Rpad = 44,
    Rtrim = 45,
    Sha224 = 46,
    Sha256 = 47,
    Sha384 = 48,
    Sha512 = 49,
    SplitPart = 50,
    StartsWith = 51,
    Strpos = 52,
    Substr = 53,
    ToHex = 54,
    ToTimestamp = 55,
    ToTimestampMillis = 56,
    ToTimestampMicros = 57,
    ToTimestampSeconds = 58,
    Now = 59,
    Translate = 60,
    Trim = 61,
    Upper = 62,
    Coalesce = 63,
    Power = 64,
    StructFun = 65,
    FromUnixtime = 66,
    Atan2 = 67,
    DateBin = 68,
    ArrowTypeof = 69,
    CurrentDate = 70,
    CurrentTime = 71,
    Uuid = 72,
}
impl ScalarFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ScalarFunction::Abs => "Abs",
            ScalarFunction::Acos => "Acos",
            ScalarFunction::Asin => "Asin",
            ScalarFunction::Atan => "Atan",
            ScalarFunction::Ascii => "Ascii",
            ScalarFunction::Ceil => "Ceil",
            ScalarFunction::Cos => "Cos",
            ScalarFunction::Digest => "Digest",
            ScalarFunction::Exp => "Exp",
            ScalarFunction::Floor => "Floor",
            ScalarFunction::Ln => "Ln",
            ScalarFunction::Log => "Log",
            ScalarFunction::Log10 => "Log10",
            ScalarFunction::Log2 => "Log2",
            ScalarFunction::Round => "Round",
            ScalarFunction::Signum => "Signum",
            ScalarFunction::Sin => "Sin",
            ScalarFunction::Sqrt => "Sqrt",
            ScalarFunction::Tan => "Tan",
            ScalarFunction::Trunc => "Trunc",
            ScalarFunction::Array => "Array",
            ScalarFunction::RegexpMatch => "RegexpMatch",
            ScalarFunction::BitLength => "BitLength",
            ScalarFunction::Btrim => "Btrim",
            ScalarFunction::CharacterLength => "CharacterLength",
            ScalarFunction::Chr => "Chr",
            ScalarFunction::Concat => "Concat",
            ScalarFunction::ConcatWithSeparator => "ConcatWithSeparator",
            ScalarFunction::DatePart => "DatePart",
            ScalarFunction::DateTrunc => "DateTrunc",
            ScalarFunction::InitCap => "InitCap",
            ScalarFunction::Left => "Left",
            ScalarFunction::Lpad => "Lpad",
            ScalarFunction::Lower => "Lower",
            ScalarFunction::Ltrim => "Ltrim",
            ScalarFunction::Md5 => "MD5",
            ScalarFunction::NullIf => "NullIf",
            ScalarFunction::OctetLength => "OctetLength",
            ScalarFunction::Random => "Random",
            ScalarFunction::RegexpReplace => "RegexpReplace",
            ScalarFunction::Repeat => "Repeat",
            ScalarFunction::Replace => "Replace",
            ScalarFunction::Reverse => "Reverse",
            ScalarFunction::Right => "Right",
            ScalarFunction::Rpad => "Rpad",
            ScalarFunction::Rtrim => "Rtrim",
            ScalarFunction::Sha224 => "SHA224",
            ScalarFunction::Sha256 => "SHA256",
            ScalarFunction::Sha384 => "SHA384",
            ScalarFunction::Sha512 => "SHA512",
            ScalarFunction::SplitPart => "SplitPart",
            ScalarFunction::StartsWith => "StartsWith",
            ScalarFunction::Strpos => "Strpos",
            ScalarFunction::Substr => "Substr",
            ScalarFunction::ToHex => "ToHex",
            ScalarFunction::ToTimestamp => "ToTimestamp",
            ScalarFunction::ToTimestampMillis => "ToTimestampMillis",
            ScalarFunction::ToTimestampMicros => "ToTimestampMicros",
            ScalarFunction::ToTimestampSeconds => "ToTimestampSeconds",
            ScalarFunction::Now => "Now",
            ScalarFunction::Translate => "Translate",
            ScalarFunction::Trim => "Trim",
            ScalarFunction::Upper => "Upper",
            ScalarFunction::Coalesce => "Coalesce",
            ScalarFunction::Power => "Power",
            ScalarFunction::StructFun => "StructFun",
            ScalarFunction::FromUnixtime => "FromUnixtime",
            ScalarFunction::Atan2 => "Atan2",
            ScalarFunction::DateBin => "DateBin",
            ScalarFunction::ArrowTypeof => "ArrowTypeof",
            ScalarFunction::CurrentDate => "CurrentDate",
            ScalarFunction::CurrentTime => "CurrentTime",
            ScalarFunction::Uuid => "Uuid",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateFunction {
    Min = 0,
    Max = 1,
    Sum = 2,
    Avg = 3,
    Count = 4,
    ApproxDistinct = 5,
    ArrayAgg = 6,
    Variance = 7,
    VariancePop = 8,
    Covariance = 9,
    CovariancePop = 10,
    Stddev = 11,
    StddevPop = 12,
    Correlation = 13,
    ApproxPercentileCont = 14,
    ApproxMedian = 15,
    ApproxPercentileContWithWeight = 16,
    Grouping = 17,
    Median = 18,
}
impl AggregateFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Count => "COUNT",
            AggregateFunction::ApproxDistinct => "APPROX_DISTINCT",
            AggregateFunction::ArrayAgg => "ARRAY_AGG",
            AggregateFunction::Variance => "VARIANCE",
            AggregateFunction::VariancePop => "VARIANCE_POP",
            AggregateFunction::Covariance => "COVARIANCE",
            AggregateFunction::CovariancePop => "COVARIANCE_POP",
            AggregateFunction::Stddev => "STDDEV",
            AggregateFunction::StddevPop => "STDDEV_POP",
            AggregateFunction::Correlation => "CORRELATION",
            AggregateFunction::ApproxPercentileCont => "APPROX_PERCENTILE_CONT",
            AggregateFunction::ApproxMedian => "APPROX_MEDIAN",
            AggregateFunction::ApproxPercentileContWithWeight => {
                "APPROX_PERCENTILE_CONT_WITH_WEIGHT"
            }
            AggregateFunction::Grouping => "GROUPING",
            AggregateFunction::Median => "MEDIAN",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BuiltInWindowFunction {
    RowNumber = 0,
    Rank = 1,
    DenseRank = 2,
    PercentRank = 3,
    CumeDist = 4,
    Ntile = 5,
    Lag = 6,
    Lead = 7,
    FirstValue = 8,
    LastValue = 9,
    NthValue = 10,
}
impl BuiltInWindowFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BuiltInWindowFunction::RowNumber => "ROW_NUMBER",
            BuiltInWindowFunction::Rank => "RANK",
            BuiltInWindowFunction::DenseRank => "DENSE_RANK",
            BuiltInWindowFunction::PercentRank => "PERCENT_RANK",
            BuiltInWindowFunction::CumeDist => "CUME_DIST",
            BuiltInWindowFunction::Ntile => "NTILE",
            BuiltInWindowFunction::Lag => "LAG",
            BuiltInWindowFunction::Lead => "LEAD",
            BuiltInWindowFunction::FirstValue => "FIRST_VALUE",
            BuiltInWindowFunction::LastValue => "LAST_VALUE",
            BuiltInWindowFunction::NthValue => "NTH_VALUE",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WindowFrameUnits {
    Rows = 0,
    Range = 1,
    Groups = 2,
}
impl WindowFrameUnits {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WindowFrameBoundType {
    CurrentRow = 0,
    Preceding = 1,
    Following = 2,
}
impl WindowFrameBoundType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WindowFrameBoundType::CurrentRow => "CURRENT_ROW",
            WindowFrameBoundType::Preceding => "PRECEDING",
            WindowFrameBoundType::Following => "FOLLOWING",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DateUnit {
    Day = 0,
    DateMillisecond = 1,
}
impl DateUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DateUnit::Day => "Day",
            DateUnit::DateMillisecond => "DateMillisecond",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Second => "Second",
            TimeUnit::Millisecond => "Millisecond",
            TimeUnit::Microsecond => "Microsecond",
            TimeUnit::Nanosecond => "Nanosecond",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
    MonthDayNano = 2,
}
impl IntervalUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
            IntervalUnit::MonthDayNano => "MonthDayNano",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UnionMode {
    Sparse = 0,
    Dense = 1,
}
impl UnionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UnionMode::Sparse => "sparse",
            UnionMode::Dense => "dense",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PartitionMode {
    CollectLeft = 0,
    Partitioned = 1,
    Auto = 2,
}
impl PartitionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PartitionMode::CollectLeft => "COLLECT_LEFT",
            PartitionMode::Partitioned => "PARTITIONED",
            PartitionMode::Auto => "AUTO",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateMode {
    Partial = 0,
    Final = 1,
    FinalPartitioned = 2,
}
impl AggregateMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregateMode::Partial => "PARTIAL",
            AggregateMode::Final => "FINAL",
            AggregateMode::FinalPartitioned => "FINAL_PARTITIONED",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinSide {
    LeftSide = 0,
    RightSide = 1,
}
impl JoinSide {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinSide::LeftSide => "LEFT_SIDE",
            JoinSide::RightSide => "RIGHT_SIDE",
        }
    }
}
