#![allow(nonstandard_style)]
// Generated from Presto.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::prestoparser::*;

pub trait PrestoListener<'input> : ParseTreeListener<'input,PrestoParserContextType>{
/**
 * Enter a parse tree produced by {@link PrestoParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#standalonePathSpecification}.
 * @param ctx the parse tree
 */
fn enter_standalonePathSpecification(&mut self, _ctx: &StandalonePathSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#standalonePathSpecification}.
 * @param ctx the parse tree
 */
fn exit_standalonePathSpecification(&mut self, _ctx: &StandalonePathSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#standaloneRowPattern}.
 * @param ctx the parse tree
 */
fn enter_standaloneRowPattern(&mut self, _ctx: &StandaloneRowPatternContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#standaloneRowPattern}.
 * @param ctx the parse tree
 */
fn exit_standaloneRowPattern(&mut self, _ctx: &StandaloneRowPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commentTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commentTable(&mut self, _ctx: &CommentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commentTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commentTable(&mut self, _ctx: &CommentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commentView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commentView(&mut self, _ctx: &CommentViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commentView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commentView(&mut self, _ctx: &CommentViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commentColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commentColumn(&mut self, _ctx: &CommentColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commentColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commentColumn(&mut self, _ctx: &CommentColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropMaterializedView(&mut self, _ctx: &DropMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropMaterializedView(&mut self, _ctx: &DropMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code call}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code call}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropRole(&mut self, _ctx: &DropRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropRole(&mut self, _ctx: &DropRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grantRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grantRoles(&mut self, _ctx: &GrantRolesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grantRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grantRoles(&mut self, _ctx: &GrantRolesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revokeRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revokeRoles(&mut self, _ctx: &RevokeRolesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revokeRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revokeRoles(&mut self, _ctx: &RevokeRolesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setRole(&mut self, _ctx: &SetRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setRole}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setRole(&mut self, _ctx: &SetRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deny}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deny}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showGrants}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showGrants(&mut self, _ctx: &ShowGrantsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showGrants}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showGrants(&mut self, _ctx: &ShowGrantsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explain}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explain}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explainAnalyze}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explainAnalyze(&mut self, _ctx: &ExplainAnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explainAnalyze}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explainAnalyze(&mut self, _ctx: &ExplainAnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showCreateTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showCreateTable(&mut self, _ctx: &ShowCreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showCreateTable}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showCreateTable(&mut self, _ctx: &ShowCreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showCreateSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showCreateSchema(&mut self, _ctx: &ShowCreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showCreateSchema}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showCreateSchema(&mut self, _ctx: &ShowCreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showCreateView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showCreateView(&mut self, _ctx: &ShowCreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showCreateView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showCreateView(&mut self, _ctx: &ShowCreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showCreateMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showCreateMaterializedView(&mut self, _ctx: &ShowCreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showCreateMaterializedView}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showCreateMaterializedView(&mut self, _ctx: &ShowCreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showTables}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showTables(&mut self, _ctx: &ShowTablesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showTables}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showTables(&mut self, _ctx: &ShowTablesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showSchemas}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showSchemas(&mut self, _ctx: &ShowSchemasContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showSchemas}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showSchemas(&mut self, _ctx: &ShowSchemasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showCatalogs}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showCatalogs(&mut self, _ctx: &ShowCatalogsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showCatalogs}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showCatalogs(&mut self, _ctx: &ShowCatalogsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showStats}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showStats(&mut self, _ctx: &ShowStatsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showStats}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showStats(&mut self, _ctx: &ShowStatsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showStatsForQuery}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showStatsForQuery(&mut self, _ctx: &ShowStatsForQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showStatsForQuery}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showStatsForQuery(&mut self, _ctx: &ShowStatsForQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showRoles(&mut self, _ctx: &ShowRolesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showRoles}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showRoles(&mut self, _ctx: &ShowRolesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showRoleGrants}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showRoleGrants(&mut self, _ctx: &ShowRoleGrantsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showRoleGrants}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showRoleGrants(&mut self, _ctx: &ShowRoleGrantsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showFunctions}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showFunctions(&mut self, _ctx: &ShowFunctionsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showFunctions}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showFunctions(&mut self, _ctx: &ShowFunctionsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showSession(&mut self, _ctx: &ShowSessionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showSession(&mut self, _ctx: &ShowSessionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSession(&mut self, _ctx: &SetSessionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSession(&mut self, _ctx: &SetSessionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code resetSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_resetSession(&mut self, _ctx: &ResetSessionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code resetSession}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_resetSession(&mut self, _ctx: &ResetSessionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prepare}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prepare}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setPath}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setPath(&mut self, _ctx: &SetPathContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setPath}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setPath(&mut self, _ctx: &SetPathContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTimeZone}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTimeZone(&mut self, _ctx: &SetTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTimeZone}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTimeZone(&mut self, _ctx: &SetTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link PrestoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#likeClause}.
 * @param ctx the parse tree
 */
fn enter_likeClause(&mut self, _ctx: &LikeClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#likeClause}.
 * @param ctx the parse tree
 */
fn exit_likeClause(&mut self, _ctx: &LikeClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#property}.
 * @param ctx the parse tree
 */
fn enter_property(&mut self, _ctx: &PropertyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#property}.
 * @param ctx the parse tree
 */
fn exit_property(&mut self, _ctx: &PropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link PrestoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link PrestoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nonDefaultPropertyValue}
 * labeled alternative in {@link PrestoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_nonDefaultPropertyValue(&mut self, _ctx: &NonDefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nonDefaultPropertyValue}
 * labeled alternative in {@link PrestoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_nonDefaultPropertyValue(&mut self, _ctx: &NonDefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryTermDefault}
 * labeled alternative in {@link PrestoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTermDefault(&mut self, _ctx: &QueryTermDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryTermDefault}
 * labeled alternative in {@link PrestoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTermDefault(&mut self, _ctx: &QueryTermDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setOperation}
 * labeled alternative in {@link PrestoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setOperation}
 * labeled alternative in {@link PrestoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTable}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTable}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link PrestoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupBy(&mut self, _ctx: &GroupByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupBy(&mut self, _ctx: &GroupByContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollup}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollup}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cube}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cube}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link PrestoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link PrestoParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link PrestoParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectAll}
 * labeled alternative in {@link PrestoParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectAll(&mut self, _ctx: &SelectAllContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectAll}
 * labeled alternative in {@link PrestoParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectAll(&mut self, _ctx: &SelectAllContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link PrestoParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link PrestoParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link PrestoParser#relation}.
 * @param ctx the parse tree
 */
fn enter_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link PrestoParser#relation}.
 * @param ctx the parse tree
 */
fn exit_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn enter_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn exit_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#sampleType}.
 * @param ctx the parse tree
 */
fn enter_sampleType(&mut self, _ctx: &SampleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#sampleType}.
 * @param ctx the parse tree
 */
fn exit_sampleType(&mut self, _ctx: &SampleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn enter_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn exit_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn enter_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn exit_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn enter_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn exit_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn enter_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn exit_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn enter_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn exit_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn enter_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn exit_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#skipTo}.
 * @param ctx the parse tree
 */
fn enter_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#skipTo}.
 * @param ctx the parse tree
 */
fn exit_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn enter_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn exit_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unnest}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unnest}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lateral}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lateral}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link PrestoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionCall(&mut self, _ctx: &TableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionCall(&mut self, _ctx: &TableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link PrestoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link PrestoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link PrestoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link PrestoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn enter_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn exit_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#descriptorField}.
 * @param ctx the parse tree
 */
fn enter_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#descriptorField}.
 * @param ctx the parse tree
 */
fn exit_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn enter_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn exit_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link PrestoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link PrestoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link PrestoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code specialDateTimeFunction}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_specialDateTimeFunction(&mut self, _ctx: &SpecialDateTimeFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code specialDateTimeFunction}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_specialDateTimeFunction(&mut self, _ctx: &SpecialDateTimeFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code substring}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code substring}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parameter}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parameter(&mut self, _ctx: &ParameterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parameter}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parameter(&mut self, _ctx: &ParameterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code normalize}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code normalize}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subscript}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subscript}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentPath}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_currentPath(&mut self, _ctx: &CurrentPathContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentPath}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_currentPath(&mut self, _ctx: &CurrentPathContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentUser}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_currentUser(&mut self, _ctx: &CurrentUserContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentUser}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_currentUser(&mut self, _ctx: &CurrentUserContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extract}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extract}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentSchema}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_currentSchema(&mut self, _ctx: &CurrentSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentSchema}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_currentSchema(&mut self, _ctx: &CurrentSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code position}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code position}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code listagg}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code listagg}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentCatalog}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_currentCatalog(&mut self, _ctx: &CurrentCatalogContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentCatalog}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_currentCatalog(&mut self, _ctx: &CurrentCatalogContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupingOperation}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_groupingOperation(&mut self, _ctx: &GroupingOperationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupingOperation}
 * labeled alternative in {@link PrestoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_groupingOperation(&mut self, _ctx: &GroupingOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn enter_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn exit_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn enter_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn exit_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn enter_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn exit_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn enter_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn exit_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#processingMode}.
 * @param ctx the parse tree
 */
fn enter_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#processingMode}.
 * @param ctx the parse tree
 */
fn exit_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link PrestoParser#string}.
 * @param ctx the parse tree
 */
fn enter_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link PrestoParser#string}.
 * @param ctx the parse tree
 */
fn exit_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link PrestoParser#string}.
 * @param ctx the parse tree
 */
fn enter_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link PrestoParser#string}.
 * @param ctx the parse tree
 */
fn exit_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code timeZoneInterval}
 * labeled alternative in {@link PrestoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneInterval(&mut self, _ctx: &TimeZoneIntervalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code timeZoneInterval}
 * labeled alternative in {@link PrestoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneInterval(&mut self, _ctx: &TimeZoneIntervalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code timeZoneString}
 * labeled alternative in {@link PrestoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneString(&mut self, _ctx: &TimeZoneStringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code timeZoneString}
 * labeled alternative in {@link PrestoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneString(&mut self, _ctx: &TimeZoneStringContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#intervalField}.
 * @param ctx the parse tree
 */
fn enter_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#intervalField}.
 * @param ctx the parse tree
 */
fn exit_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#normalForm}.
 * @param ctx the parse tree
 */
fn enter_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#normalForm}.
 * @param ctx the parse tree
 */
fn exit_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_arrayType(&mut self, _ctx: &ArrayTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_arrayType(&mut self, _ctx: &ArrayTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code genericType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_genericType(&mut self, _ctx: &GenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code genericType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_genericType(&mut self, _ctx: &GenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link PrestoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeUpdate}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeUpdate(&mut self, _ctx: &MergeUpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeUpdate}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeUpdate(&mut self, _ctx: &MergeUpdateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeDelete}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeDelete(&mut self, _ctx: &MergeDeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeDelete}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeDelete(&mut self, _ctx: &MergeDeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeInsert}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeInsert(&mut self, _ctx: &MergeInsertContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeInsert}
 * labeled alternative in {@link PrestoParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeInsert(&mut self, _ctx: &MergeInsertContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link PrestoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link PrestoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link PrestoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link PrestoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#updateAssignment}.
 * @param ctx the parse tree
 */
fn enter_updateAssignment(&mut self, _ctx: &UpdateAssignmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#updateAssignment}.
 * @param ctx the parse tree
 */
fn exit_updateAssignment(&mut self, _ctx: &UpdateAssignmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explainFormat}
 * labeled alternative in {@link PrestoParser#explainOption}.
 * @param ctx the parse tree
 */
fn enter_explainFormat(&mut self, _ctx: &ExplainFormatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explainFormat}
 * labeled alternative in {@link PrestoParser#explainOption}.
 * @param ctx the parse tree
 */
fn exit_explainFormat(&mut self, _ctx: &ExplainFormatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explainType}
 * labeled alternative in {@link PrestoParser#explainOption}.
 * @param ctx the parse tree
 */
fn enter_explainType(&mut self, _ctx: &ExplainTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explainType}
 * labeled alternative in {@link PrestoParser#explainOption}.
 * @param ctx the parse tree
 */
fn exit_explainType(&mut self, _ctx: &ExplainTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link PrestoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link PrestoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link PrestoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link PrestoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code serializable}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code serializable}
 * labeled alternative in {@link PrestoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link PrestoParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link PrestoParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link PrestoParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link PrestoParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedArgument}
 * labeled alternative in {@link PrestoParser#pathElement}.
 * @param ctx the parse tree
 */
fn enter_qualifiedArgument(&mut self, _ctx: &QualifiedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedArgument}
 * labeled alternative in {@link PrestoParser#pathElement}.
 * @param ctx the parse tree
 */
fn exit_qualifiedArgument(&mut self, _ctx: &QualifiedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unqualifiedArgument}
 * labeled alternative in {@link PrestoParser#pathElement}.
 * @param ctx the parse tree
 */
fn enter_unqualifiedArgument(&mut self, _ctx: &UnqualifiedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unqualifiedArgument}
 * labeled alternative in {@link PrestoParser#pathElement}.
 * @param ctx the parse tree
 */
fn exit_unqualifiedArgument(&mut self, _ctx: &UnqualifiedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#pathSpecification}.
 * @param ctx the parse tree
 */
fn enter_pathSpecification(&mut self, _ctx: &PathSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#pathSpecification}.
 * @param ctx the parse tree
 */
fn exit_pathSpecification(&mut self, _ctx: &PathSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedName(&mut self, _ctx: &QualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedName(&mut self, _ctx: &QualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code specifiedPrincipal}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn enter_specifiedPrincipal(&mut self, _ctx: &SpecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code specifiedPrincipal}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn exit_specifiedPrincipal(&mut self, _ctx: &SpecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentUserGrantor}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn enter_currentUserGrantor(&mut self, _ctx: &CurrentUserGrantorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentUserGrantor}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn exit_currentUserGrantor(&mut self, _ctx: &CurrentUserGrantorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRoleGrantor}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn enter_currentRoleGrantor(&mut self, _ctx: &CurrentRoleGrantorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRoleGrantor}
 * labeled alternative in {@link PrestoParser#grantor}.
 * @param ctx the parse tree
 */
fn exit_currentRoleGrantor(&mut self, _ctx: &CurrentRoleGrantorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link PrestoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#roles}.
 * @param ctx the parse tree
 */
fn enter_roles(&mut self, _ctx: &RolesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#roles}.
 * @param ctx the parse tree
 */
fn exit_roles(&mut self, _ctx: &RolesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code digitIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_digitIdentifier(&mut self, _ctx: &DigitIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code digitIdentifier}
 * labeled alternative in {@link PrestoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_digitIdentifier(&mut self, _ctx: &DigitIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link PrestoParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PrestoParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PrestoParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : PrestoListener<'input> }


