package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class CloudSpannerMetaData extends AbstractCloudSpannerMetaData
{
	private CloudSpannerConnection connection;

	CloudSpannerMetaData(CloudSpannerConnection connection)
	{
		this.connection = connection;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException
	{
		return true;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException
	{
		return true;
	}

	@Override
	public String getURL() throws SQLException
	{
		return connection.getUrl();
	}

	@Override
	public String getUserName() throws SQLException
	{
		return null;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException
	{
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException
	{
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException
	{
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException
	{
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException
	{
		return connection.getProductName();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException
	{
		return null;
	}

	@Override
	public String getDriverName() throws SQLException
	{
		return CloudSpannerDriver.class.getName();
	}

	@Override
	public String getDriverVersion() throws SQLException
	{
		return CloudSpannerDriver.MAJOR_VERSION + "." + CloudSpannerDriver.MINOR_VERSION;
	}

	@Override
	public int getDriverMajorVersion()
	{
		return CloudSpannerDriver.MAJOR_VERSION;
	}

	@Override
	public int getDriverMinorVersion()
	{
		return CloudSpannerDriver.MINOR_VERSION;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException
	{
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
	{
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException
	{
		return "`";
	}

	@Override
	public String getSQLKeywords() throws SQLException
	{
		return "INTERLEAVE, PARENT";
	}

	@Override
	public String getNumericFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException
	{
		return "\\";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException
	{
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException
	{
		return true;
	}

	@Override
	public String getSchemaTerm() throws SQLException
	{
		return null;
	}

	@Override
	public String getProcedureTerm() throws SQLException
	{
		return null;
	}

	@Override
	public String getCatalogTerm() throws SQLException
	{
		return null;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException
	{
		return false;
	}

	@Override
	public String getCatalogSeparator() throws SQLException
	{
		return null;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsUnion() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException
	{
		return 128;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException
	{
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException
	{
		return 128;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException
	{
		return Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException
	{
		return Connection.TRANSACTION_SERIALIZABLE == level;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException
	{
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		return true;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException
	{
		return false;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException
	{
		String sql = "select TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, 'TABLE' AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM information_schema.tables AS t ORDER BY TABLE_NAME";
		return connection.createStatement().executeQuery(sql);
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		String sql = "select 'TABLE' AS TABLE_TYPE";
		return connection.createStatement().executeQuery(sql);
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException
	{
		String sql = "select TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, 1 AS DATA_TYPE, SPANNER_TYPE AS TYPE_NAME, "
				+ "0 AS COLUMN_LENGTH, 0 AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, "
				+ "CASE "
				+ "	WHEN IS_NULLABLE = 'YES' THEN 1 "
				+ "	WHEN IS_NULLABLE = 'NO' THEN 0 "
				+ "	ELSE 2 "
				+ "END AS NULLABLE, NULL AS REMARKS, NULL AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, NULL AS SCOPE_CATALOG, "
				+ "NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, 'NO' AS IS_GENERATEDCOLUMN "
				+ "FROM information_schema.columns "
				+ "WHERE TABLE_NAME LIKE %TABLE_NAME% "
				+ "AND COLUMN_NAME LIKE %COLUMN_NAME% "
				+ "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ";

		sql = sql.replace("%TABLE_NAME%", tableNamePattern == null ? "%" : tableNamePattern);
		sql = sql.replace("%COLUMN_NAME%", columnNamePattern == null ? "%" : columnNamePattern);

		return connection.createStatement().executeQuery(sql);
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException
	{
		String sql = "select IDX.TABLE_CATALOG AS TABLE_CAT, IDX.TABLE_SCHEMA AS TABLE_SCHEM, IDX.TABLE_NAME AS TABLE_NAME, COLS.COLUMN_NAME AS COLUMN_NAME, ORDINAL_POSITION AS KEY_SEQ, IDX.INDEX_NAME AS PK_NAME "
				+ "from information_schema.indexes idx "
				+ "inner join information_schema.index_columns cols on idx.table_catalog=cols.table_catalog and idx.table_schema=cols.table_schema and idx.table_name=cols.table_name and idx.index_name=cols.index_name "
				+ "where index_type='PRIMARY_KEY' and idx.table_name = %TABLE_NAME% ORDER BY ORDINAL_POSITION";
		sql = sql.replace("%TABLE_NAME%", table);
		return connection.createStatement().executeQuery(sql);
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException
	{
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException
	{
		return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException
	{
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException
	{
		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException
	{
		return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getSQLStateType() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException
	{
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException
	{
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
	{
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException
	{
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException
	{
		return false;
	}

}
