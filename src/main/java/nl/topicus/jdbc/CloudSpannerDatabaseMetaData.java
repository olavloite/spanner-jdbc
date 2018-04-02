package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;

public class CloudSpannerDatabaseMetaData extends AbstractCloudSpannerDatabaseMetaData
{
	private static final int JDBC_MAJOR_VERSION = 4;

	private static final int JDBC_MINOR_VERSION = 2;

	private static final String FROM_STATEMENT_WITHOUT_RESULTS = " FROM INFORMATION_SCHEMA.TABLES T WHERE 1=2 ";

	private static final String VERSION_AND_IDENTIFIER_COLUMNS_SELECT_STATEMENT = "SELECT 0 AS SCOPE, '' AS COLUMN_NAME, 0 AS DATA_TYPE, '' AS TYPE_NAME, 0 AS COLUMN_SIZE, 0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS PSEUDO_COLUMN ";

	private CloudSpannerConnection connection;

	CloudSpannerDatabaseMetaData(CloudSpannerConnection connection)
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
		return connection.getClientId();
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
		return true;
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
		return getDatabaseMajorVersion() + "." + getDatabaseMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException
	{
		return CloudSpannerDriver.class.getName();
	}

	@Override
	public String getDriverVersion() throws SQLException
	{
		return getDriverMajorVersion() + "." + getDriverMinorVersion();
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
		return true;
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
		return true;
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
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		return true;
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
		return 16;
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
		return 1024;
	}

	/**
	 * Limit is 10,000 per database per node
	 */
	@Override
	public int getMaxConnections() throws SQLException
	{
		return connection.getNodeCount() * 10000;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException
	{
		return 8000;
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
		return 1024 * 10000000;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		return true;
	}

	@Override
	public int getMaxStatementLength() throws SQLException
	{
		return 1000000;
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
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		return false;
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
		String sql = "SELECT '' AS PROCEDURE_CAT, '' AS PROCEDURE_SCHEM, '' AS PROCEDURE_NAME, NULL AS RES1, NULL AS RES2, NULL AS RES3, "
				+ "'' AS REMARKS, 0 AS PROCEDURE_TYPE, '' AS SPECIFIC_NAME " + FROM_STATEMENT_WITHOUT_RESULTS;

		PreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException
	{
		String sql = "SELECT '' AS PROCEDURE_CAT, '' AS PROCEDURE_SCHEM, '' AS PROCEDURE_NAME, '' AS COLUMN_NAME, 0 AS COLUMN_TYPE, "
				+ "0 AS DATA_TYPE, '' AS TYPE_NAME, 0 AS PRECISION, 0 AS LENGTH, 0 AS SCALE, 0 AS RADIX, "
				+ "0 AS NULLABLE, '' AS REMARKS, '' AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATATIME_SUB, "
				+ "0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, '' AS IS_NULLABLE, '' AS SPECIFIC_NAME "
				+ FROM_STATEMENT_WITHOUT_RESULTS;

		PreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	private CloudSpannerPreparedStatement prepareStatement(String sql, String... params) throws SQLException
	{
		CloudSpannerPreparedStatement statement = connection.prepareStatement(sql);
		statement.setForceSingleUseReadContext(true);
		int paramIndex = 1;
		for (String param : params)
		{
			if (param != null)
			{
				statement.setString(paramIndex, param.toUpperCase());
				paramIndex++;
			}
		}
		return statement;
	}

	private String getCatalogSchemaTableWhereClause(String alias, String catalog, String schema, String table)
	{
		StringBuilder res = new StringBuilder();
		if (catalog != null)
			res.append(String.format("AND UPPER(%s.TABLE_CATALOG) like ? ", alias));
		if (schema != null)
			res.append(String.format("AND UPPER(%s.TABLE_SCHEMA) like ? ", alias));
		if (table != null)
			res.append(String.format("AND UPPER(%s.TABLE_NAME) like ? ", alias));
		return res.toString();
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException
	{
		String sql = CloudSpannerDatabaseMetaDataConstants.SELECT_TABLES_COLUMNS
				+ CloudSpannerDatabaseMetaDataConstants.FROM_TABLES_T
				+ CloudSpannerDatabaseMetaDataConstants.WHERE_1_EQUALS_1;
		sql = sql + getCatalogSchemaTableWhereClause("T", catalog, schemaPattern, tableNamePattern);
		sql = sql + "ORDER BY TABLE_NAME";

		CloudSpannerPreparedStatement statement = prepareStatement(sql, catalog, schemaPattern, tableNamePattern);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		return getSchemas(null, null);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException
	{
		String sql = "SELECT '' AS TABLE_CAT " + FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		String sql = "SELECT 'TABLE' AS TABLE_TYPE";
		return prepareStatement(sql).executeQuery();
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException
	{
		String sql = CloudSpannerDatabaseMetaDataConstants.GET_COLUMNS;
		sql = sql + getCatalogSchemaTableWhereClause("C", catalog, schemaPattern, tableNamePattern);
		if (columnNamePattern != null)
			sql = sql + "AND UPPER(COLUMN_NAME) LIKE ? ";
		sql = sql + "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ";

		CloudSpannerPreparedStatement statement = prepareStatement(sql, catalog, schemaPattern, tableNamePattern,
				columnNamePattern);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException
	{
		String sql = "SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS COLUMN_NAME, '' AS GRANTOR, '' AS GRANTEE, '' AS PRIVILEGE, 'NO' AS IS_GRANTABLE "
				+ FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException
	{
		String sql = "SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS GRANTOR, '' AS GRANTEE, '' AS PRIVILEGE, 'NO' AS IS_GRANTABLE "
				+ FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException
	{
		return getVersionColumnsOrBestRowIdentifier();
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException
	{
		return getVersionColumnsOrBestRowIdentifier();
	}

	/**
	 * A simple private method that combines the result of two methods that
	 * return exactly the same result
	 * 
	 * @return An empty {@link ResultSet} containing the columns for the methods
	 *         {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)}
	 *         and
	 *         {@link DatabaseMetaData#getVersionColumns(String, String, String)}
	 * @throws SQLException
	 */
	private ResultSet getVersionColumnsOrBestRowIdentifier() throws SQLException
	{
		String sql = VERSION_AND_IDENTIFIER_COLUMNS_SELECT_STATEMENT + FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException
	{
		String sql = "SELECT IDX.TABLE_CATALOG AS TABLE_CAT, IDX.TABLE_SCHEMA AS TABLE_SCHEM, IDX.TABLE_NAME AS TABLE_NAME, COLS.COLUMN_NAME AS COLUMN_NAME, ORDINAL_POSITION AS KEY_SEQ, IDX.INDEX_NAME AS PK_NAME "
				+ "FROM INFORMATION_SCHEMA.INDEXES IDX "
				+ "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS COLS ON IDX.TABLE_CATALOG=COLS.TABLE_CATALOG AND IDX.TABLE_SCHEMA=COLS.TABLE_SCHEMA AND IDX.TABLE_NAME=COLS.TABLE_NAME AND IDX.INDEX_NAME=COLS.INDEX_NAME "
				+ "WHERE IDX.INDEX_TYPE='PRIMARY_KEY' ";
		sql = sql + getCatalogSchemaTableWhereClause("IDX", catalog, schema, table);
		sql = sql + "ORDER BY COLS.ORDINAL_POSITION ";

		PreparedStatement statement = prepareStatement(sql, catalog, schema, table);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException
	{
		String sql = "SELECT PARENT.TABLE_CATALOG AS PKTABLE_CAT, PARENT.TABLE_SCHEMA AS PKTABLE_SCHEM, PARENT.TABLE_NAME AS PKTABLE_NAME, COL.COLUMN_NAME AS PKCOLUMN_NAME, CHILD.TABLE_CATALOG AS FKTABLE_CAT, CHILD.TABLE_SCHEMA AS FKTABLE_SCHEM, CHILD.TABLE_NAME AS FKTABLE_NAME, COL.COLUMN_NAME FKCOLUMN_NAME, COL.ORDINAL_POSITION AS KEY_SEQ, 3 AS UPDATE_RULE, CASE WHEN CHILD.ON_DELETE_ACTION = 'CASCADE' THEN 0 ELSE 3 END AS DELETE_RULE, NULL AS FK_NAME, INDEXES.INDEX_NAME AS PK_NAME, 7 AS DEFERRABILITY "
				+ "FROM INFORMATION_SCHEMA.TABLES CHILD "
				+ "INNER JOIN INFORMATION_SCHEMA.TABLES PARENT ON CHILD.TABLE_CATALOG=PARENT.TABLE_CATALOG AND CHILD.TABLE_SCHEMA=PARENT.TABLE_SCHEMA AND CHILD.PARENT_TABLE_NAME=PARENT.TABLE_NAME "
				+ "INNER JOIN INFORMATION_SCHEMA.INDEXES ON PARENT.TABLE_CATALOG=INDEXES.TABLE_CATALOG AND PARENT.TABLE_SCHEMA=INDEXES.TABLE_SCHEMA AND PARENT.TABLE_NAME=INDEXES.TABLE_NAME AND INDEXES.INDEX_TYPE='PRIMARY_KEY' "
				+ "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS COL ON INDEXES.TABLE_CATALOG=COL.TABLE_CATALOG AND INDEXES.TABLE_SCHEMA=COL.TABLE_SCHEMA AND INDEXES.TABLE_NAME=COL.TABLE_NAME AND INDEXES.INDEX_NAME=COL.INDEX_NAME "
				+ "WHERE CHILD.PARENT_TABLE_NAME IS NOT NULL ";

		sql = sql + getCatalogSchemaTableWhereClause("CHILD", catalog, schema, table);
		sql = sql + "ORDER BY PARENT.TABLE_CATALOG, PARENT.TABLE_SCHEMA, PARENT.TABLE_NAME, COL.ORDINAL_POSITION ";

		PreparedStatement statement = prepareStatement(sql, catalog, schema, table);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
	{
		String sql = "SELECT "
				+ "NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, PARENT.TABLE_NAME AS PKTABLE_NAME, PARENT_INDEX_COLUMNS.COLUMN_NAME AS PKCOLUMN_NAME, "
				+ "NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, CHILD.TABLE_NAME AS FKTABLE_NAME, PARENT_INDEX_COLUMNS.COLUMN_NAME AS FKCOLUMN_NAME, "
				+ "PARENT_INDEX_COLUMNS.ORDINAL_POSITION AS KEY_SEQ, 3 AS UPDATE_RULE, CASE WHEN CHILD.ON_DELETE_ACTION='CASCADE' THEN 0 ELSE 3 END AS DELETE_RULE, "
				+ "NULL AS FK_NAME, 'PRIMARY_KEY' AS PK_NAME, 7 AS DEFERRABILITY "
				+ "FROM INFORMATION_SCHEMA.TABLES PARENT "
				+ "INNER JOIN INFORMATION_SCHEMA.TABLES CHILD ON CHILD.PARENT_TABLE_NAME=PARENT.TABLE_NAME "
				+ "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS PARENT_INDEX_COLUMNS ON PARENT_INDEX_COLUMNS.TABLE_NAME=PARENT.TABLE_NAME AND PARENT_INDEX_COLUMNS.INDEX_NAME='PRIMARY_KEY' "
				+ CloudSpannerDatabaseMetaDataConstants.WHERE_1_EQUALS_1;

		sql = sql + getCatalogSchemaTableWhereClause("PARENT", catalog, schema, table);
		sql = sql
				+ "ORDER BY CHILD.TABLE_CATALOG, CHILD.TABLE_SCHEMA, CHILD.TABLE_NAME, PARENT_INDEX_COLUMNS.ORDINAL_POSITION ";

		CloudSpannerPreparedStatement statement = prepareStatement(sql, catalog, schema, table);
		return statement.executeQuery();
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
		String sql = CloudSpannerDatabaseMetaDataConstants.GET_TYPE_INFO;
		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException
	{
		return getIndexInfo(catalog, schema, table, null, unique);
	}

	public ResultSet getIndexInfo(String catalog, String schema, String indexName) throws SQLException
	{
		return getIndexInfo(catalog, schema, null, indexName, false);
	}

	private ResultSet getIndexInfo(String catalog, String schema, String table, String indexName, boolean unique)
			throws SQLException
	{
		String sql = CloudSpannerDatabaseMetaDataConstants.GET_INDEX_INFO;

		sql = sql + getCatalogSchemaTableWhereClause("IDX", catalog, schema, table);
		if (unique)
			sql = sql + "AND IS_UNIQUE=TRUE ";
		if (indexName != null)
			sql = sql + " AND IDX.INDEX_NAME LIKE ? ";
		sql = sql + "ORDER BY IS_UNIQUE, INDEX_NAME, ORDINAL_POSITION ";

		PreparedStatement statement = prepareStatement(sql, catalog, schema, table, indexName);
		return statement.executeQuery();
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
		return true;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException
	{
		String sql = CloudSpannerDatabaseMetaDataConstants.GET_UDTS;
		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException
	{
		return false;
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
		String sql = "SELECT '' AS TYPE_CAT, '' AS TYPE_SCHEM, '' AS TYPE_NAME, "
				+ "'' AS SUPERTYPE_CAT, '' AS SUPERTYPE_SCHEM, '' AS SUPERTYPE_NAME " + FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
	{
		String sql = "SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS SUPERTABLE_NAME "
				+ FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException
	{
		String sql = "SELECT '' AS TYPE_CAT, '' AS TYPE_SCHEM, '' AS TYPE_NAME, '' AS ATTR_NAME, 0 AS DATA_TYPE, "
				+ "'' AS ATTR_TYPE_NAME, 0 AS ATTR_SIZE, 0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE, "
				+ "'' AS REMARKS, '' AS ATTR_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH, "
				+ "0 AS ORDINAL_POSITION, 'NO' AS IS_NULLABLE, '' AS SCOPE_CATALOG, '' AS SCOPE_SCHEMA, '' AS SCOPE_TABLE, "
				+ "0 AS SOURCE_DATA_TYPE " + FROM_STATEMENT_WITHOUT_RESULTS;

		CloudSpannerPreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException
	{
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException
	{
		return connection.getSimulateMajorVersion() == null ? 1 : connection.getSimulateMajorVersion();
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException
	{
		return connection.getSimulateMinorVersion() == null ? 0 : connection.getSimulateMinorVersion();
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException
	{
		return JDBC_MAJOR_VERSION;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException
	{
		return JDBC_MINOR_VERSION;
	}

	@Override
	public int getSQLStateType() throws SQLException
	{
		return sqlStateSQL;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return true;
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
		String sql = "SELECT SCHEMA_NAME AS TABLE_SCHEM, CATALOG_NAME AS TABLE_CATALOG FROM INFORMATION_SCHEMA.SCHEMATA "
				+ CloudSpannerDatabaseMetaDataConstants.WHERE_1_EQUALS_1;
		if (catalog != null)
			sql = sql + "AND UPPER(CATALOG_NAME) like ? ";
		if (schemaPattern != null)
			sql = sql + "AND UPPER(SCHEMA_NAME) like ? ";
		sql = sql + "ORDER BY SCHEMA_NAME";

		PreparedStatement statement = prepareStatement(sql, catalog, schemaPattern);
		return statement.executeQuery();
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
		String sql = "SELECT '' AS NAME, 0 AS MAX_LEN, '' AS DEFAULT_VALUE, '' AS DESCRIPTION "
				+ FROM_STATEMENT_WITHOUT_RESULTS;
		sql = sql + " ORDER BY NAME ";

		PreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException
	{
		String sql = "SELECT '' AS FUNCTION_CAT, '' AS FUNCTION_SCHEM, '' AS FUNCTION_NAME, '' AS REMARKS, 0 AS FUNCTION_TYPE, '' AS SPECIFIC_NAME "
				+ FROM_STATEMENT_WITHOUT_RESULTS;
		sql = sql + " ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME ";

		PreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException
	{
		String sql = "SELECT '' AS FUNCTION_CAT, '' AS FUNCTION_SCHEM, '' AS FUNCTION_NAME, '' AS COLUMN_NAME, 0 AS COLUMN_TYPE, 1111 AS DATA_TYPE, '' AS TYPE_NAME, 0 AS PRECISION, 0 AS LENGTH, 0 AS SCALE, 0 AS RADIX, 0 AS NULLABLE, '' AS REMARKS, 0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, '' AS IS_NULLABLE, '' AS SPECIFIC_NAME "
				+ FROM_STATEMENT_WITHOUT_RESULTS;
		sql = sql + " ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME ";

		PreparedStatement statement = prepareStatement(sql);
		return statement.executeQuery();
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException
	{
		String sql = "select TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, "
				+ "CASE " + "	WHEN SPANNER_TYPE = 'ARRAY' THEN " + Types.ARRAY + " "
				+ "	WHEN SPANNER_TYPE = 'BOOL' THEN " + Types.BOOLEAN + " " + "	WHEN SPANNER_TYPE = 'BYTES' THEN "
				+ Types.BINARY + " " + "	WHEN SPANNER_TYPE = 'DATE' THEN " + Types.DATE + " "
				+ "	WHEN SPANNER_TYPE = 'FLOAT64' THEN " + Types.DOUBLE + " " + "	WHEN SPANNER_TYPE = 'INT64' THEN "
				+ Types.BIGINT + " " + "	WHEN SPANNER_TYPE = 'STRING' THEN " + Types.NVARCHAR + " "
				+ "	WHEN SPANNER_TYPE = 'STRUCT' THEN " + Types.STRUCT + " "
				+ "	WHEN SPANNER_TYPE = 'TIMESTAMP' THEN " + Types.TIMESTAMP + " " + "END AS DATA_TYPE, "
				+ "0 AS COLUMN_SIZE, NULL AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 'USAGE_UNKNOWN' AS COLUMN_USAGE, NULL AS REMARKS, 0 AS CHAR_OCTET_LENGTH, IS_NULLABLE "
				+ FROM_STATEMENT_WITHOUT_RESULTS;

		sql = sql + getCatalogSchemaTableWhereClause("T", catalog, schemaPattern, tableNamePattern);
		if (columnNamePattern != null)
			sql = sql + "AND UPPER(COLUMN_NAME) LIKE ? ";
		sql = sql + "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ";

		CloudSpannerPreparedStatement statement = prepareStatement(sql, catalog, schemaPattern, tableNamePattern,
				columnNamePattern);
		return statement.executeQuery();
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException
	{
		return false;
	}

}
