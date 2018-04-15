package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDatabaseMetaDataTest
{
	private static final String URL = "jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json;SimulateProductName=PostgreSQL";

	private final CloudSpannerDatabaseMetaData testSubject;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public CloudSpannerDatabaseMetaDataTest() throws SQLException
	{
		testSubject = createTestSubject(URL);
	}

	private CloudSpannerDatabaseMetaData createTestSubject(String url) throws SQLException
	{
		CloudSpannerConnection connection = MockCloudSpannerConnection.create(url);
		return new CloudSpannerDatabaseMetaData(connection);
	}

	@Test
	public void testAllProceduresAreCallable() throws SQLException
	{
		assertTrue(testSubject.allProceduresAreCallable());
	}

	@Test
	public void testAllTablesAreSelectable() throws SQLException
	{
		assertTrue(testSubject.allTablesAreSelectable());
	}

	@Test
	public void testGetURL() throws SQLException
	{
		assertEquals(URL, testSubject.getURL());
	}

	@Test
	public void testGetUserName() throws SQLException
	{
		assertNull(testSubject.getUserName());
	}

	@Test
	public void testIsReadOnly() throws SQLException
	{
		assertEquals(false, testSubject.isReadOnly());
	}

	@Test
	public void testNullsAreSortedHigh() throws SQLException
	{
		assertEquals(false, testSubject.nullsAreSortedHigh());
	}

	@Test
	public void testNullsAreSortedLow() throws SQLException
	{
		assertTrue(testSubject.nullsAreSortedLow());
	}

	@Test
	public void testNullsAreSortedAtStart() throws SQLException
	{
		assertTrue(testSubject.nullsAreSortedAtStart());
	}

	@Test
	public void testNullsAreSortedAtEnd() throws SQLException
	{
		assertEquals(false, testSubject.nullsAreSortedAtEnd());
	}

	@Test
	public void testGetDatabaseProductName() throws SQLException
	{
		assertEquals("PostgreSQL", testSubject.getDatabaseProductName());
	}

	@Test
	public void testGetDatabaseProductVersion() throws SQLException
	{
		assertEquals("1.0", testSubject.getDatabaseProductVersion());
	}

	@Test
	public void testGetDriverName() throws SQLException
	{
		assertEquals(CloudSpannerDriver.class.getName(), testSubject.getDriverName());
	}

	@Test
	public void testGetDriverVersion() throws SQLException
	{
		assertEquals(CloudSpannerDriver.MAJOR_VERSION + "." + CloudSpannerDriver.MINOR_VERSION,
				testSubject.getDriverVersion());
	}

	@Test
	public void testGetDriverMajorVersion()
	{
		assertEquals(CloudSpannerDriver.MAJOR_VERSION, testSubject.getDriverMajorVersion());
	}

	@Test
	public void testGetDriverMinorVersion()
	{
		assertEquals(CloudSpannerDriver.MINOR_VERSION, testSubject.getDriverMinorVersion());
	}

	@Test
	public void testUsesLocalFiles() throws SQLException
	{
		assertEquals(false, testSubject.usesLocalFiles());
	}

	@Test
	public void testUsesLocalFilePerTable() throws SQLException
	{
		assertEquals(false, testSubject.usesLocalFilePerTable());
	}

	@Test
	public void testSupportsMixedCaseIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.supportsMixedCaseIdentifiers());
	}

	@Test
	public void testStoresUpperCaseIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.storesUpperCaseIdentifiers());
	}

	@Test
	public void testStoresLowerCaseIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.storesLowerCaseIdentifiers());
	}

	@Test
	public void testStoresMixedCaseIdentifiers() throws SQLException
	{
		assertTrue(testSubject.storesMixedCaseIdentifiers());
	}

	@Test
	public void testSupportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.supportsMixedCaseQuotedIdentifiers());
	}

	@Test
	public void testStoresUpperCaseQuotedIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.storesUpperCaseQuotedIdentifiers());
	}

	@Test
	public void testStoresLowerCaseQuotedIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.storesLowerCaseQuotedIdentifiers());
	}

	@Test
	public void testStoresMixedCaseQuotedIdentifiers() throws SQLException
	{
		assertTrue(testSubject.storesMixedCaseQuotedIdentifiers());
	}

	@Test
	public void testGetIdentifierQuoteString() throws SQLException
	{
		assertEquals("`", testSubject.getIdentifierQuoteString());
	}

	@Test
	public void testGetSQLKeywords() throws SQLException
	{
		assertEquals("INTERLEAVE, PARENT", testSubject.getSQLKeywords());
	}

	@Test
	public void testGetNumericFunctions() throws SQLException
	{
		assertEquals("", testSubject.getNumericFunctions());
	}

	@Test
	public void testGetStringFunctions() throws SQLException
	{
		assertEquals("", testSubject.getStringFunctions());
	}

	@Test
	public void testGetSystemFunctions() throws SQLException
	{
		assertEquals("", testSubject.getSystemFunctions());
	}

	@Test
	public void testGetTimeDateFunctions() throws SQLException
	{
		assertEquals("", testSubject.getTimeDateFunctions());
	}

	@Test
	public void testGetSearchStringEscape() throws SQLException
	{
		assertEquals("\\", testSubject.getSearchStringEscape());
	}

	@Test
	public void testGetExtraNameCharacters() throws SQLException
	{
		assertEquals("", testSubject.getExtraNameCharacters());
	}

	@Test
	public void testSupportsAlterTableWithAddColumn() throws SQLException
	{
		assertTrue(testSubject.supportsAlterTableWithAddColumn());
	}

	@Test
	public void testSupportsAlterTableWithDropColumn() throws SQLException
	{
		assertTrue(testSubject.supportsAlterTableWithDropColumn());
	}

	@Test
	public void testSupportsColumnAliasing() throws SQLException
	{
		assertTrue(testSubject.supportsColumnAliasing());
	}

	@Test
	public void testNullPlusNonNullIsNull() throws SQLException
	{
		assertTrue(testSubject.nullPlusNonNullIsNull());
	}

	@Test
	public void testSupportsConvert() throws SQLException
	{
		assertEquals(false, testSubject.supportsConvert());
	}

	@Test
	public void testSupportsConvertWithParameters() throws SQLException
	{
		assertEquals(false, testSubject.supportsConvert(Types.BOOLEAN, Types.BIT));
	}

	@Test
	public void testSupportsTableCorrelationNames() throws SQLException
	{
		assertTrue(testSubject.supportsTableCorrelationNames());
	}

	@Test
	public void testSupportsDifferentTableCorrelationNames() throws SQLException
	{
		assertEquals(false, testSubject.supportsDifferentTableCorrelationNames());
	}

	@Test
	public void testSupportsExpressionsInOrderBy() throws SQLException
	{
		assertTrue(testSubject.supportsExpressionsInOrderBy());
	}

	@Test
	public void testSupportsOrderByUnrelated() throws SQLException
	{
		assertTrue(testSubject.supportsOrderByUnrelated());
	}

	@Test
	public void testSupportsGroupBy() throws SQLException
	{
		assertTrue(testSubject.supportsGroupBy());
	}

	@Test
	public void testUnwrap() throws SQLException
	{
		assertTrue(testSubject.isWrapperFor(DatabaseMetaData.class));
		assertEquals(testSubject, testSubject.unwrap(DatabaseMetaData.class));
	}

	@Test
	public void testSupportsGroupByUnrelated() throws SQLException
	{
		assertTrue(testSubject.supportsGroupByUnrelated());
	}

	@Test
	public void testSupportsGroupByBeyondSelect() throws SQLException
	{
		assertTrue(testSubject.supportsGroupByBeyondSelect());
	}

	@Test
	public void testSupportsLikeEscapeClause() throws SQLException
	{
		assertTrue(testSubject.supportsLikeEscapeClause());
	}

	@Test
	public void testSupportsMultipleResultSets() throws SQLException
	{
		assertTrue(testSubject.supportsMultipleResultSets());
	}

	@Test
	public void testSupportsMultipleTransactions() throws SQLException
	{
		assertTrue(testSubject.supportsMultipleTransactions());
	}

	@Test
	public void testSupportsNonNullableColumns() throws SQLException
	{
		assertTrue(testSubject.supportsNonNullableColumns());
	}

	@Test
	public void testSupportsMinimumSQLGrammar() throws SQLException
	{
		assertEquals(false, testSubject.supportsMinimumSQLGrammar());
	}

	@Test
	public void testSupportsCoreSQLGrammar() throws SQLException
	{
		assertEquals(false, testSubject.supportsCoreSQLGrammar());
	}

	@Test
	public void testSupportsExtendedSQLGrammar() throws SQLException
	{
		assertEquals(false, testSubject.supportsExtendedSQLGrammar());
	}

	@Test
	public void testSupportsANSI92EntryLevelSQL() throws SQLException
	{
		assertEquals(false, testSubject.supportsANSI92EntryLevelSQL());
	}

	@Test
	public void testSupportsANSI92IntermediateSQL() throws SQLException
	{
		assertEquals(false, testSubject.supportsANSI92IntermediateSQL());
	}

	@Test
	public void testSupportsANSI92FullSQL() throws SQLException
	{
		assertEquals(false, testSubject.supportsANSI92FullSQL());
	}

	@Test
	public void testSupportsIntegrityEnhancementFacility() throws SQLException
	{
		assertEquals(false, testSubject.supportsIntegrityEnhancementFacility());
	}

	@Test
	public void testSupportsOuterJoins() throws SQLException
	{
		assertTrue(testSubject.supportsOuterJoins());
	}

	@Test
	public void testSupportsFullOuterJoins() throws SQLException
	{
		assertTrue(testSubject.supportsFullOuterJoins());
	}

	@Test
	public void testSupportsLimitedOuterJoins() throws SQLException
	{
		assertTrue(testSubject.supportsLimitedOuterJoins());
	}

	@Test
	public void testGetSchemaTerm() throws SQLException
	{
		assertNull(testSubject.getSchemaTerm());
	}

	@Test
	public void testGetProcedureTerm() throws SQLException
	{
		assertNull(testSubject.getProcedureTerm());
	}

	@Test
	public void testGetCatalogTerm() throws SQLException
	{
		assertNull(testSubject.getCatalogTerm());
	}

	@Test
	public void testIsCatalogAtStart() throws SQLException
	{
		assertEquals(false, testSubject.isCatalogAtStart());
	}

	@Test
	public void testGetCatalogSeparator() throws SQLException
	{
		assertNull(testSubject.getCatalogSeparator());
	}

	@Test
	public void testSupportsSchemasInDataManipulation() throws SQLException
	{
		assertEquals(false, testSubject.supportsSchemasInDataManipulation());
	}

	@Test
	public void testSupportsSchemasInProcedureCalls() throws SQLException
	{
		assertEquals(false, testSubject.supportsSchemasInProcedureCalls());
	}

	@Test
	public void testSupportsSchemasInTableDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsSchemasInTableDefinitions());
	}

	@Test
	public void testSupportsSchemasInIndexDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsSchemasInIndexDefinitions());
	}

	@Test
	public void testSupportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsSchemasInPrivilegeDefinitions());
	}

	@Test
	public void testSupportsCatalogsInDataManipulation() throws SQLException
	{
		assertEquals(false, testSubject.supportsCatalogsInDataManipulation());
	}

	@Test
	public void testSupportsCatalogsInProcedureCalls() throws SQLException
	{
		assertEquals(false, testSubject.supportsCatalogsInProcedureCalls());
	}

	@Test
	public void testSupportsCatalogsInTableDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsCatalogsInTableDefinitions());
	}

	@Test
	public void testSupportsCatalogsInIndexDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsCatalogsInIndexDefinitions());
	}

	@Test
	public void testSupportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		assertEquals(false, testSubject.supportsCatalogsInPrivilegeDefinitions());
	}

	@Test
	public void testSupportsPositionedDelete() throws SQLException
	{
		assertEquals(false, testSubject.supportsPositionedDelete());
	}

	@Test
	public void testSupportsPositionedUpdate() throws SQLException
	{
		assertEquals(false, testSubject.supportsPositionedUpdate());
	}

	@Test
	public void testSupportsSelectForUpdate() throws SQLException
	{
		assertEquals(false, testSubject.supportsSelectForUpdate());
	}

	@Test
	public void testSupportsStoredProcedures() throws SQLException
	{
		assertEquals(false, testSubject.supportsStoredProcedures());
	}

	@Test
	public void testSupportsSubqueriesInComparisons() throws SQLException
	{
		assertTrue(testSubject.supportsSubqueriesInComparisons());
	}

	@Test
	public void testSupportsSubqueriesInExists() throws SQLException
	{
		assertTrue(testSubject.supportsSubqueriesInExists());
	}

	@Test
	public void testSupportsSubqueriesInIns() throws SQLException
	{
		assertTrue(testSubject.supportsSubqueriesInIns());
	}

	@Test
	public void testSupportsSubqueriesInQuantifieds() throws SQLException
	{
		assertTrue(testSubject.supportsSubqueriesInQuantifieds());
	}

	@Test
	public void testSupportsCorrelatedSubqueries() throws SQLException
	{
		assertTrue(testSubject.supportsCorrelatedSubqueries());
	}

	@Test
	public void testSupportsUnion() throws SQLException
	{
		assertTrue(testSubject.supportsUnion());
	}

	@Test
	public void testSupportsUnionAll() throws SQLException
	{
		assertTrue(testSubject.supportsUnionAll());
	}

	@Test
	public void testSupportsOpenCursorsAcrossCommit() throws SQLException
	{
		assertFalse(testSubject.supportsOpenCursorsAcrossCommit());
	}

	@Test
	public void testSupportsOpenCursorsAcrossRollback() throws SQLException
	{
		assertFalse(testSubject.supportsOpenCursorsAcrossRollback());
	}

	@Test
	public void testSupportsOpenStatementsAcrossCommit() throws SQLException
	{
		assertTrue(testSubject.supportsOpenStatementsAcrossCommit());
	}

	@Test
	public void testSupportsOpenStatementsAcrossRollback() throws SQLException
	{
		assertTrue(testSubject.supportsOpenStatementsAcrossRollback());
	}

	@Test
	public void testGetMaxBinaryLiteralLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxBinaryLiteralLength());
	}

	@Test
	public void testGetMaxCharLiteralLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxCharLiteralLength());
	}

	@Test
	public void testGetMaxColumnNameLength() throws SQLException
	{
		assertEquals(128, testSubject.getMaxColumnNameLength());
	}

	@Test
	public void testGetMaxColumnsInGroupBy() throws SQLException
	{
		assertEquals(0, testSubject.getMaxColumnsInGroupBy());
	}

	@Test
	public void testGetMaxColumnsInIndex() throws SQLException
	{
		assertEquals(16, testSubject.getMaxColumnsInIndex());
	}

	@Test
	public void testGetMaxColumnsInOrderBy() throws SQLException
	{
		assertEquals(0, testSubject.getMaxColumnsInOrderBy());
	}

	@Test
	public void testGetMaxColumnsInSelect() throws SQLException
	{
		assertEquals(0, testSubject.getMaxColumnsInSelect());
	}

	@Test
	public void testGetMaxColumnsInTable() throws SQLException
	{
		assertEquals(1024, testSubject.getMaxColumnsInTable());
	}

	@Test
	public void testGetMaxConnections() throws SQLException
	{
		assertEquals(10000, testSubject.getMaxConnections());
	}

	@Test
	public void testGetMaxCursorNameLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxCursorNameLength());
	}

	@Test
	public void testGetMaxIndexLength() throws SQLException
	{
		assertEquals(8000, testSubject.getMaxIndexLength());
	}

	@Test
	public void testGetMaxSchemaNameLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxSchemaNameLength());
	}

	@Test
	public void testGetMaxProcedureNameLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxProcedureNameLength());
	}

	@Test
	public void testGetMaxCatalogNameLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxCatalogNameLength());
	}

	@Test
	public void testGetMaxRowSize() throws SQLException
	{
		assertEquals(1024 * 10000000, testSubject.getMaxRowSize());
	}

	@Test
	public void testGoesMaxRowSizeIncludeBlobs() throws SQLException
	{
		assertTrue(testSubject.doesMaxRowSizeIncludeBlobs());
	}

	@Test
	public void testGetMaxStatementLength() throws SQLException
	{
		assertEquals(1000000, testSubject.getMaxStatementLength());
	}

	@Test
	public void testGetMaxStatements() throws SQLException
	{
		assertEquals(0, testSubject.getMaxStatements());
	}

	@Test
	public void testGetMaxTableNameLength() throws SQLException
	{
		assertEquals(128, testSubject.getMaxTableNameLength());
	}

	@Test
	public void testGetMaxTablesInSelect() throws SQLException
	{
		assertEquals(0, testSubject.getMaxTablesInSelect());
	}

	@Test
	public void testGetMaxUserNameLength() throws SQLException
	{
		assertEquals(0, testSubject.getMaxUserNameLength());
	}

	@Test
	public void testGetDefaultTransactionIsolation() throws SQLException
	{
		assertEquals(Connection.TRANSACTION_SERIALIZABLE, testSubject.getDefaultTransactionIsolation());
	}

	@Test
	public void testSupportsTransactions() throws SQLException
	{
		assertTrue(testSubject.supportsTransactions());
	}

	@Test
	public void testSupportsTransactionIsolationLevel() throws SQLException
	{
		assertFalse(testSubject.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
		assertFalse(testSubject.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
		assertFalse(testSubject.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
		assertTrue(testSubject.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
	}

	@Test
	public void testSupportsDataDefinitionAndDataManipulationTransactions() throws SQLException
	{
		assertFalse(testSubject.supportsDataDefinitionAndDataManipulationTransactions());
	}

	@Test
	public void testSupportsDataManipulationTransactionsOnly() throws SQLException
	{
		assertFalse(testSubject.supportsDataManipulationTransactionsOnly());
	}

	@Test
	public void testDataDefinitionCausesTransactionCommit() throws SQLException
	{
		assertFalse(testSubject.dataDefinitionCausesTransactionCommit());
	}

	@Test
	public void testDataDefinitionIgnoredInTransactions() throws SQLException
	{
		assertFalse(testSubject.dataDefinitionIgnoredInTransactions());
	}

	@Test
	public void testGetCrossReference() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubject.getCrossReference("", "", "foo", "test", "test", "bar");
	}

	@Test
	public void testSupportsResultSetType() throws SQLException
	{
		assertTrue(testSubject.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testSupportsResultSetConcurrency() throws SQLException
	{
		assertTrue(testSubject.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
		assertFalse(testSubject.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
		assertFalse(testSubject.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY));
		assertFalse(testSubject.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_UPDATABLE));
		assertFalse(
				testSubject.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
		assertFalse(
				testSubject.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));
	}

	@Test
	public void testOwnUpdatesAreVisible() throws SQLException
	{
		assertFalse(testSubject.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testOwnDeletesAreVisible() throws SQLException
	{
		assertFalse(testSubject.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testOwnInsertsAreVisible() throws SQLException
	{
		assertFalse(testSubject.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testOthersUpdatesAreVisible() throws SQLException
	{
		assertFalse(testSubject.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testOthersDeletesAreVisible() throws SQLException
	{
		assertFalse(testSubject.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testOthersInsertsAreVisible() throws SQLException
	{
		assertFalse(testSubject.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testUpdatesAreDetected() throws SQLException
	{
		assertFalse(testSubject.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testDeletesAreDetected() throws SQLException
	{
		assertFalse(testSubject.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testInsertsAreDetected() throws SQLException
	{
		assertFalse(testSubject.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(testSubject.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(testSubject.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	public void testSupportsBatchUpdates() throws SQLException
	{
		assertTrue(testSubject.supportsBatchUpdates());
	}

	@Test
	public void testGetConnection() throws SQLException
	{
		assertNotNull(testSubject.getConnection());
	}

	@Test
	public void testSupportsSavePoints() throws SQLException
	{
		assertTrue(testSubject.supportsSavepoints());
		// Check that the connection does not throw an Exception
		@SuppressWarnings("resource")
		CloudSpannerConnection connection = new CloudSpannerConnection(mock(DatabaseClient.class),
				mock(BatchClient.class));
		connection.setAutoCommit(false);
		connection.setSavepoint();
	}

	@Test
	public void testSupportsNamedParameters() throws SQLException
	{
		assertFalse(testSubject.supportsNamedParameters());
	}

	@Test
	public void testSupportsMultipleOpenResults() throws SQLException
	{
		assertFalse(testSubject.supportsMultipleOpenResults());
	}

	@Test
	public void testSupportsGetGeneratedKeys() throws SQLException
	{
		assertFalse(testSubject.supportsGetGeneratedKeys());
	}

	@Test
	public void testSupportsResultSetHoldability() throws SQLException
	{
		assertTrue(testSubject.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
		assertFalse(testSubject.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
	}

	@Test
	public void testGetResultSetHoldability() throws SQLException
	{
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, testSubject.getResultSetHoldability());
	}

	@Test
	public void testGetJDBCVersion() throws SQLException
	{
		assertEquals(4, testSubject.getJDBCMajorVersion());
		assertEquals(2, testSubject.getJDBCMinorVersion());
	}

	@Test
	public void testGetSqlStateType() throws SQLException
	{
		assertEquals(DatabaseMetaData.sqlStateSQL, testSubject.getSQLStateType());
	}

	@Test
	public void testLocatorsUpdateCopy() throws SQLException
	{
		assertTrue(testSubject.locatorsUpdateCopy());
	}

	@Test
	public void testSupportsStatementPooling() throws SQLException
	{
		assertFalse(testSubject.supportsStatementPooling());
	}

	@Test
	public void testGetRowIdLifetime() throws SQLException
	{
		assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, testSubject.getRowIdLifetime());
	}

	@Test
	public void testSupportsStoredFunctionsUsingCallSyntax() throws SQLException
	{
		assertFalse(testSubject.supportsStoredFunctionsUsingCallSyntax());
	}

	@Test
	public void testAutoCommitFailureClosesAllResultSets() throws SQLException
	{
		assertFalse(testSubject.autoCommitFailureClosesAllResultSets());
	}

	@Test
	public void testGeneratedKeyAlwaysReturned() throws SQLException
	{
		assertFalse(testSubject.generatedKeyAlwaysReturned());
	}
}