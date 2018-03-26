package nl.topicus.jdbc.test.integration.ddl;

import static org.junit.Assert.assertEquals;

import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Assert;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;

/**
 * Class for testing different MetaData methods of the JDBC driver.
 * 
 * @author loite
 *
 */
public class MetaDataTester
{
	private static final Logger log = Logger.getLogger(MetaDataTester.class.getName());

	private static final String[] TABLES = { "ASYNCTEST", "TEST", "TESTCHILD", "TEST_QUOTED", "TEST_WITH_ARRAY" };

	private static final String[] INDEXES = { "IDX_TEST_UUID", "IDX_TESTCHILD_DESCRIPTION" };

	private static final String[][] COLUMNS = {
			{ "ID", "UUID", "ACTIVE", "AMOUNT", "DESCRIPTION", "CREATED_DATE", "LAST_UPDATED" },
			{ "ID", "UUID", "ACTIVE", "AMOUNT", "DESCRIPTION", "CREATED_DATE", "LAST_UPDATED" },
			{ "ID", "CHILDID", "DESCRIPTION" },
			{ "Id", "UUID", "active", "Amount", "Description", "Created_Date", "Last_Updated" },
			{ "ID", "ID2", "UUID", "ACTIVE", "AMOUNT", "DESCRIPTION", "CREATED_DATE", "LAST_UPDATED" } };

	private static final int[][] COLUMN_TYPES = {
			{ Types.BIGINT, Types.BINARY, Types.BOOLEAN, Types.DOUBLE, Types.NVARCHAR, Types.DATE, Types.TIMESTAMP },
			{ Types.BIGINT, Types.BINARY, Types.BOOLEAN, Types.DOUBLE, Types.NVARCHAR, Types.DATE, Types.TIMESTAMP },
			{ Types.BIGINT, Types.BIGINT, Types.NVARCHAR },
			{ Types.BIGINT, Types.BINARY, Types.BOOLEAN, Types.DOUBLE, Types.NVARCHAR, Types.DATE, Types.TIMESTAMP },
			{ Types.BIGINT, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY,
					Types.ARRAY } };

	private static final Map<String, String[]> INDEX_COLUMNS = new HashMap<>();
	static
	{
		INDEX_COLUMNS.put("ASYNCTEST.PRIMARY_KEY", new String[] { "ID" });
		INDEX_COLUMNS.put("TEST.PRIMARY_KEY", new String[] { "ID" });
		INDEX_COLUMNS.put("TEST_QUOTED.PRIMARY_KEY", new String[] { "Id" });
		INDEX_COLUMNS.put("TESTCHILD.PRIMARY_KEY", new String[] { "ID", "CHILDID" });
		INDEX_COLUMNS.put("TEST.IDX_TEST_UUID", new String[] { "UUID" });
		INDEX_COLUMNS.put("TESTCHILD.IDX_TESTCHILD_DESCRIPTION", new String[] { "DESCRIPTION" });
		INDEX_COLUMNS.put("TEST_WITH_ARRAY.PRIMARY_KEY", new String[] { "ID" });
	}

	private static final Map<String, Boolean> INDEX_UNIQUE = new HashMap<>();
	static
	{
		INDEX_UNIQUE.put("ASYNCTEST.PRIMARY_KEY", Boolean.TRUE);
		INDEX_UNIQUE.put("TEST.PRIMARY_KEY", Boolean.TRUE);
		INDEX_UNIQUE.put("TESTCHILD.PRIMARY_KEY", Boolean.TRUE);
		INDEX_UNIQUE.put("TEST_QUOTED.PRIMARY_KEY", Boolean.TRUE);
		INDEX_UNIQUE.put("TEST.IDX_TEST_UUID", Boolean.TRUE);
		INDEX_UNIQUE.put("TESTCHILD.IDX_TESTCHILD_DESCRIPTION", Boolean.FALSE);
		INDEX_UNIQUE.put("TEST_WITH_ARRAY.PRIMARY_KEY", Boolean.TRUE);
	}

	private CloudSpannerConnection connection;

	public MetaDataTester(CloudSpannerConnection connection)
	{
		this.connection = connection;
	}

	public void runMetaDataTests() throws SQLException
	{
		log.info("Starting table meta data tests");
		runTableMetaDataTests();
		log.info("Starting column meta data tests");
		runColumnMetaDataTests();
		log.info("Starting index meta data tests");
		runIndexMetaDataTests();
		log.info("Starting other meta data tests");
		runOtherMetaDataTests();
		log.info("Starting parameter meta data tests");
		runParameterMetaDataTests();
		log.info("Finished meta data tests");
	}

	private void runTableMetaDataTests() throws SQLException
	{
		DatabaseMetaData metadata = connection.getMetaData();
		int count = 0;
		try (ResultSet tables = metadata.getTables("", "", null, null))
		{
			while (tables.next())
			{
				assertEquals(TABLES[count], tables.getString("TABLE_NAME"));
				assertEquals("TABLE", tables.getString("TABLE_TYPE"));
				count++;
			}
		}
		assertEquals(5, count);
	}

	private void runColumnMetaDataTests() throws SQLException
	{
		DatabaseMetaData metadata = connection.getMetaData();
		int tableIndex = 0;
		for (String table : TABLES)
		{
			int columnIndex = 0;
			try (ResultSet columns = metadata.getColumns("", "", table, null))
			{
				while (columns.next())
				{
					assertEquals(COLUMNS[tableIndex][columnIndex], columns.getString("COLUMN_NAME"));
					assertEquals(COLUMN_TYPES[tableIndex][columnIndex], columns.getInt("DATA_TYPE"));
					columnIndex++;
				}
			}
			tableIndex++;
		}
	}

	private void runIndexMetaDataTests() throws SQLException
	{
		String currentKey = "";
		DatabaseMetaData metadata = connection.getMetaData();
		for (String table : TABLES)
		{
			int columnIndex = 0;
			try (ResultSet indexes = metadata.getIndexInfo("", "", table, false, false))
			{
				while (indexes.next())
				{
					String key = indexes.getString("TABLE_NAME") + "." + indexes.getString("INDEX_NAME");
					if (!currentKey.equals(key))
					{
						columnIndex = 0;
						currentKey = key;
					}
					String[] columns = INDEX_COLUMNS.get(key);
					Boolean unique = INDEX_UNIQUE.get(key);
					Assert.assertNotNull(columns);
					assertEquals(columns[columnIndex], indexes.getString("COLUMN_NAME"));
					assertEquals(unique, !indexes.getBoolean("NON_UNIQUE"));
					columnIndex++;
				}
			}
		}
	}

	private void runOtherMetaDataTests() throws SQLException
	{
		CloudSpannerDatabaseMetaData metadata = connection.getMetaData();
		try (ResultSet rs = metadata.getCatalogs())
		{
		}
		try (ResultSet rs = metadata.getAttributes("", "", null, null))
		{
		}
		try (ResultSet rs = metadata.getClientInfoProperties())
		{
		}
		try (ResultSet rs = metadata.getFunctionColumns("", "", null, null))
		{
		}
		try (ResultSet rs = metadata.getFunctions("", "", null))
		{
		}
		try (ResultSet rs = metadata.getProcedureColumns("", "", null, null))
		{
		}
		try (ResultSet rs = metadata.getProcedures("", "", null))
		{
		}
		try (ResultSet rs = metadata.getSchemas())
		{
			assertNumberOfResults(rs, 2);
		}
		try (ResultSet rs = metadata.getSchemas("", null))
		{
			assertNumberOfResults(rs, 2);
		}
		try (ResultSet rs = metadata.getSchemas("", ""))
		{
			assertNumberOfResults(rs, 1);
		}
		try (ResultSet rs = metadata.getSchemas("", "INFORMATION_SCHEMA"))
		{
			assertNumberOfResults(rs, 1);
		}
		try (ResultSet rs = metadata.getSchemas("", "FOO"))
		{
			assertNumberOfResults(rs, 0);
		}
		try (ResultSet rs = metadata.getSuperTypes("", "", null))
		{
		}
		try (ResultSet rs = metadata.getTableTypes())
		{
		}
		try (ResultSet rs = metadata.getTypeInfo())
		{
		}
		try (ResultSet rs = metadata.getUDTs(null, null, null, null))
		{
		}
		for (String index : INDEXES)
		{
			try (ResultSet rs = metadata.getIndexInfo("", "", index))
			{
			}
		}
		for (String table : TABLES)
		{
			try (ResultSet rs = metadata.getIndexInfo("", "", table, true, true))
			{
			}
			try (ResultSet rs = metadata.getExportedKeys("", "", table))
			{
			}
			try (ResultSet rs = metadata.getImportedKeys("", "", table))
			{
			}
			try (ResultSet rs = metadata.getBestRowIdentifier("", "", table, DatabaseMetaData.bestRowTransaction,
					false))
			{
			}
			try (ResultSet rs = metadata.getColumnPrivileges("", "", table, null))
			{
			}
			try (ResultSet rs = metadata.getPrimaryKeys("", "", table))
			{
			}
			try (ResultSet rs = metadata.getPseudoColumns("", "", table, null))
			{
			}
			try (ResultSet rs = metadata.getPseudoColumns("", "", table, "FOO"))
			{
			}
			try (ResultSet rs = metadata.getSuperTables("", "", table))
			{
			}
			try (ResultSet rs = metadata.getTablePrivileges("", "", table))
			{
			}
			try (ResultSet rs = metadata.getVersionColumns("", "", table))
			{
			}
		}
	}

	private void assertNumberOfResults(ResultSet rs, int expectedCount) throws SQLException
	{
		int actualCount = 0;
		while (rs.next())
			actualCount++;
		assertEquals(expectedCount, actualCount);
	}

	private void runParameterMetaDataTests() throws SQLException
	{
		for (String sql : new String[] {
				"UPDATE `TEST` SET `UUID`=?, `ACTIVE`=?, `AMOUNT`=?, `DESCRIPTION`=?, `CREATED_DATE`=?, `LAST_UPDATED`=? WHERE `ID` = ?",
				"UPDATE `TEST` SET `UUID`=?, `ACTIVE`=?, `AMOUNT`=?, `DESCRIPTION`=?, `CREATED_DATE`=?, `LAST_UPDATED`=? WHERE `ID` > ?" })
		{
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			ParameterMetaData metadata = preparedStatement.getParameterMetaData();
			Assert.assertEquals(7, metadata.getParameterCount());
			Assert.assertEquals(Types.BINARY, metadata.getParameterType(1));
			Assert.assertEquals(Types.BOOLEAN, metadata.getParameterType(2));
			Assert.assertEquals(Types.DOUBLE, metadata.getParameterType(3));
			Assert.assertEquals(Types.NVARCHAR, metadata.getParameterType(4));
			Assert.assertEquals(Types.DATE, metadata.getParameterType(5));
			Assert.assertEquals(Types.TIMESTAMP, metadata.getParameterType(6));
			Assert.assertEquals(Types.BIGINT, metadata.getParameterType(7));
		}
		int delIndex = 0;
		int[] types = new int[] { Types.BIGINT, Types.NVARCHAR };
		for (String sql : new String[] { "DELETE FROM `TEST` WHERE `ID` = ?",
				"DELETE FROM `TEST` WHERE `DESCRIPTION` LIKE ?" })
		{
			PreparedStatement statement = connection.prepareStatement(sql);
			ParameterMetaData pmd = statement.getParameterMetaData();
			Assert.assertEquals(1, pmd.getParameterCount());
			Assert.assertEquals(types[delIndex], pmd.getParameterType(1));
			delIndex++;
		}
		for (String sql : new String[] {
				"INSERT INTO `TEST` (`UUID`, `ACTIVE`, `AMOUNT`, `DESCRIPTION`, `CREATED_DATE`, `LAST_UPDATED`, `ID`) VALUES (?, ?, ?, ?, ?, ?, ?)",
				"INSERT INTO `TEST` (`UUID`, `ACTIVE`, `AMOUNT`, `DESCRIPTION`, `CREATED_DATE`, `LAST_UPDATED`, `ID`) SELECT `UUID`, `ACTIVE`, `AMOUNT`, `DESCRIPTION`, `CREATED_DATE`, `LAST_UPDATED`, `ID` FROM TEST WHERE `UUID`=? AND `ACTIVE`=? AND `AMOUNT`=? AND `DESCRIPTION`=? AND `CREATED_DATE`=? AND `LAST_UPDATED`=? AND `ID` > ?" })
		{
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			ParameterMetaData metadata = preparedStatement.getParameterMetaData();
			Assert.assertEquals(7, metadata.getParameterCount());
			Assert.assertEquals(Types.BINARY, metadata.getParameterType(1));
			Assert.assertEquals(Types.BOOLEAN, metadata.getParameterType(2));
			Assert.assertEquals(Types.DOUBLE, metadata.getParameterType(3));
			Assert.assertEquals(Types.NVARCHAR, metadata.getParameterType(4));
			Assert.assertEquals(Types.DATE, metadata.getParameterType(5));
			Assert.assertEquals(Types.TIMESTAMP, metadata.getParameterType(6));
			Assert.assertEquals(Types.BIGINT, metadata.getParameterType(7));
		}
	}

}
