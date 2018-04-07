package nl.topicus.jdbc.test.integration;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.cloud.Timestamp;

import nl.topicus.jdbc.ICloudSpannerConnection;

/**
 * Test a variety of SELECT-statements
 * 
 * @author loite
 *
 */
public class SelectStatementsTester
{

	private Connection connection;

	public SelectStatementsTester(Connection connection)
	{
		this.connection = connection;
	}

	private enum BATCH_READ_ONLY_TEST
	{
		YES, NO;
	}

	public void runSelectTests() throws SQLException
	{
		// Turn off auto commit
		boolean wasAutocommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		connection.commit();
		for (BATCH_READ_ONLY_TEST batchReadOnly : BATCH_READ_ONLY_TEST.values())
		{
			connection.unwrap(ICloudSpannerConnection.class)
					.setBatchReadOnly(batchReadOnly == BATCH_READ_ONLY_TEST.YES);
			testSelect("SELECT * FROM TEST ORDER BY UUID");
			testSelect("SELECT * FROM TEST ORDER BY UUID DESC");
			testSelect("SELECT ID, UUID, LAST_UPDATED FROM TEST ORDER BY UUID DESC, ID");
			testSelect("SELECT * FROM TEST WHERE ID=?", 1L);
			testSelect("SELECT * FROM TEST ORDER BY ID LIMIT ? OFFSET ?", 2L, 10L);
			testSelect("SELECT `ID`, `UUID`, `LAST_UPDATED` FROM `TEST` ORDER BY `UUID` DESC, `ID`");
			testSelect("SELECT ID, UUID, LAST_UPDATED FROM TEST WHERE ID=? ORDER BY UUID DESC, ID", 1L);
			testSelect("SELECT * FROM TEST WHERE ID IN (SELECT CHILDID FROM TESTCHILD)", 1L);
			testSelect("SELECT * FROM TESTCHILD@{FORCE_INDEX=IDX_TESTCHILD_DESCRIPTION} WHERE DESCRIPTION LIKE ?",
					"%CHILD%");
			if (batchReadOnly == BATCH_READ_ONLY_TEST.YES)
			{
				Timestamp ts = connection.unwrap(ICloudSpannerConnection.class).getReadTimestamp();
				assertNotNull(ts);
			}
			connection.commit();
		}
		connection.setAutoCommit(wasAutocommit);
	}

	private void testSelect(String sql, Object... parameters) throws SQLException
	{
		PreparedStatement ps = connection.prepareStatement(sql);
		for (int i = 1; i <= parameters.length; i++)
		{
			ps.setObject(i, parameters[i - 1]);
		}
		try (ResultSet rs = ps.executeQuery())
		{
			ResultSetMetaData metadata = rs.getMetaData();
			for (int i = 1; i <= metadata.getColumnCount(); i++)
			{
				metadata.getColumnClassName(i);
				metadata.getColumnDisplaySize(i);
				metadata.getColumnLabel(i);
				metadata.getColumnName(i);
				metadata.getColumnType(i);
				metadata.getColumnTypeName(i);
				metadata.getPrecision(i);
				metadata.getScale(i);
				metadata.getCatalogName(i);
				metadata.getSchemaName(i);
				metadata.getTableName(i);
				metadata.isNullable(i);
				metadata.isAutoIncrement(i);
				metadata.isCaseSensitive(i);
				metadata.isCurrency(i);
				metadata.isDefinitelyWritable(i);
				metadata.isReadOnly(i);
				metadata.isSearchable(i);
				metadata.isSigned(i);
				metadata.isWritable(i);
			}
			while (rs.next())
			{
				// do nothing
			}
		}
	}

}
