package nl.topicus.jdbc.test.util;

import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.spanner.Partition;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;
import nl.topicus.jdbc.Logger;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;
import nl.topicus.jdbc.transaction.CloudSpannerTransaction;
import nl.topicus.jdbc.xa.CloudSpannerXAConnection;

public class CloudSpannerTestObjects
{

	public static CloudSpannerConnection createConnection() throws SQLException
	{
		CloudSpannerConnection connection = Mockito.mock(CloudSpannerConnection.class);
		Mockito.doCallRealMethod().when(connection).setAutoCommit(Mockito.anyBoolean());
		Mockito.when(connection.getAutoCommit()).thenCallRealMethod();
		connection.setAutoCommit(false);
		Mockito.doCallRealMethod().when(connection).setBatchReadOnly(Mockito.anyBoolean());
		Mockito.when(connection.isBatchReadOnly()).thenCallRealMethod();
		Mockito.doCallRealMethod().when(connection).setReadOnly(Mockito.anyBoolean());
		Mockito.when(connection.isReadOnly()).thenCallRealMethod();

		Mockito.when(connection.isAllowExtendedMode()).thenAnswer(new Returns(true));
		Mockito.when(connection.createArrayOf(Mockito.anyString(), Mockito.any())).thenCallRealMethod();
		CloudSpannerDatabaseMetaData metadata = createMetaData();
		Mockito.when(connection.getMetaData()).thenReturn(metadata);
		CloudSpannerTransaction transaction = Mockito.mock(CloudSpannerTransaction.class);
		Mockito.when(transaction.executeQuery(Mockito.any()))
				.thenReturn(Mockito.mock(com.google.cloud.spanner.ResultSet.class));
		Mockito.when(transaction.partitionQuery(Mockito.any(), Mockito.any()))
				.thenReturn(Arrays.asList(mock(Partition.class), mock(Partition.class), mock(Partition.class)));
		Mockito.when(connection.getTransaction()).thenReturn(transaction);

		TableKeyMetaData tableFoo = Mockito.mock(TableKeyMetaData.class);
		Mockito.when(tableFoo.getKeyColumns()).thenAnswer(new Returns(Arrays.asList("ID")));
		Mockito.when(connection
				.getTable(Mockito.matches(Pattern.compile("FOO", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE))))
				.thenAnswer(new Returns(tableFoo));

		TableKeyMetaData tableBar = Mockito.mock(TableKeyMetaData.class);
		Mockito.when(tableBar.getKeyColumns()).thenAnswer(new Returns(Arrays.asList("ID1", "ID2")));
		Mockito.when(connection
				.getTable(Mockito.matches(Pattern.compile("BAR", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE))))
				.thenAnswer(new Returns(tableBar));
		Mockito.when(connection.getLogger()).thenAnswer(new Returns(new Logger()));

		mockXAMethods(connection);

		return connection;
	}

	private static void mockXAMethods(CloudSpannerConnection connection) throws SQLException
	{
		String checkTable = null;
		try
		{
			Field field = CloudSpannerXAConnection.class.getDeclaredField("CHECK_TABLE_EXISTENCE");
			field.setAccessible(true);
			checkTable = (String) field.get(null);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		CloudSpannerPreparedStatement ps = Mockito.mock(CloudSpannerPreparedStatement.class);
		CloudSpannerResultSet rs = Mockito.mock(CloudSpannerResultSet.class);
		Mockito.when(rs.next()).thenReturn(true, false);
		Mockito.when(connection.prepareStatement(checkTable)).thenAnswer(new Returns(ps));
		Mockito.when(ps.executeQuery()).thenAnswer(new Returns(rs));
	}

	private static CloudSpannerDatabaseMetaData createMetaData() throws SQLException
	{
		CloudSpannerDatabaseMetaData metadata = Mockito.mock(CloudSpannerDatabaseMetaData.class);
		Mockito.when(metadata.getPrimaryKeys(Mockito.any(), Mockito.any(),
				Mockito.matches(Pattern.compile("FOO", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE))))
				.thenAnswer(new Answer<ResultSet>()
				{

					@Override
					public ResultSet answer(InvocationOnMock invocation) throws Throwable
					{
						CloudSpannerResultSet primaryKeyFoo = Mockito.mock(CloudSpannerResultSet.class);
						Mockito.when(primaryKeyFoo.next()).thenReturn(true, false);
						Mockito.when(primaryKeyFoo.getString("COLUMN_NAME")).thenReturn("ID");
						return primaryKeyFoo;
					}
				});

		Mockito.when(metadata.getPrimaryKeys(Mockito.any(), Mockito.any(),
				Mockito.matches(Pattern.compile("BAR", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE))))
				.thenAnswer(new Answer<ResultSet>()
				{

					@Override
					public ResultSet answer(InvocationOnMock invocation) throws Throwable
					{
						CloudSpannerResultSet primaryKeyBar = Mockito.mock(CloudSpannerResultSet.class);
						Mockito.when(primaryKeyBar.next()).thenReturn(true, true, false);
						Mockito.when(primaryKeyBar.getString("COLUMN_NAME")).thenReturn("ID1", "ID2");
						return primaryKeyBar;
					}
				});

		Mockito.when(metadata.getColumns(Mockito.any(), Mockito.any(),
				Mockito.matches(Pattern.compile("FOO", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE)),
				Mockito.any())).thenAnswer(new Answer<ResultSet>()
				{

					@Override
					public ResultSet answer(InvocationOnMock invocation) throws Throwable
					{
						CloudSpannerResultSet fooColumns = Mockito.mock(CloudSpannerResultSet.class);
						Mockito.when(fooColumns.next()).thenReturn(true, true, true, false);
						Mockito.when(fooColumns.getString("COLUMN_NAME")).thenReturn("ID", "COL1", "COL2");
						Mockito.when(fooColumns.getInt("COLUMN_SIZE")).thenReturn(8, 50, 100);
						Mockito.when(fooColumns.getInt("DATA_TYPE")).thenReturn(Types.BIGINT, Types.NVARCHAR,
								Types.NVARCHAR);
						Mockito.when(fooColumns.getInt("NULLABLE")).thenReturn(ResultSetMetaData.columnNoNulls,
								ResultSetMetaData.columnNoNulls, ResultSetMetaData.columnNullable);
						return fooColumns;
					}
				});

		Mockito.when(metadata.getIndexInfo(Mockito.any(), Mockito.any(),
				Mockito.matches(Pattern.compile("FOO", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE)),
				Mockito.anyBoolean(), Mockito.anyBoolean())).thenAnswer(new Answer<ResultSet>()
				{

					@Override
					public ResultSet answer(InvocationOnMock invocation) throws Throwable
					{
						CloudSpannerResultSet indices = Mockito.mock(CloudSpannerResultSet.class);
						Mockito.when(indices.next()).thenReturn(true, false);
						return indices;
					}
				});

		return metadata;
	}

	public static CloudSpannerPreparedStatement createPreparedStatement(String sql) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, createConnection(), null);
	}

}
