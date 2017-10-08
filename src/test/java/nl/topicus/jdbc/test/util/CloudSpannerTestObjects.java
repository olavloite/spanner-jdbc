package nl.topicus.jdbc.test.util;

import java.sql.DatabaseMetaData;
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

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;
import nl.topicus.jdbc.transaction.CloudSpannerTransaction;

public class CloudSpannerTestObjects
{

	public static CloudSpannerConnection createConnection() throws SQLException
	{
		CloudSpannerConnection connection = Mockito.mock(CloudSpannerConnection.class);
		Mockito.when(connection.isAllowExtendedMode()).thenAnswer(new Returns(true));
		Mockito.when(connection.createArrayOf(Mockito.anyString(), Mockito.any())).thenCallRealMethod();
		DatabaseMetaData metadata = createMetaData();
		Mockito.when(connection.getMetaData()).thenReturn(metadata);
		CloudSpannerTransaction transaction = Mockito.mock(CloudSpannerTransaction.class);
		Mockito.when(transaction.executeQuery(Mockito.any()))
				.thenReturn(Mockito.mock(com.google.cloud.spanner.ResultSet.class));
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

		return connection;
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
