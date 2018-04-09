package nl.topicus.jdbc.emulator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;

public abstract class AbstractReadContextEmulator implements ReadContext
{
	private static final String METHOD_NOT_IMPLEMENTED = "Method not implemented";

	private final Connection connection;

	AbstractReadContextEmulator(String url)
	{
		try
		{
			connection = DriverManager.getConnection(url);
			connection.setAutoCommit(false);
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN,
					"Could not open JDBC connection: " + e.getMessage(), e);
		}
	}

	Connection getConnection()
	{
		return connection;
	}

	@Override
	public void close()
	{
		try
		{
			connection.close();
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNIMPLEMENTED,
					"Could not open JDBC connection: " + e.getMessage(), e);
		}
	}

	@Override
	public ResultSet read(String table, KeySet keys, Iterable<String> columns, ReadOption... options)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

	@Override
	public ResultSet readUsingIndex(String table, String index, KeySet keys, Iterable<String> columns,
			ReadOption... options)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

	@Override
	public Struct readRow(String table, Key key, Iterable<String> columns)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

	@Override
	public Struct readRowUsingIndex(String table, String index, Key key, Iterable<String> columns)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

	@Override
	public ResultSet executeQuery(Statement statement, QueryOption... options)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

	@Override
	public ResultSet analyzeQuery(Statement statement, QueryAnalyzeMode queryMode)
	{
		throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, METHOD_NOT_IMPLEMENTED);
	}

}
