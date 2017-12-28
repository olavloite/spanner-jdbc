package nl.topicus.jdbc.statement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;

/**
 * 
 * @author loite
 *
 */
public class CloudSpannerStatement extends AbstractCloudSpannerStatement
{
	protected ResultSet lastResultSet = null;

	protected int lastUpdateCount = -1;

	private Pattern commentPattern = Pattern.compile("//.*|/\\*((.|\\n)(?!=*/))+\\*/|--.*(?=\\n)", Pattern.DOTALL);

	public CloudSpannerStatement(CloudSpannerConnection connection, DatabaseClient dbClient)
	{
		super(connection, dbClient);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		String[] sqlTokens = getTokens(sql);
		CustomDriverStatement custom = getCustomDriverStatement(sqlTokens);
		if (custom != null && custom.isQuery())
		{
			return custom.executeQuery(sqlTokens);
		}
		try (ReadContext context = getReadContext())
		{
			com.google.cloud.spanner.ResultSet rs = context.executeQuery(com.google.cloud.spanner.Statement.of(sql));
			return new CloudSpannerResultSet(this, rs);
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException
	{
		String[] sqlTokens = getTokens(sql);
		CustomDriverStatement custom = getCustomDriverStatement(sqlTokens);
		if (custom != null && !custom.isQuery())
		{
			return custom.executeUpdate(sqlTokens);
		}
		PreparedStatement ps = getConnection().prepareStatement(sql);
		return ps.executeUpdate();
	}

	@Override
	public boolean execute(String sql) throws SQLException
	{
		String[] sqlTokens = getTokens(sql);
		CustomDriverStatement custom = getCustomDriverStatement(sqlTokens);
		if (custom != null)
			return custom.execute(sqlTokens);
		Statement statement = null;
		boolean ddl = isDDLStatement(sqlTokens);
		if (!ddl)
		{
			try
			{
				statement = CCJSqlParserUtil.parse(sanitizeSQL(sql));
			}
			catch (JSQLParserException | TokenMgrError e)
			{
				throw new CloudSpannerSQLException(
						"Error while parsing sql statement " + sql + ": " + e.getLocalizedMessage(),
						Code.INVALID_ARGUMENT, e);
			}
		}
		if (!ddl && statement instanceof Select)
		{
			lastResultSet = executeQuery(sql);
			lastUpdateCount = -1;
			return true;
		}
		else
		{
			lastUpdateCount = executeUpdate(sql);
			lastResultSet = null;
			return false;
		}
	}

	private static final String[] DDL_STATEMENTS = { "CREATE", "ALTER", "DROP" };

	/**
	 * Do a quick check if this SQL statement is a DDL statement
	 * 
	 * @param sqlTokens
	 *            The statement to check
	 * @return true if the SQL statement is a DDL statement
	 */
	protected boolean isDDLStatement(String[] sqlTokens)
	{
		if (sqlTokens.length > 0)
		{
			for (String statement : DDL_STATEMENTS)
			{
				if (sqlTokens[0].equalsIgnoreCase(statement))
					return true;
			}
		}

		return false;
	}

	/**
	 * Remove comments from the given sql string and split it into parts based
	 * on all space characters
	 * 
	 * @param sql
	 * @return String array with all the parts of the sql statement
	 */
	protected String[] getTokens(String sql)
	{
		return getTokens(sql, 5);
	}

	/**
	 * Remove comments from the given sql string and split it into parts based
	 * on all space characters
	 * 
	 * @param sql
	 * @param limit
	 * @return String array with all the parts of the sql statement
	 */
	protected String[] getTokens(String sql, int limit)
	{
		String result = removeComments(sql);
		String generated = result.replaceFirst("=", " = ");
		return generated.split("\\s+", limit);
	}

	protected String removeComments(String sql)
	{
		return commentPattern.matcher(sql).replaceAll("").trim();
	}

	protected boolean isSelectStatement(String[] sqlTokens)
	{
		if (sqlTokens.length > 0 && sqlTokens[0].equalsIgnoreCase("SELECT"))
			return true;

		return false;
	}

	public abstract class CustomDriverStatement
	{
		private final String statement;

		private final boolean query;

		private CustomDriverStatement(String statement, boolean query)
		{
			this.statement = statement;
			this.query = query;
		}

		protected final boolean isQuery()
		{
			return query;
		}

		protected final boolean execute(String[] sqlTokens) throws SQLException
		{
			if (query)
			{
				lastResultSet = executeQuery(sqlTokens);
				lastUpdateCount = -1;
				return true;
			}
			else
			{
				lastResultSet = null;
				lastUpdateCount = executeUpdate(sqlTokens);
				return false;
			}
		}

		protected ResultSet executeQuery(String[] sqlTokens) throws SQLException
		{
			throw new IllegalArgumentException("This statement is not valid for execution as a query");
		}

		protected int executeUpdate(String[] sqlTokens) throws SQLException
		{
			throw new IllegalArgumentException("This statement is not valid for execution as an update");
		}
	}

	private class ShowDdlOperations extends CustomDriverStatement
	{
		private ShowDdlOperations()
		{
			super("SHOW_DDL_OPERATIONS", true);
		}

		@Override
		public ResultSet executeQuery(String[] sqlTokens) throws SQLException
		{
			return getConnection().getRunningDDLOperations(CloudSpannerStatement.this);
		}
	}

	private class CleanDdlOperations extends CustomDriverStatement
	{
		private CleanDdlOperations()
		{
			super("CLEAN_DDL_OPERATIONS", false);
		}

		@Override
		public int executeUpdate(String[] sqlTokens) throws SQLException
		{
			return getConnection().clearFinishedDDLOperations();
		}
	}

	private class SetDriverProperty extends CustomDriverStatement
	{
		private SetDriverProperty()
		{
			super("SET_DRIVER_PROPERTY", false);
		}

		@Override
		public int executeUpdate(String[] sqlTokens) throws SQLException
		{
			if (sqlTokens.length != 4 || !"=".equals(sqlTokens[2]))
				throw new CloudSpannerSQLException(
						"Invalid argument(s) for SET_DRIVER_PROPERTY. Expected \"SET_DRIVER_PROPERTY propertyName=propertyValue\"",
						Code.INVALID_ARGUMENT);
			return getConnection().setDynamicDriverProperty(sqlTokens[1], sqlTokens[3]);
		}
	}

	private class GetDriverProperty extends CustomDriverStatement
	{
		private GetDriverProperty()
		{
			super("GET_DRIVER_PROPERTY", true);
		}

		@Override
		public ResultSet executeQuery(String[] sqlTokens) throws SQLException
		{
			if (sqlTokens.length == 1)
				return getConnection().getDynamicDriverProperties(CloudSpannerStatement.this);
			if (sqlTokens.length == 2)
				return getConnection().getDynamicDriverProperty(CloudSpannerStatement.this, sqlTokens[1]);
			throw new CloudSpannerSQLException(
					"Invalid argument(s) for GET_DRIVER_PROPERTY. Expected \"GET_DRIVER_PROPERTY propertyName\" or \"GET_DRIVER_PROPERTY\"",
					Code.INVALID_ARGUMENT);
		}
	}

	private final List<CustomDriverStatement> CUSTOM_DRIVER_STATEMENTS = Arrays.asList(new ShowDdlOperations(),
			new CleanDdlOperations(), new SetDriverProperty(), new GetDriverProperty());

	/**
	 * Checks if a sql statement is a custom statement only recognized by this
	 * driver
	 * 
	 * @param sqlTokens
	 *            The statement to check
	 * @return The custom driver statement if the given statement is a custom
	 *         statement only recognized by the Cloud Spanner JDBC driver, such
	 *         as show_ddl_operations
	 */
	protected CustomDriverStatement getCustomDriverStatement(String[] sqlTokens)
	{
		if (sqlTokens.length > 0)
		{
			for (CustomDriverStatement statement : CUSTOM_DRIVER_STATEMENTS)
			{
				if (sqlTokens[0].equalsIgnoreCase(statement.statement))
				{
					return statement;
				}
			}
		}
		return null;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return lastResultSet;
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		return lastUpdateCount;
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		moveToNextResult(CLOSE_CURRENT_RESULT);
		return false;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException
	{
		moveToNextResult(current);
		return false;
	}

	private void moveToNextResult(int current) throws SQLException
	{
		if (current != java.sql.Statement.KEEP_CURRENT_RESULT && lastResultSet != null)
			lastResultSet.close();
		lastResultSet = null;
		lastUpdateCount = -1;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
