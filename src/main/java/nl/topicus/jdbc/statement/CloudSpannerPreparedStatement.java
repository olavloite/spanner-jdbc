package nl.topicus.jdbc.statement;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ReadContext;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;

/**
 * 
 * @author loite
 *
 */
public class CloudSpannerPreparedStatement extends AbstractCloudSpannerPreparedStatement
{
	private static final String INVALID_WHERE_CLAUSE_DELETE_MESSAGE = "The DELETE statement does not contain a valid WHERE clause. DELETE statements must contain a WHERE clause specifying the value of the primary key of the record(s) to be deleted in the form 'ID=value' or 'ID1=value1 AND ID2=value2'";

	private static final String INVALID_WHERE_CLAUSE_UPDATE_MESSAGE = "The UPDATE statement does not contain a valid WHERE clause. UPDATE statements must contain a WHERE clause specifying the value of the primary key of the record(s) to be deleted in the form 'ID=value' or 'ID1=value1 AND ID2=value2'";

	private static final String PARSE_ERROR = "Error while parsing sql statement ";

	private String sql;

	private List<Mutation> batchMutations = new ArrayList<>();

	public CloudSpannerPreparedStatement(String sql, CloudSpannerConnection connection, DatabaseClient dbClient)
	{
		super(connection, dbClient);
		this.sql = sql;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		throw new SQLException("The executeQuery(String sql)-method may not be called on a PreparedStatement");
	}

	@Override
	public ResultSet executeQuery() throws SQLException
	{
		Statement statement;
		try
		{
			statement = CCJSqlParserUtil.parse(sql);
		}
		catch (JSQLParserException | TokenMgrError e)
		{
			throw new SQLException(PARSE_ERROR + sql + ": " + e.getLocalizedMessage(), e);
		}
		if (statement instanceof Select)
		{
			com.google.cloud.spanner.Statement.Builder builder = createSelectBuilder(statement);
			try (ReadContext context = getReadContext())
			{
				com.google.cloud.spanner.ResultSet rs = context.executeQuery(builder.build());
				return new CloudSpannerResultSet(rs);
			}
		}
		throw new SQLException("SQL statement not suitable for executeQuery. Expected SELECT-statement.");
	}

	private com.google.cloud.spanner.Statement.Builder createSelectBuilder(Statement statement)
	{
		String namedSql = convertPositionalParametersToNamedParameters(sql);
		com.google.cloud.spanner.Statement.Builder builder = com.google.cloud.spanner.Statement.newBuilder(namedSql);
		setSelectParameters(((Select) statement).getSelectBody(), builder);

		return builder;
	}

	private String convertPositionalParametersToNamedParameters(String sql)
	{
		boolean inString = false;
		StringBuilder res = new StringBuilder(sql);
		int i = 0;
		int parIndex = 1;
		while (i < res.length())
		{
			char c = res.charAt(i);
			if (c == '\'')
			{
				inString = !inString;
			}
			else if (c == '?' && !inString)
			{
				res.replace(i, i + 1, "@p" + parIndex);
				parIndex++;
			}
			i++;
		}

		return res.toString();
	}

	private void setSelectParameters(SelectBody body, com.google.cloud.spanner.Statement.Builder builder)
	{
		body.accept(new SelectVisitorAdapter()
		{
			@Override
			public void visit(PlainSelect plainSelect)
			{
				setWhereParameters(plainSelect.getWhere(), builder);
				if (plainSelect.getLimit() != null)
				{
					setWhereParameters(plainSelect.getLimit().getRowCount(), builder);
				}
				if (plainSelect.getOffset() != null && plainSelect.getOffset().isOffsetJdbcParameter())
				{
					ValueBinderExpressionVisitorAdapter<com.google.cloud.spanner.Statement.Builder> binder = new ValueBinderExpressionVisitorAdapter<>(
							getParameterStore(), builder.bind("p" + getParameterStore().getHighestIndex()), null);
					binder.setValue(getParameterStore().getParameter(getParameterStore().getHighestIndex()));
					getParameterStore().setType(getParameterStore().getHighestIndex(), Types.BIGINT);
				}
			}
		});
	}

	private void setWhereParameters(Expression where, com.google.cloud.spanner.Statement.Builder builder)
	{
		if (where != null)
		{
			where.accept(new ExpressionVisitorAdapter()
			{

				@Override
				public void visit(JdbcParameter parameter)
				{
					parameter.accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
							builder.bind("p" + parameter.getIndex()), null));
				}

				@Override
				public void visit(SubSelect subSelect)
				{
					setSelectParameters(subSelect.getSelectBody(), builder);
				}

			});
		}
	}

	@Override
	public void addBatch() throws SQLException
	{
		if (getConnection().getAutoCommit())
		{
			throw new SQLFeatureNotSupportedException(
					"Batching of statements is only allowed when not running in autocommit mode");
		}
		if (isDDLStatement(sql))
		{
			throw new SQLFeatureNotSupportedException("DDL statements may not be batched");
		}
		Mutation mutation = createMutation();
		batchMutations.add(mutation);
		getParameterStore().clearParameters();
	}

	@Override
	public void clearBatch() throws SQLException
	{
		batchMutations.clear();
		getParameterStore().clearParameters();
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		int[] res = new int[batchMutations.size()];
		int index = 0;
		for (Mutation mutation : batchMutations)
		{
			res[index] = writeMutation(mutation);
			index++;
		}
		batchMutations.clear();
		getParameterStore().clearParameters();
		return res;
	}

	@Override
	public int executeUpdate() throws SQLException
	{
		if (isDDLStatement(sql))
		{
			String ddl = formatDDLStatement(sql);
			return executeDDL(ddl);
		}
		return writeMutation(createMutation());
	}

	private Mutation createMutation() throws SQLException
	{
		try
		{
			if (isDDLStatement(sql))
			{
				throw new SQLException("Cannot create mutation for DDL statement. Expected INSERT, UPDATE or DELETE");
			}
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement instanceof Insert)
			{
				return createInsertMutation((Insert) statement);
			}
			else if (statement instanceof Update)
			{
				return createUpdateMutation((Update) statement);
			}
			else if (statement instanceof Delete)
			{
				return createDeleteMutation((Delete) statement);
			}
			else
			{
				throw new SQLFeatureNotSupportedException(
						"Unrecognized or unsupported SQL-statment: Expected one of INSERT, UPDATE or DELETE. Please note that batching of prepared statements is not supported for SELECT-statements.");
			}
		}
		catch (JSQLParserException | IllegalArgumentException | TokenMgrError e)
		{
			throw new SQLException(PARSE_ERROR + sql + ": " + e.getLocalizedMessage(), e);
		}
	}

	/**
	 * Does some formatting to DDL statements that might have been generated by
	 * standard SQL generators to make it compatible with Google Cloud Spanner.
	 * 
	 * @param sql
	 *            The sql to format
	 * @return The formatted DDL statement.
	 */
	private String formatDDLStatement(String sql)
	{
		String res = sql.trim().toUpperCase();
		String[] parts = res.split("\\s+");
		if (parts.length >= 2)
		{
			String sqlWithSingleSpaces = String.join(" ", parts);
			if (sqlWithSingleSpaces.startsWith("CREATE TABLE"))
			{
				int primaryKeyIndex = res.indexOf(", PRIMARY KEY (");
				if (primaryKeyIndex > -1)
				{
					int endPrimaryKeyIndex = res.indexOf(')', primaryKeyIndex);
					String primaryKeySpec = res.substring(primaryKeyIndex + 2, endPrimaryKeyIndex + 1);
					res = res.replace(", " + primaryKeySpec, "");
					res = res + " " + primaryKeySpec;
				}
			}
		}

		return res;
	}

	private Mutation createInsertMutation(Insert insert) throws SQLException
	{
		ItemsList items = insert.getItemsList();
		if (!(items instanceof ExpressionList))
		{
			throw new SQLException("Insert statement must contain a list of values");
		}
		List<Expression> expressions = ((ExpressionList) items).getExpressions();
		String table = unquoteIdentifier(insert.getTable().getFullyQualifiedName());
		getParameterStore().setTable(table);
		WriteBuilder builder = Mutation.newInsertBuilder(table);
		int index = 0;
		for (Column col : insert.getColumns())
		{
			String columnName = unquoteIdentifier(col.getFullyQualifiedName());
			expressions.get(index).accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
					builder.set(columnName), columnName));
			index++;
		}
		return builder.build();
	}

	private Mutation createUpdateMutation(Update update) throws SQLException
	{
		if (update.getTables().isEmpty())
			throw new SQLException("No table found in update statement");
		if (update.getTables().size() > 1)
			throw new SQLException("Update statements for multiple tables at once are not supported");
		String table = unquoteIdentifier(update.getTables().get(0).getFullyQualifiedName());
		getParameterStore().setTable(table);
		List<Expression> expressions = update.getExpressions();
		WriteBuilder builder = Mutation.newUpdateBuilder(table);
		int index = 0;
		for (Column col : update.getColumns())
		{
			String columnName = unquoteIdentifier(col.getFullyQualifiedName());
			expressions.get(index).accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
					builder.set(columnName), columnName));
			index++;
		}
		visitUpdateWhereClause(update.getWhere(), builder);

		return builder.build();
	}

	private Mutation createDeleteMutation(Delete delete) throws SQLException
	{
		String table = unquoteIdentifier(delete.getTable().getFullyQualifiedName());
		getParameterStore().setTable(table);
		Expression where = delete.getWhere();
		if (where == null)
		{
			// Delete all
			return Mutation.delete(table, KeySet.all());
		}
		else
		{
			// Delete one
			Key.Builder keyBuilder = Key.newBuilder();
			visitDeleteWhereClause(where, keyBuilder);
			return Mutation.delete(table, keyBuilder.build());
		}
	}

	private void visitDeleteWhereClause(Expression where, Key.Builder keyBuilder) throws SQLException
	{
		if (where != null)
		{
			DMLWhereClauseVisitor whereClauseVisitor = new DMLWhereClauseVisitor(getParameterStore())
			{

				@Override
				protected void visitExpression(Column col, Expression expression)
				{
					expression.accept(new KeyBuilderExpressionVisitorAdapter(getParameterStore(), keyBuilder));
				}

			};
			where.accept(whereClauseVisitor);
			if (!whereClauseVisitor.isValid())
			{
				throw new SQLException(INVALID_WHERE_CLAUSE_DELETE_MESSAGE);
			}
		}
	}

	private void visitUpdateWhereClause(Expression where, WriteBuilder builder) throws SQLException
	{
		if (where != null)
		{
			DMLWhereClauseVisitor whereClauseVisitor = new DMLWhereClauseVisitor(getParameterStore())
			{

				@Override
				protected void visitExpression(Column col, Expression expression)
				{
					String columnName = unquoteIdentifier(col.getFullyQualifiedName());
					expression.accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
							builder.set(columnName), columnName));
				}

			};
			where.accept(whereClauseVisitor);
			if (!whereClauseVisitor.isValid())
			{
				throw new SQLException(INVALID_WHERE_CLAUSE_UPDATE_MESSAGE);
			}
		}
		else
		{
			throw new SQLException(INVALID_WHERE_CLAUSE_UPDATE_MESSAGE);
		}
	}

	private static String unquoteIdentifier(String identifier)
	{
		String res = identifier;
		if (identifier == null)
			return identifier;
		if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`')
			res = identifier.substring(1, identifier.length() - 1);
		return res;
	}

	private int executeDDL(String ddl) throws SQLException
	{
		getConnection().executeDDL(ddl);
		return 0;
	}

	@Override
	public boolean execute() throws SQLException
	{
		Statement statement = null;
		boolean ddl = isDDLStatement(sql);
		if (!ddl)
		{
			try
			{
				statement = CCJSqlParserUtil.parse(sql);
			}
			catch (JSQLParserException | TokenMgrError e)
			{
				throw new SQLException(PARSE_ERROR + sql + ": " + e.getLocalizedMessage(), e);
			}
		}
		if (!ddl && statement instanceof Select)
		{
			lastResultSet = executeQuery();
			lastUpdateCount = -1;
			return true;
		}
		else
		{
			lastUpdateCount = executeUpdate();
			lastResultSet = null;
			return false;
		}
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		// parse the SQL statement without executing it
		try
		{
			if (isDDLStatement(sql))
			{
				throw new SQLException("Cannot get parameter meta data for DDL statement");
			}
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement instanceof Insert || statement instanceof Update || statement instanceof Delete)
			{
				// Create mutation, but don't do anything with it. This
				// initializes column names of the parameter store.
				createMutation();
			}
			else if (statement instanceof Select)
			{
				// Create select builder, but don't do anything with it. This
				// initializes column names of the parameter store.
				createSelectBuilder(statement);
			}
		}
		catch (JSQLParserException | TokenMgrError e)
		{
			throw new SQLException(PARSE_ERROR + sql + ": " + e.getLocalizedMessage(), e);
		}
		return new CloudSpannerParameterMetaData(this);
	}

}
