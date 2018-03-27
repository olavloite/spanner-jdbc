package nl.topicus.sql2.operation;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import nl.topicus.java.sql2.JdbcType;
import nl.topicus.java.sql2.ParameterizedCountOperation;
import nl.topicus.java.sql2.Result.Count;
import nl.topicus.java.sql2.RowOperation;
import nl.topicus.java.sql2.SqlType;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.sql.SqlParser;
import nl.topicus.sql2.CloudSpannerConnection;

public class CloudSpannerParameterizedCountOperation<T> extends CloudSpannerParameterizedOperation<T>
		implements ParameterizedCountOperation<T>
{
	private static final String INVALID_WHERE_CLAUSE_DELETE_MESSAGE = "The DELETE statement does not contain a valid WHERE clause. DELETE statements must contain a WHERE clause specifying the value of the primary key of the record(s) to be deleted in the form 'ID=value' or 'ID1=value1 AND ID2=value2'";
	private static final String INVALID_WHERE_CLAUSE_UPDATE_MESSAGE = "The UPDATE statement does not contain a valid WHERE clause. UPDATE statements must contain a WHERE clause specifying the value of the primary key of the record(s) to be deleted in the form 'ID=value' or 'ID1=value1 AND ID2=value2'";
	static final String PARSE_ERROR = "Error while parsing sql statement ";

	private final SqlParser parser = new SqlParser();

	private final String sql;

	private final String[] sqlTokens;

	private Function<Count, T> processor;

	private boolean forceUpdate;

	protected CloudSpannerParameterizedCountOperation(Executor exec, CloudSpannerConnection connection, String sql)
	{
		super(exec, connection);
		this.sql = sql;
		this.sqlTokens = parser.getTokens(sql);
	}

	@Override
	public T get()
	{
		try
		{
			if (isDDLStatement())
			{
				String ddl = parser.formatDDLStatement(sql);
				return (T) executeDDL(ddl);
			}
			Mutations mutations = createMutations();
			return (T) writeMutations(mutations);
		}
		catch (SQLException e)
		{
			handle(e);
		}
		return null;
	}

	private boolean isDDLStatement()
	{
		return parser.isDDLStatement(sqlTokens);
	}

	private Long executeDDL(String ddl) throws SQLException
	{
		return executeDDL(Arrays.asList(ddl));
	}

	protected Long executeDDL(List<String> ddl) throws SQLException
	{
		return getConnection().executeDDL(ddl);
	}

	protected Long writeMutations(Mutations mutations) throws SQLException
	{
		// TODO: Implement bulk updates
		if (mutations.isWorker())
		{
			// ConversionResult result = mutations.getWorker().call();
			// if (result.getException() != null)
			// {
			// if (result.getException() instanceof SQLException)
			// throw (SQLException) result.getException();
			// if (result.getException() instanceof SpannerException)
			// throw new CloudSpannerSQLException((SpannerException)
			// result.getException());
			// throw new
			// CloudSpannerSQLException(result.getException().getMessage(),
			// Code.UNKNOWN,
			// result.getException());
			// }
		}
		else
		{

			if (getConnection().getAutoCommit())
			{
				getDbClient().readWriteTransaction().run(new TransactionCallable<Void>()
				{

					@Override
					public Void run(TransactionContext transaction) throws Exception
					{
						transaction.buffer(mutations.getMutations());
						return null;
					}
				});
			}
			else
			{
				// TODO: Implement transactions
				// getConnection().getTransaction().buffer(mutations.getMutations());
			}
		}
		return mutations.getNumberOfResults();
	}

	private Mutations createMutations() throws SQLException
	{
		return createMutations(sql, false, false);
	}

	private Mutations createMutations(String sql, boolean forceUpdate, boolean generateParameterMetaData)
			throws SQLException
	{
		try
		{
			Statement statement = CCJSqlParserUtil.parse(parser.sanitizeSQL(sql));
			if (statement instanceof Insert)
			{
				Insert insertStatement = (Insert) statement;
				// TODO: Implement insert-with-select statement
				return new Mutations(createInsertMutation(insertStatement, generateParameterMetaData));
			}
			else if (statement instanceof Update)
			{
				Update updateStatement = (Update) statement;
				if (updateStatement.getSelect() != null)
					throw new CloudSpannerSQLException(
							"UPDATE statement using SELECT is not supported. Try to re-write the statement as an INSERT INTO ... SELECT A, B, C FROM TABLE WHERE ... ON DUPLICATE KEY UPDATE",
							Code.INVALID_ARGUMENT);
				if (updateStatement.getTables().size() > 1)
					throw new CloudSpannerSQLException(
							"UPDATE statement using multiple tables is not supported. Try to re-write the statement as an INSERT INTO ... SELECT A, B, C FROM TABLE WHERE ... ON DUPLICATE KEY UPDATE",
							Code.INVALID_ARGUMENT);

				// TODO: Implement bulk update statement
				return new Mutations(createUpdateMutation(updateStatement, generateParameterMetaData));
			}
			else if (statement instanceof Delete)
			{
				Delete deleteStatement = (Delete) statement;
				// TODO: implement bulk delete statement
				return new Mutations(createDeleteMutation(deleteStatement, generateParameterMetaData));
			}
			else
			{
				throw new CloudSpannerSQLException(
						"Unrecognized or unsupported SQL-statment: Expected one of INSERT, UPDATE or DELETE. Please note that batching of prepared statements is not supported for SELECT-statements.",
						Code.INVALID_ARGUMENT);
			}
		}
		catch (JSQLParserException | IllegalArgumentException | TokenMgrError e)
		{
			throw new CloudSpannerSQLException(PARSE_ERROR + sql + ": " + e.getLocalizedMessage(),
					Code.INVALID_ARGUMENT, e);
		}
	}

	private Mutation createInsertMutation(Insert insert, boolean generateParameterMetaData) throws SQLException
	{
		ItemsList items = insert.getItemsList();
		if (generateParameterMetaData && items == null && insert.getSelect() != null)
		{
			// Just initialize the parameter meta data of the select statement
			createSelectBuilder(insert.getSelect(), insert.getSelect().toString());
			return null;
		}
		if (!(items instanceof ExpressionList))
		{
			throw new CloudSpannerSQLException("Insert statement must specify a list of values", Code.INVALID_ARGUMENT);
		}
		if (insert.getColumns() == null || insert.getColumns().isEmpty())
		{
			throw new CloudSpannerSQLException("Insert statement must specify a list of column names",
					Code.INVALID_ARGUMENT);
		}
		List<Expression> expressions = ((ExpressionList) items).getExpressions();
		String table = CloudSpannerDriver.unquoteIdentifier(insert.getTable().getFullyQualifiedName());
		getParameterStore().setTable(table);
		WriteBuilder builder;
		if (insert.isUseDuplicate())
		{
			/**
			 * Do an insert-or-update. BUT: Cloud Spanner does not support
			 * supplying different values for the insert and update statements,
			 * meaning that only the values specified in the INSERT part of the
			 * statement will be considered. Anything specified in the 'ON
			 * DUPLICATE KEY UPDATE ...' statement will be ignored.
			 */
			if (this.forceUpdate)
				builder = Mutation.newUpdateBuilder(table);
			else
				builder = Mutation.newInsertOrUpdateBuilder(table);
		}
		else
		{
			/**
			 * Just do an insert and throw an error if a row with the specified
			 * key alread exists.
			 */
			builder = Mutation.newInsertBuilder(table);
		}
		int index = 0;
		for (Column col : insert.getColumns())
		{
			String columnName = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
			expressions.get(index).accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
					builder.set(columnName), columnName));
			index++;
		}
		return builder.build();
	}

	private Mutation createUpdateMutation(Update update, boolean generateParameterMetaData) throws SQLException
	{
		if (update.getTables().isEmpty())
			throw new CloudSpannerSQLException("No table found in update statement", Code.INVALID_ARGUMENT);
		if (update.getTables().size() > 1)
			throw new CloudSpannerSQLException("Update statements for multiple tables at once are not supported",
					Code.INVALID_ARGUMENT);
		String table = CloudSpannerDriver.unquoteIdentifier(update.getTables().get(0).getFullyQualifiedName());
		getParameterStore().setTable(table);
		List<Expression> expressions = update.getExpressions();
		WriteBuilder builder = Mutation.newUpdateBuilder(table);
		int index = 0;
		for (Column col : update.getColumns())
		{
			String columnName = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
			expressions.get(index).accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
					builder.set(columnName), columnName));
			index++;
		}
		visitUpdateWhereClause(update.getWhere(), builder, generateParameterMetaData);

		return builder.build();
	}

	private Mutation createDeleteMutation(Delete delete, boolean generateParameterMetaData) throws SQLException
	{
		String table = CloudSpannerDriver.unquoteIdentifier(delete.getTable().getFullyQualifiedName());
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
			DeleteKeyBuilder keyBuilder = new DeleteKeyBuilder(null,
					// TODO: Implement getTable(String table)
					// getConnection().getTable(table),
					generateParameterMetaData);
			visitDeleteWhereClause(where, keyBuilder, generateParameterMetaData);
			return Mutation.delete(table, keyBuilder.getKeyBuilder().build());
		}
	}

	private void visitDeleteWhereClause(Expression where, DeleteKeyBuilder keyBuilder,
			boolean generateParameterMetaData) throws SQLException
	{
		if (where != null)
		{
			DMLWhereClauseVisitor whereClauseVisitor = new DMLWhereClauseVisitor(getParameterStore())
			{

				@Override
				protected void visitExpression(Column col, Expression expression)
				{
					String columnName = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
					keyBuilder.set(columnName);
					expression.accept(
							new KeyBuilderExpressionVisitorAdapter(getParameterStore(), columnName, keyBuilder));
				}

			};
			where.accept(whereClauseVisitor);
			if (!generateParameterMetaData && !whereClauseVisitor.isValid())
			{
				throw new CloudSpannerSQLException(INVALID_WHERE_CLAUSE_DELETE_MESSAGE, Code.INVALID_ARGUMENT);
			}
		}
	}

	private boolean isSingleRowWhereClause(TableKeyMetaData table, Expression where)
	{
		if (where != null)
		{
			SingleRowWhereClauseValidator validator = new SingleRowWhereClauseValidator(table);
			DMLWhereClauseVisitor whereClauseVisitor = new DMLWhereClauseVisitor(getParameterStore())
			{

				@Override
				protected void visitExpression(Column col, Expression expression)
				{
					String columnName = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
					validator.set(columnName);
					expression.accept(
							new SingleRowWhereClauseValidatorExpressionVisitorAdapter(getParameterStore(), validator));
				}

			};
			where.accept(whereClauseVisitor);
			return whereClauseVisitor.isValid() && validator.isValid();
		}
		return false;
	}

	private void visitUpdateWhereClause(Expression where, WriteBuilder builder, boolean generateParameterMetaData)
			throws SQLException
	{
		if (where != null)
		{
			DMLWhereClauseVisitor whereClauseVisitor = new DMLWhereClauseVisitor(getParameterStore())
			{

				@Override
				protected void visitExpression(Column col, Expression expression)
				{
					String columnName = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
					expression.accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
							builder.set(columnName), columnName));
				}

			};
			where.accept(whereClauseVisitor);
			if (!generateParameterMetaData && !whereClauseVisitor.isValid())
			{
				throw new CloudSpannerSQLException(INVALID_WHERE_CLAUSE_UPDATE_MESSAGE, Code.INVALID_ARGUMENT);
			}
		}
		else
		{
			throw new SQLException(INVALID_WHERE_CLAUSE_UPDATE_MESSAGE);
		}
	}

	private com.google.cloud.spanner.Statement.Builder createSelectBuilder(Statement statement, String sql)
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
		if (body instanceof PlainSelect)
		{
			setPlainSelectParameters((PlainSelect) body, builder);
		}
		else
		{
			body.accept(new SelectVisitorAdapter()
			{
				@Override
				public void visit(PlainSelect plainSelect)
				{
					setPlainSelectParameters(plainSelect, builder);
				}
			});
		}
	}

	private void setPlainSelectParameters(PlainSelect plainSelect, com.google.cloud.spanner.Statement.Builder builder)
	{
		if (plainSelect.getFromItem() != null)
		{
			plainSelect.getFromItem().accept(new FromItemVisitorAdapter()
			{
				private int tableCount = 0;

				@Override
				public void visit(Table table)
				{
					tableCount++;
					if (tableCount == 1)
						getParameterStore()
								.setTable(CloudSpannerDriver.unquoteIdentifier(table.getFullyQualifiedName()));
					else
						getParameterStore().setTable(null);
				}
			});
		}
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
			getParameterStore().setType(getParameterStore().getHighestIndex(), JdbcType.BIGINT);
		}
	}

	private void setWhereParameters(Expression where, com.google.cloud.spanner.Statement.Builder builder)
	{
		if (where != null)
		{
			where.accept(new ExpressionVisitorAdapter()
			{
				private String currentCol = null;

				@Override
				public void visit(Column col)
				{
					currentCol = CloudSpannerDriver.unquoteIdentifier(col.getFullyQualifiedName());
				}

				@Override
				public void visit(JdbcParameter parameter)
				{
					parameter.accept(new ValueBinderExpressionVisitorAdapter<>(getParameterStore(),
							builder.bind("p" + parameter.getIndex()), currentCol));
					currentCol = null;
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
	public RowOperation<T> returning(String... keys)
	{
		return null;
	}

	@Override
	public ParameterizedCountOperation<T> resultProcessor(Function<Count, T> processor)
	{
		this.processor = processor;
		return this;
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, Object value)
	{
		return (ParameterizedCountOperation<T>) super.set(id, value);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, Object value, SqlType type)
	{
		return (ParameterizedCountOperation<T>) super.set(id, value, type);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, CompletableFuture<?> valueFuture)
	{
		return (ParameterizedCountOperation<T>) super.set(id, valueFuture);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type)
	{
		return (ParameterizedCountOperation<T>) super.set(id, valueFuture, type);
	}

	@Override
	public ParameterizedCountOperation<T> timeout(long milliseconds)
	{
		return (ParameterizedCountOperation<T>) super.timeout(milliseconds);
	}

}
