package nl.topicus.sql2.operation;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.cloud.spanner.ReadContext;
import com.google.common.base.Preconditions;
import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;
import nl.topicus.java.sql2.JdbcType;
import nl.topicus.java.sql2.ParameterizedRowOperation;
import nl.topicus.java.sql2.Result.Row;
import nl.topicus.java.sql2.SqlType;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.sql.SqlParser;
import nl.topicus.sql2.CloudSpannerConnection;

public class CloudSpannerParameterizedRowOperation<T> extends CloudSpannerParameterizedOperation<T>
		implements ParameterizedRowOperation<T>
{
	private static final String PARSE_ERROR = "Error while parsing sql statement ";

	private final SqlParser parser = new SqlParser();

	private boolean forceSingleUseReadContext = false;

	private Supplier<? extends T> initialValue;

	private BiFunction<? super T, Row, ? extends T> aggregator;

	private long fetchSize = 1l;

	private final String sql;

	private final String[] sqlTokens;

	protected CloudSpannerParameterizedRowOperation(Executor exec, CloudSpannerConnection connection, String sql)
	{
		super(exec, connection);
		this.sql = sql;
		this.sqlTokens = parser.getTokens(sql);
	}

	@Override
	public T get()
	{
		Statement statement;
		try
		{
			statement = CCJSqlParserUtil.parse(parser.sanitizeSQL(sql));
		}
		catch (JSQLParserException | TokenMgrError e)
		{
			throw new IllegalArgumentException(new CloudSpannerSQLException(
					PARSE_ERROR + sql + ": " + e.getLocalizedMessage(), Code.INVALID_ARGUMENT, e));
		}
		if (statement instanceof Select)
		{
			this.forceSingleUseReadContext = parser.determineForceSingleUseReadContext((Select) statement);
			com.google.cloud.spanner.Statement.Builder builder = createSelectBuilder(statement, sql);
			try (ReadContext context = getReadContext())
			{
				com.google.cloud.spanner.ResultSet rs = context.executeQuery(builder.build());
				CloudSpannerRow row = new CloudSpannerRow(rs);
				T res = initialValue.get();
				while (rs.next())
				{
					aggregator.apply(res, row);
					row.nextRow();
				}
				return res;
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		throw new IllegalArgumentException(new CloudSpannerSQLException(
				"SQL statement not suitable for executeQuery. Expected SELECT-statement.", Code.INVALID_ARGUMENT));
	}

	protected ReadContext getReadContext() throws SQLException
	{
		if (getConnection().getAutoCommit() || forceSingleUseReadContext)
		{
			return getConnection().getDbClient().singleUse();
		}
		// TODO: Implement transactions
		// return getConnection().getTransaction();
		throw new IllegalStateException("Transactions are not yet supported");
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
	public ParameterizedRowOperation<T> fetchSize(long rows) throws IllegalArgumentException
	{
		Preconditions.checkArgument(rows > 0, "Rows must be larger than zero");
		this.fetchSize = rows;
		return this;
	}

	@Override
	public ParameterizedRowOperation<T> initialValue(Supplier<? extends T> supplier)
	{
		Preconditions.checkNotNull(supplier, "Supplier cannot be null");
		this.initialValue = supplier;
		return this;
	}

	@Override
	public ParameterizedRowOperation<T> rowAggregator(BiFunction<? super T, Row, ? extends T> aggregator)
	{
		Preconditions.checkNotNull(aggregator, "Aggregator cannot be null");
		this.aggregator = aggregator;
		return this;
	}

	@Override
	public ParameterizedRowOperation<T> set(String id, Object value)
	{
		return (ParameterizedRowOperation<T>) super.set(id, value);
	}

	@Override
	public ParameterizedRowOperation<T> set(String id, Object value, SqlType type)
	{
		return (ParameterizedRowOperation<T>) super.set(id, value, type);
	}

	@Override
	public ParameterizedRowOperation<T> set(String id, CompletableFuture<?> valueFuture)
	{
		return (ParameterizedRowOperation<T>) super.set(id, valueFuture);
	}

	@Override
	public ParameterizedRowOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type)
	{
		return (ParameterizedRowOperation<T>) super.set(id, valueFuture, type);
	}

	@Override
	public ParameterizedRowOperation<T> timeout(long milliseconds)
	{
		return (ParameterizedRowOperation<T>) super.timeout(milliseconds);
	}

}
