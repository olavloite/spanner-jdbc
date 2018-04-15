package nl.topicus.jdbc.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.spanner.ResultSet;
import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;
import nl.topicus.jdbc.statement.CloudSpannerStatement;

public class CloudSpannerResultSetMetaData extends AbstractCloudSpannerWrapper implements ResultSetMetaData
{
	private static final String UNKNOWN_COLUMN = "Unknown column at index ";

	private static String getUnknownColumnMsg(int column)
	{
		return UNKNOWN_COLUMN + column;
	}

	private static final class ParseException extends RuntimeException
	{
		private static final long serialVersionUID = 1L;

		private final SQLException e;

		private ParseException(SQLException e)
		{
			this.e = e;
		}
	}

	private final ResultSet resultSet;

	private final CloudSpannerStatement statement;

	private final String sql;

	private boolean initialized = false;

	/**
	 * The tables used in this result set in the order that they appear in the
	 * select statement
	 */
	private List<Table> tables = null;

	/**
	 * The columns of this result set in the order that they appear in the
	 * result set
	 */
	private List<Column> columns = null;

	/**
	 * The aliases of the columns of this result set in the order that they
	 * appear in the result set
	 */
	private List<Alias> aliases = null;

	public CloudSpannerResultSetMetaData(ResultSet resultSet, CloudSpannerStatement statement, String sql)
	{
		this.resultSet = resultSet;
		this.statement = statement;
		this.sql = sql;
	}

	private void initMetaData() throws SQLException
	{
		if (initialized)
			return;

		initialized = true;
		Statement sqlStatement = null;
		if (sql == null)
			return;
		try
		{
			sqlStatement = CCJSqlParserUtil.parse(sanitizeSQL(sql));
		}
		catch (JSQLParserException | TokenMgrError e)
		{
			// ignore
			return;
		}
		if (!(sqlStatement instanceof Select))
			return;
		Select select = (Select) sqlStatement;
		try
		{
			initTables(select);
			initColumns(select);
		}
		catch (ParseException e)
		{
			throw e.e;
		}
	}

	private void initTables(Select select)
	{
		tables = new ArrayList<>();
		if (select.getSelectBody() != null)
		{
			select.getSelectBody().accept(new SelectVisitorAdapter()
			{
				@Override
				public void visit(PlainSelect plainSelect)
				{
					plainSelect.getFromItem().accept(new FromItemVisitorAdapter()
					{
						@Override
						public void visit(Table table)
						{
							initTable(table);
						}
					});
					if (plainSelect.getJoins() != null)
					{
						for (Join join : plainSelect.getJoins())
						{
							join.getRightItem().accept(new FromItemVisitorAdapter()
							{
								@Override
								public void visit(Table table)
								{
									initTable(table);
								}
							});
						}
					}
				}
			});
		}
	}

	private void initTable(Table table)
	{
		tables.add(table);
	}

	private void initColumns(Select select)
	{
		columns = new ArrayList<>();
		aliases = new ArrayList<>();
		select.getSelectBody().accept(new SelectVisitorAdapter()
		{
			@Override
			public void visit(PlainSelect plainSelect)
			{
				for (SelectItem selectItem : plainSelect.getSelectItems())
				{
					selectItem.accept(new SelectItemVisitor()
					{
						private boolean foundColumn = false;

						@Override
						public void visit(SelectExpressionItem selectExpressionItem)
						{
							selectExpressionItem.getExpression().accept(new ExpressionVisitorAdapter()
							{
								@Override
								public void visit(Column column)
								{
									registerColumn(column, selectExpressionItem.getAlias());
									foundColumn = true;
								}
							});
							if (!foundColumn)
							{
								registerColumn(null, selectExpressionItem.getAlias());
							}
						}

						@Override
						public void visit(AllTableColumns allTableColumns)
						{
							registerAllTableColumns(allTableColumns.getTable());
						}

						@Override
						public void visit(AllColumns allColumns)
						{
							for (Table table : tables)
							{
								registerAllTableColumns(table);
							}
						}
					});
				}
			}
		});
	}

	private void registerAllTableColumns(Table table)
	{
		try (java.sql.ResultSet rs = statement.getConnection().getMetaData().getColumns("", "", table.getName(), null))
		{
			while (rs.next())
			{
				registerColumn(new Column(table, rs.getString("COLUMN_NAME")));
			}
		}
		catch (SQLException e)
		{
			throw new ParseException(e);
		}
	}

	private void registerColumn(Column column)
	{
		registerColumn(column, null);
	}

	private void registerColumn(Column column, Alias alias)
	{
		columns.add(column);
		aliases.add(alias);
	}

	private String sanitizeSQL(String sql)
	{
		// Add a pseudo update to the end if no columns have been specified in
		// an 'on duplicate key update'-statement
		if (sql.matches("(?is)\\s*INSERT\\s+.*\\s+ON\\s+DUPLICATE\\s+KEY\\s+UPDATE\\s*"))
		{
			sql = sql + " FOO=BAR";
		}
		// Remove @{FORCE_INDEX...} statements
		sql = sql.replaceAll("(?is)\\@\\{\\s*FORCE_INDEX.*\\}", "");

		return sql;
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return resultSet.getColumnCount();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException
	{
		int type = getColumnType(column);
		return type == Types.NVARCHAR || type == Types.BINARY;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException
	{
		Column col = getColumn(column);
		return col != null && col.getTable() != null;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException
	{
		Column col = getColumn(column);
		if (col != null && col.getTable() != null)
		{
			try (java.sql.ResultSet rs = statement.getConnection().getMetaData().getColumns("", "",
					col.getTable().getName(), col.getColumnName()))
			{
				if (rs.next())
				{
					return rs.getInt("NULLABLE");
				}
			}
		}
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException
	{
		int type = getColumnType(column);
		return type == Types.DOUBLE || type == Types.BIGINT;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException
	{
		int colType = getColumnType(column);
		switch (colType)
		{
		case Types.ARRAY:
			return 50;
		case Types.BOOLEAN:
			return 5;
		case Types.BINARY:
			return 50;
		case Types.DATE:
			return 10;
		case Types.DOUBLE:
			return 14;
		case Types.BIGINT:
			return 10;
		case Types.NVARCHAR:
			int length = getPrecision(column);
			return length == 0 ? 50 : length;
		case Types.TIMESTAMP:
			return 16;
		default:
			return 10;
		}
	}

	@Override
	public String getColumnLabel(int column) throws SQLException
	{
		Alias alias = getAlias(column);
		if (alias != null)
			return alias.getName();
		return resultSet.getType().getStructFields().get(column - 1).getName();
	}

	@Override
	public String getColumnName(int column) throws SQLException
	{
		Column col = getColumn(column);
		if (col != null)
			return col.getColumnName();
		return resultSet.getType().getStructFields().get(column - 1).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException
	{
		return statement.getConnection().getSchema();
	}

	private Column getColumn(int column) throws SQLException
	{
		initMetaData();
		if (column > columns.size())
			throw new CloudSpannerSQLException(getUnknownColumnMsg(column), Code.INVALID_ARGUMENT);
		return columns.get(column - 1);
	}

	private Alias getAlias(int column) throws SQLException
	{
		initMetaData();
		if (column > aliases.size())
			throw new CloudSpannerSQLException(getUnknownColumnMsg(column), Code.INVALID_ARGUMENT);
		return aliases.get(column - 1);
	}

	@Override
	public int getPrecision(int column) throws SQLException
	{
		int colType = getColumnType(column);
		switch (colType)
		{
		case Types.BOOLEAN:
			return 1;
		case Types.DATE:
			return 10;
		case Types.DOUBLE:
			return 14;
		case Types.BIGINT:
			return 10;
		case Types.TIMESTAMP:
			return 24;
		default:
			// Not fixed size, try to get it from INFORMATION_SCHEMA
		}
		Column col = getColumn(column);
		if (col != null && col.getTable() != null)
		{
			try (java.sql.ResultSet rs = statement.getConnection().getMetaData().getColumns("", "",
					col.getTable().getName(), col.getColumnName()))
			{
				if (rs.next())
				{
					return rs.getInt("COLUMN_SIZE");
				}
			}
		}
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		int colType = getColumnType(column);
		if (colType == Types.DOUBLE)
			return 15;
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		Column col = getColumn(column);
		if (col != null && col.getTable() != null)
			return col.getTable().getName();

		return "";
	}

	@Override
	public String getCatalogName(int column) throws SQLException
	{
		// Use this method to honor the null / "" setting
		return statement.getConnection().getCatalog();
	}

	@Override
	public int getColumnType(int column) throws SQLException
	{
		return extractColumnType(resultSet.getColumnType(column - 1));
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException
	{
		return resultSet.getColumnType(column - 1).getCode().name();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		Column col = getColumn(column);
		if (col == null || col.getTable() == null)
			return true;
		// Primary key columns are always read-only, all other columns are
		// writable.
		try (java.sql.ResultSet rs = statement.getConnection().getMetaData().getPrimaryKeys("", "",
				col.getTable().getName()))
		{
			while (rs.next())
			{
				if (rs.getString("COLUMN_NAME").equalsIgnoreCase(col.getColumnName()))
				{
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		return !isReadOnly(column);
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		return !isReadOnly(column);
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		return getClassName(resultSet.getColumnType(column - 1));
	}

	@Override
	public String toString()
	{
		StringBuilder res = new StringBuilder();
		try
		{
			for (int col = 1; col <= getColumnCount(); col++)
			{
				res.append("Col ").append(col).append(": ");
				res.append(getColumnName(col)).append(" ").append(getColumnTypeName(col));
				res.append("\n");
			}
		}
		catch (SQLException e)
		{
			return "An error occurred while generating string: " + e.getMessage();
		}
		return res.toString();
	}

}
