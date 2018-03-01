package nl.topicus.jdbc.statement;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public class DeleteWorker extends AbstractTablePartWorker
{
	final Delete delete;

	public DeleteWorker(CloudSpannerConnection connection, Delete delete, ParameterStore parameters,
			boolean allowExtendedMode) throws SQLException
	{
		super(connection, createSelect(connection, delete), parameters, allowExtendedMode, DMLOperation.DELETE);
		this.delete = delete;
	}

	private static Select createSelect(CloudSpannerConnection connection, Delete delete) throws SQLException
	{
		TableKeyMetaData table = connection.getTable(CloudSpannerDriver.unquoteIdentifier(delete.getTable().getName()));
		List<String> keyCols = table.getKeyColumns().stream()
				.map(x -> CloudSpannerDriver.quoteIdentifier(delete.getTable().getName()) + "."
						+ CloudSpannerDriver.quoteIdentifier(x))
				.collect(Collectors.toList());
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ").append(String.join(", ", keyCols));
		sql.append("\nFROM ").append(CloudSpannerDriver.quoteIdentifier(delete.getTable().getName()));
		sql.append("\nWHERE ").append(delete.getWhere().toString());

		try
		{
			return (Select) CCJSqlParserUtil.parse(sql.toString());
		}
		catch (JSQLParserException e)
		{
			throw new CloudSpannerSQLException("Could not parse generated SELECT statement: " + sql,
					Code.INVALID_ARGUMENT);
		}
	}

	@Override
	protected List<String> getColumnNames() throws SQLException
	{
		String unquotedTableName = CloudSpannerDriver.unquoteIdentifier(getTable().getName());
		return ConverterUtils.getQuotedColumnNames(connection, null, null, unquotedTableName);
	}

	@Override
	protected String createSQL() throws SQLException
	{
		TableKeyMetaData table = connection.getTable(CloudSpannerDriver.unquoteIdentifier(getTable().getName()));
		List<String> keyCols = table.getKeyColumns().stream().map(CloudSpannerDriver::quoteIdentifier)
				.collect(Collectors.toList());
		StringBuilder sql = new StringBuilder("DELETE FROM ")
				.append(CloudSpannerDriver.quoteIdentifier(delete.getTable().getName())).append(" WHERE ");
		boolean first = true;
		for (String key : keyCols)
		{
			if (!first)
				sql.append(" AND ");
			sql.append(key).append("=?");
			first = false;
		}
		return sql.toString();
	}

	@Override
	protected Table getTable()
	{
		return delete.getTable();
	}

}
