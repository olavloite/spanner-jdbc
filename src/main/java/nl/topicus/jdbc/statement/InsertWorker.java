package nl.topicus.jdbc.statement;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;

public class InsertWorker extends AbstractTablePartWorker
{
	final Insert insert;

	public InsertWorker(CloudSpannerConnection connection, Select select, Insert insert, ParameterStore parameters,
			boolean allowExtendedMode, DMLOperation operation)
	{
		super(connection, select, parameters, allowExtendedMode, operation);
		this.insert = insert;
	}

	@Override
	protected List<String> getColumnNames() throws SQLException
	{
		String unquotedTableName = CloudSpannerDriver.unquoteIdentifier(getTable().getName());
		List<String> columnNamesList;
		if (insert.getColumns() == null)
		{
			columnNamesList = ConverterUtils.getQuotedColumnNames(connection, null, null, unquotedTableName);
		}
		else
		{
			columnNamesList = insert.getColumns().stream()
					.map(x -> CloudSpannerDriver.quoteIdentifier(x.getColumnName())).collect(Collectors.toList());
		}
		return columnNamesList;
	}

	@Override
	protected String createSQL() throws SQLException
	{
		List<String> columnNamesList = getColumnNames();
		String columnNames = String.join(", ", columnNamesList);
		String[] params = new String[columnNamesList.size()];
		Arrays.fill(params, "?");
		String parameterNames = String.join(", ", params);

		String sql = "INSERT INTO " + CloudSpannerDriver.quoteIdentifier(insert.getTable().getName()) + " ("
				+ columnNames + ") VALUES \n";
		sql = sql + "(" + parameterNames + ")";
		if (operation == DMLOperation.ONDUPLICATEKEYUPDATE || operation == DMLOperation.UPDATE)
		{
			sql = sql + " ON DUPLICATE KEY UPDATE";
		}
		return sql;
	}

	@Override
	protected Table getTable()
	{
		return insert.getTable();
	}

}
