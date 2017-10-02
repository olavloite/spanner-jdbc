package nl.topicus.jdbc.statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;

public class DeleteWorker extends AbstractTablePartWorker
{
	private final Delete delete;

	private long recordCount;

	public DeleteWorker(CloudSpannerConnection connection, Delete delete, boolean allowExtendedMode) throws SQLException
	{
		super(connection, createSelect(connection, delete), allowExtendedMode);
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
			throw new SQLException("Could not parse generated SELECT statement: " + sql);
		}
	}

	@Override
	protected void run() throws SQLException
	{
		ConverterUtils converterUtils = new ConverterUtils();
		String unquotedTableName = CloudSpannerDriver.unquoteIdentifier(delete.getTable().getName());
		TableKeyMetaData table = connection.getTable(CloudSpannerDriver.unquoteIdentifier(delete.getTable().getName()));
		List<String> keyCols = table.getKeyColumns().stream().map(x -> CloudSpannerDriver.quoteIdentifier(x))
				.collect(Collectors.toList());
		List<String> columnNamesList = converterUtils.getQuotedColumnNames(connection, null, null, unquotedTableName);
		long batchSize = converterUtils.calculateActualBatchSize(columnNamesList.size(), connection, null, null,
				unquotedTableName);
		boolean isExtendedMode = isExtendedMode(batchSize);

		boolean wasAutocommit = connection.getAutoCommit();
		if (!isExtendedMode && wasAutocommit)
		{
			connection.setAutoCommit(false);
		}
		try (Connection destination = isExtendedMode
				? DriverManager.getConnection(connection.getUrl(), connection.getSuppliedProperties()) : null)
		{
			if (destination != null)
			{
				destination.setAutoCommit(false);
			}

			String sql = "DELETE FROM " + CloudSpannerDriver.quoteIdentifier(delete.getTable().getName()) + " WHERE ";
			boolean first = true;
			for (String key : keyCols)
			{
				if (!first)
					sql = sql + " AND ";
				sql = sql + key + "=?";
				first = false;
			}

			try (PreparedStatement statement = destination == null ? connection.prepareStatement(sql)
					: destination.prepareStatement(sql))
			{
				try (ResultSet rs = connection.prepareStatement(select.toString()).executeQuery())
				{
					while (rs.next())
					{
						for (int index = 1; index <= keyCols.size(); index++)
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object);
						}
						statement.executeUpdate();
						recordCount++;
						if (destination != null && recordCount % batchSize == 0)
							destination.commit();
					}
				}
			}
			if (destination != null)
			{
				destination.commit();
			}
			if (wasAutocommit && !isExtendedMode)
			{
				connection.commit();
				connection.setAutoCommit(true);
			}
		}
		catch (Exception e)
		{
			if (wasAutocommit && !isExtendedMode)
			{
				connection.rollback();
				connection.setAutoCommit(true);
			}
			if (e instanceof SQLException)
			{
				throw (SQLException) e;
			}
			else
			{
				throw new SQLException(e.getMessage(), e);
			}
		}
	}

	@Override
	public long getRecordCount()
	{
		return recordCount;
	}

	Delete getDelete()
	{
		return delete;
	}

}
