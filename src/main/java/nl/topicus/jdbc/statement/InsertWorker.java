package nl.topicus.jdbc.statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;

public class InsertWorker extends AbstractTablePartWorker
{
	public static enum Mode
	{
		Insert, OnDuplicateKeyUpdate, Update;
	}

	private final Insert insert;

	private long recordCount;

	private final Mode mode;

	public InsertWorker(CloudSpannerConnection connection, Select select, Insert insert, boolean allowExtendedMode,
			Mode mode)
	{
		super(connection, select, allowExtendedMode);
		this.insert = insert;
		this.mode = mode;
	}

	@Override
	protected void run() throws SQLException
	{
		ConverterUtils converterUtils = new ConverterUtils();
		String unquotedTableName = CloudSpannerDriver.unquoteIdentifier(insert.getTable().getName());
		List<String> columnNamesList;
		if (insert.getColumns() == null)
		{
			columnNamesList = converterUtils.getQuotedColumnNames(connection, null, null, unquotedTableName);
		}
		else
		{
			columnNamesList = insert.getColumns().stream()
					.map(x -> CloudSpannerDriver.quoteIdentifier(x.getColumnName())).collect(Collectors.toList());
		}
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

			String columnNames = String.join(", ", columnNamesList);
			String[] params = new String[columnNamesList.size()];
			Arrays.fill(params, "?");
			String parameterNames = String.join(", ", params);

			String sql = "INSERT INTO " + CloudSpannerDriver.quoteIdentifier(insert.getTable().getName()) + " ("
					+ columnNames + ") VALUES \n";
			sql = sql + "(" + parameterNames + ")";
			if (mode == Mode.OnDuplicateKeyUpdate || mode == Mode.Update)
			{
				sql = sql + " ON DUPLICATE KEY UPDATE";
			}
			try (PreparedStatement statement = destination == null ? connection.prepareStatement(sql)
					: destination.prepareStatement(sql))
			{
				if (mode == Mode.Update)
				{
					// Set force update
					((CloudSpannerPreparedStatement) statement).setForceUpdate(true);
				}
				try (ResultSet rs = connection.prepareStatement(select.toString()).executeQuery())
				{
					while (rs.next())
					{
						for (int index = 1; index <= columnNamesList.size(); index++)
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

	Insert getInsert()
	{
		return insert;
	}

	Mode getMode()
	{
		return mode;
	}

}
