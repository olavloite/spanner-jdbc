package nl.topicus.jdbc.extended;

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

public class InsertWorker extends AbstractTablePartWorker
{
	private final Insert insert;

	private long recordCount;

	private final boolean onDuplicateKeyUpdate;

	public InsertWorker(CloudSpannerConnection connection, Select select, Insert insert,
			long extendedRecordCountThreshold, boolean onDuplicateKeyUpdate)
	{
		super(connection, select, extendedRecordCountThreshold);
		this.insert = insert;
		this.onDuplicateKeyUpdate = onDuplicateKeyUpdate;
	}

	@Override
	protected void run() throws SQLException
	{
		boolean isExtendedMode = isExtendedMode();
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
			ConverterUtils converterUtils = new ConverterUtils();
			List<String> columnNamesList;
			if (insert.getColumns() == null)
				columnNamesList = converterUtils.getColumnNames(destination == null ? connection : destination, null,
						null, insert.getTable().getName());
			else
				columnNamesList = insert.getColumns().stream().map(x -> x.getColumnName()).collect(Collectors.toList());
			int batchSize = converterUtils.calculateActualBatchSize(columnNamesList.size(),
					destination == null ? connection : destination, null, null, insert.getTable().getName());

			String columnNames = String.join(", ", columnNamesList);
			String[] params = new String[columnNamesList.size()];
			Arrays.fill(params, "?");
			String parameterNames = String.join(", ", params);

			String sql = "INSERT INTO " + insert.getTable().getName() + " (" + columnNames + ") VALUES \n";
			sql = sql + "(" + parameterNames + ")";
			if (onDuplicateKeyUpdate)
			{
				sql = sql + " ON DUPLICATE KEY UPDATE";
			}
			try (PreparedStatement statement = destination == null ? connection.prepareStatement(sql)
					: destination.prepareStatement(sql))
			{
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

}
