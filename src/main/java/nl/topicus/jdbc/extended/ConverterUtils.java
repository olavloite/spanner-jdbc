package nl.topicus.jdbc.extended;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class ConverterUtils
{
	public ConverterUtils()
	{
	}

	public int calculateActualBatchSize(int numberOfCols, Connection destination, String catalog, String schema,
			String table) throws SQLException
	{
		int batchSize = 1500000;
		// Calculate number of rows in a batch based on the row size
		// Batch size is given as MiB when the destination is CloudSpanner
		// The maximum number of mutations per commit is 20,000
		int rowSize = getRowSize(destination, catalog, schema, table);
		int indices = getNumberOfIndices(destination, catalog, schema, table);
		int actualBatchSize = Math.max(Math.min(batchSize / rowSize, 20000 / (numberOfCols + indices)), 100);
		return actualBatchSize;
	}

	public int getRowSize(Connection destination, String catalog, String schema, String table) throws SQLException
	{
		return getEstimatedRowSizeInCloudSpanner(destination, catalog, schema, table, null);
	}

	public int getNumberOfIndices(Connection destination, String catalog, String schema, String table)
			throws SQLException
	{
		int count = 0;
		try (ResultSet indices = destination.getMetaData().getIndexInfo(catalog, schema, table, false, false))
		{
			while (indices.next())
				count++;
		}
		return count;
	}

	public List<String> getColumnNames(Connection destination, String catalog, String schema, String table)
			throws SQLException
	{
		List<String> res = new ArrayList<>();
		try (ResultSet cols = destination.getMetaData().getColumns(catalog, schema, table, null))
		{
			while (cols.next())
				res.add(cols.getString("COLUMN_NAME"));
		}
		return res;
	}

	/**
	 * 
	 * @return The estimated size in bytes of one row of the specified columns
	 *         of the specified table
	 */
	public int getEstimatedRowSizeInCloudSpanner(Connection connection, String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern) throws SQLException
	{
		// There's an 8 bytes storage overhead for each column
		int totalSize = 8;
		try (ResultSet rs = connection.getMetaData().getColumns(catalog, schemaPattern, tableNamePattern,
				columnNamePattern))
		{
			while (rs.next())
			{
				long colLength = rs.getLong("COLUMN_SIZE");
				int colType = rs.getInt("DATA_TYPE");
				switch (colType)
				{
				case Types.ARRAY:
					break;
				case Types.BOOLEAN:
					totalSize += 1;
					break;
				case Types.BINARY:
					totalSize += colLength;
					break;
				case Types.DATE:
					totalSize += 4;
					break;
				case Types.DOUBLE:
					totalSize += 8;
					break;
				case Types.BIGINT:
					totalSize += 8;
					break;
				case Types.NVARCHAR:
					totalSize += colLength * 2;
					break;
				case Types.TIMESTAMP:
					totalSize += 12;
					break;
				}
			}
		}
		return totalSize;
	}

}
