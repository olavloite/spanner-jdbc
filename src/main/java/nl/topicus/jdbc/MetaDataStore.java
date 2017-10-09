package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class for storing meta data needed for translating SQL statements into Cloud
 * Spanner mutations, such as key columns and their ordinal positions
 * 
 * @author loite
 *
 */
public class MetaDataStore
{
	/**
	 * Class for storing the primary key columns of a table in the correct
	 * order.
	 * 
	 * @author loite
	 *
	 */
	public class TableKeyMetaData
	{
		private final String name;

		private final List<String> keyColumns = new ArrayList<>(2);

		TableKeyMetaData(String name)
		{
			this.name = name;
		}

		public List<String> getKeyColumns()
		{
			return keyColumns;
		}

		@Override
		public boolean equals(Object o)
		{
			if (!(o instanceof TableKeyMetaData))
				return false;
			TableKeyMetaData other = (TableKeyMetaData) o;
			return Objects.equals(this.name, other.name);
		}

		@Override
		public int hashCode()
		{
			return name.hashCode();
		}
	}

	private final Connection connection;

	private final Map<String, TableKeyMetaData> tables = new HashMap<>();

	MetaDataStore(Connection connection)
	{
		this.connection = connection;
	}

	public TableKeyMetaData getTable(String name) throws SQLException
	{
		if (name == null)
			return null;
		TableKeyMetaData res = tables.get(name.toUpperCase());
		if (res == null)
		{
			res = initTable(name);
			tables.put(name.toUpperCase(), res);
		}
		return res;
	}

	private TableKeyMetaData initTable(String name) throws SQLException
	{
		TableKeyMetaData table = new TableKeyMetaData(name);
		try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, name))
		{
			while (rs.next())
			{
				table.keyColumns.add(rs.getString("COLUMN_NAME").toUpperCase());
			}
		}
		return table;
	}

	void clear()
	{
		tables.clear();
	}

	void clearTable(String name)
	{
		if (name == null)
			return;
		tables.remove(name.toUpperCase());
	}

}
