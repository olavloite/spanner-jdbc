package nl.topicus.jdbc.statement;

import java.util.HashMap;
import java.util.Map;

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;

public class SingleRowWhereClauseValidator
{
	private final TableKeyMetaData table;

	private Map<String, Object> keyValues = new HashMap<>();

	private String currentColumn = null;

	public SingleRowWhereClauseValidator(TableKeyMetaData table)
	{
		this.table = table;
	}

	public void set(String column)
	{
		currentColumn = column.toUpperCase();
	}

	public void to(Object value)
	{
		if (currentColumn == null)
			throw new IllegalArgumentException("No column set");
		keyValues.put(currentColumn, value);
		currentColumn = null;
	}

	public boolean isValid()
	{
		for (String key : table.getKeyColumns())
		{
			Object value = keyValues.get(key);
			if (value == null)
			{
				return false;
			}
		}
		return true;
	}

}
