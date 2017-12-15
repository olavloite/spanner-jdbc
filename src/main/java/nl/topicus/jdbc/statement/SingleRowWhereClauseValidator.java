package nl.topicus.jdbc.statement;

import java.util.HashMap;
import java.util.Map;

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;

public class SingleRowWhereClauseValidator
{
	private final TableKeyMetaData table;

	private Map<String, Object> keyValues = new HashMap<>();

	private Map<String, Boolean> keyValuesSet = new HashMap<>();

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
		keyValuesSet.put(currentColumn, Boolean.TRUE);
		currentColumn = null;
	}

	public boolean isValid()
	{
		for (String key : table.getKeyColumns())
		{
			Boolean set = keyValuesSet.get(key);
			if (set == null || !set.booleanValue())
			{
				return false;
			}
		}
		return true;
	}

}
