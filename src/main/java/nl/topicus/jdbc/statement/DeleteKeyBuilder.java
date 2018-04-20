package nl.topicus.jdbc.statement;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.spanner.Key;
import com.google.rpc.Code;

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

public class DeleteKeyBuilder
{
	private final TableKeyMetaData table;

	private final boolean generateParameterMetaData;

	private Map<String, Object> keyValues = new HashMap<>();

	private String currentColumn = null;

	public DeleteKeyBuilder(TableKeyMetaData table, boolean generateParameterMetaData)
	{
		this.table = table;
		this.generateParameterMetaData = generateParameterMetaData;
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

	public Key.Builder getKeyBuilder() throws SQLException
	{
		Key.Builder builder = Key.newBuilder();
		for (String key : table.getKeyColumns())
		{
			Object value = keyValues.get(key);
			if (!generateParameterMetaData && value == null)
			{
				throw new CloudSpannerSQLException(
						"No value supplied for key column " + key
								+ ". All key columns must be specified in the WHERE-clause of a DELETE-statement.",
						Code.INVALID_ARGUMENT);
			}
			builder.appendObject(convert(value));
		}
		return builder;
	}

	private Object convert(Object value)
	{
		if (value != null)
		{
			if (Date.class.isAssignableFrom(value.getClass()))
			{
				Date dateValue = (Date) value;
				return CloudSpannerConversionUtil.toCloudSpannerDate(dateValue);
			}
			else if (Timestamp.class.isAssignableFrom(value.getClass()))
			{
				Timestamp timeValue = (Timestamp) value;
				return CloudSpannerConversionUtil.toCloudSpannerTimestamp(timeValue);
			}
		}
		return value;
	}

}
