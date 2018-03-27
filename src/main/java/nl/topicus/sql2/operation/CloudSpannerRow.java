package nl.topicus.sql2.operation;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Future;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;

import nl.topicus.java.sql2.Result.Row;
import nl.topicus.jdbc.CloudSpannerArray;
import nl.topicus.jdbc.CloudSpannerDataType;
import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

class CloudSpannerRow implements Row
{
	private final ResultSet rs;

	private long rowNumber = 0;

	private String[] identifiers = null;

	CloudSpannerRow(ResultSet rs)
	{
		this.rs = rs;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(String id, Class<T> type)
	{
		int columnIndex = rs.getColumnIndex(id);
		if (Object.class.equals(type))
			return (T) getObject(columnIndex);

		if (boolean.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type))
			return (T) getBoolean(columnIndex);
		if (byte[].class.isAssignableFrom(type))
			return (T) getBytes(columnIndex);
		if (Date.class.isAssignableFrom(type))
			return (T) getDate(columnIndex);
		if (double.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type))
			return (T) getDouble(columnIndex);
		if (long.class.isAssignableFrom(type) || Long.class.isAssignableFrom(type))
			return (T) getLong(columnIndex);
		if (String.class.isAssignableFrom(type))
			return (T) getString(columnIndex);
		if (Timestamp.class.isAssignableFrom(type))
			return (T) getTimestamp(columnIndex);
		if (Array.class.isAssignableFrom(type))
			return (T) getArray(columnIndex);
		throw new IllegalArgumentException("Unknown type: " + type.toString());
	}

	private Object getObject(int columnIndex)
	{
		Type type = rs.getColumnType(columnIndex);
		return rs.isNull(columnIndex) ? null : getObject(type, columnIndex);
	}

	private Object getObject(Type type, int columnIndex)
	{
		if (type == Type.bool())
			return getBoolean(columnIndex);
		if (type == Type.bytes())
			return getBytes(columnIndex);
		if (type == Type.date())
			return getDate(columnIndex);
		if (type == Type.float64())
			return getDouble(columnIndex);
		if (type == Type.int64())
			return getLong(columnIndex);
		if (type == Type.string())
			return getString(columnIndex);
		if (type == Type.timestamp())
			return getTimestamp(columnIndex);
		if (type.getCode() == Code.ARRAY)
			return getArray(columnIndex);
		throw new IllegalArgumentException("Unknown type: " + type.toString());
	}

	public String getString(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getString(columnIndex);
	}

	public Boolean getBoolean(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getBoolean(columnIndex);
	}

	public Long getLong(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getLong(columnIndex);
	}

	public Double getDouble(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getDouble(columnIndex);
	}

	private BigDecimal toBigDecimal(double d)
	{
		return BigDecimal.valueOf(d);
	}

	public byte[] getBytes(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getBytes(columnIndex).toByteArray();
	}

	public Date getDate(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : CloudSpannerConversionUtil.toSqlDate(rs.getDate(columnIndex));
	}

	public Time getTime(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : CloudSpannerConversionUtil.toSqlTime(rs.getTimestamp(columnIndex));
	}

	public Timestamp getTimestamp(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : rs.getTimestamp(columnIndex).toSqlTimestamp();
	}

	public BigDecimal getBigDecimal(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : toBigDecimal(rs.getDouble(columnIndex));
	}

	public Byte getByte(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : (byte) rs.getLong(columnIndex);
	}

	public Short getShort(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : (short) rs.getLong(columnIndex);
	}

	public Integer getInt(int columnIndex)
	{
		return rs.isNull(columnIndex) ? null : (int) rs.getLong(columnIndex);
	}

	public Float getFloat(int columnIndex)
	{
		return rs.isNull(columnIndex) ? 0 : (float) rs.getDouble(columnIndex);
	}

	public Array getArray(int columnIndex)
	{
		if (rs.isNull(columnIndex))
			return null;
		Type type = rs.getColumnType(columnIndex);
		if (type.getCode() != Code.ARRAY)
			throw new IllegalArgumentException("Column with index " + columnIndex + " does not contain an array");
		CloudSpannerDataType dataType = CloudSpannerDataType.getType(type.getArrayElementType().getCode());
		List<? extends Object> elements = dataType.getArrayElements(rs, columnIndex);

		return CloudSpannerArray.createArray(dataType, elements);
	}

	@Override
	public String[] getIdentifiers()
	{
		Preconditions.checkState(rs != null, "ResultSet is null");
		if (identifiers == null)
		{
			identifiers = new String[rs.getColumnCount()];
			int index = 0;
			for (StructField sf : rs.getType().getStructFields())
			{
				identifiers[index] = sf.getName();
				index++;
			}
		}
		return identifiers;
	}

	void nextRow()
	{
		rowNumber++;
	}

	@Override
	public long rowNumber()
	{
		return rowNumber;
	}

	@Override
	public Future<Boolean> isLast()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cancel()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void request(long count)
	{
		// TODO Auto-generated method stub

	}

}
