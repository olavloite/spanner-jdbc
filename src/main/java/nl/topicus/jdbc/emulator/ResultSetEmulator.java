package nl.topicus.jdbc.emulator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.spanner.v1.ResultSetStats;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

class ResultSetEmulator extends SQLExceptionWrapper implements ResultSet
{
	private final java.sql.ResultSet rs;

	private final ResultSetMetaData metadata;

	ResultSetEmulator(java.sql.ResultSet rs) throws SQLException
	{
		this.rs = rs;
		this.metadata = rs.getMetaData();
	}

	@Override
	public Type getType()
	{
		return get(this::internalGetType);
	}

	private Type internalGetType() throws SQLException
	{
		List<StructField> fields = new ArrayList<StructField>(metadata.getColumnCount());
		for (int i = 1; i <= metadata.getColumnCount(); i++)
		{
			fields.add(StructField.of(metadata.getColumnLabel(i), convertSqlType(metadata.getColumnType(i))));
		}
		return Type.struct(fields);
	}

	private Type convertSqlType(int sqlType)
	{
		return AbstractCloudSpannerWrapper.getGoogleType(sqlType);
	}

	@Override
	public int getColumnCount()
	{
		return get(metadata::getColumnCount);
	}

	@Override
	public int getColumnIndex(String columnName)
	{
		return get(() -> {
			for (int i = 1; i <= metadata.getColumnCount(); i++)
			{
				if (columnName.equalsIgnoreCase(metadata.getColumnLabel(i)))
				{
					return i - 1;
				}
			}
			throw new IllegalArgumentException(columnName + " is not a valid column name");
		});
	}

	@Override
	public Type getColumnType(int columnIndex)
	{
		return get(() -> convertSqlType(metadata.getColumnType(columnIndex)));
	}

	@Override
	public Type getColumnType(String columnName)
	{
		return get(() -> convertSqlType(metadata.getColumnType(getColumnIndex(columnName) + 1)));
	}

	@Override
	public boolean isNull(int columnIndex)
	{
		return get(() -> {
			rs.getObject(columnIndex - 1);
			return rs.wasNull();
		});
	}

	@Override
	public boolean isNull(String columnName)
	{
		return get(() -> {
			rs.getObject(columnName);
			return rs.wasNull();
		});
	}

	@Override
	public boolean getBoolean(int columnIndex)
	{
		return get(rs::getBoolean, columnIndex + 1);
	}

	@Override
	public boolean getBoolean(String columnName)
	{
		return get(rs::getBoolean, columnName);
	}

	@Override
	public long getLong(int columnIndex)
	{
		return get(rs::getLong, columnIndex + 1);
	}

	@Override
	public long getLong(String columnName)
	{
		return get(rs::getLong, columnName);
	}

	@Override
	public double getDouble(int columnIndex)
	{
		return get(rs::getDouble, columnIndex + 1);
	}

	@Override
	public double getDouble(String columnName)
	{
		return get(rs::getDouble, columnName);
	}

	@Override
	public String getString(int columnIndex)
	{
		return get(rs::getString, columnIndex + 1);
	}

	@Override
	public String getString(String columnName)
	{
		return get(rs::getString, columnName);
	}

	@Override
	public ByteArray getBytes(int columnIndex)
	{
		return ByteArray.copyFrom(get(rs::getBytes, columnIndex + 1));
	}

	@Override
	public ByteArray getBytes(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean[] getBooleanArray(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean[] getBooleanArray(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Boolean> getBooleanList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Boolean> getBooleanList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getLongArray(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getLongArray(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> getLongList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> getLongList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[] getDoubleArray(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[] getDoubleArray(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Double> getDoubleList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Double> getDoubleList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getStringList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getStringList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteArray> getBytesList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteArray> getBytesList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Timestamp> getTimestampList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Timestamp> getTimestampList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Date> getDateList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Date> getDateList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Struct> getStructList(int columnIndex)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Struct> getStructList(String columnName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean next() throws SpannerException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Struct getCurrentRowAsStruct()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public ResultSetStats getStats()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
