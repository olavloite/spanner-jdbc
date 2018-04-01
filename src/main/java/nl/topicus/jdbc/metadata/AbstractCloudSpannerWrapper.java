package nl.topicus.jdbc.metadata;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Wrapper;

import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public abstract class AbstractCloudSpannerWrapper implements Wrapper
{

	public static int extractColumnType(Type type)
	{
		if (type.equals(Type.bool()))
			return Types.BOOLEAN;
		if (type.equals(Type.bytes()))
			return Types.BINARY;
		if (type.equals(Type.date()))
			return Types.DATE;
		if (type.equals(Type.float64()))
			return Types.DOUBLE;
		if (type.equals(Type.int64()))
			return Types.BIGINT;
		if (type.equals(Type.string()))
			return Types.NVARCHAR;
		if (type.equals(Type.timestamp()))
			return Types.TIMESTAMP;
		if (type.getCode() == Code.ARRAY)
			return Types.ARRAY;
		return Types.OTHER;
	}

	public static String getGoogleTypeName(int sqlType)
	{
		if (sqlType == Types.BOOLEAN)
			return Type.bool().getCode().name();
		if (sqlType == Types.BINARY)
			return Type.bytes().getCode().name();
		if (sqlType == Types.DATE)
			return Type.date().getCode().name();
		if (sqlType == Types.DOUBLE || sqlType == Types.FLOAT || sqlType == Types.DECIMAL)
			return Type.float64().getCode().name();
		if (sqlType == Types.BIGINT || sqlType == Types.INTEGER || sqlType == Types.TINYINT)
			return Type.int64().getCode().name();
		if (sqlType == Types.NVARCHAR)
			return Type.string().getCode().name();
		if (sqlType == Types.TIMESTAMP)
			return Type.timestamp().getCode().name();
		if (sqlType == Types.ARRAY)
			return Code.ARRAY.name();

		return "Other";
	}

	public static String getClassName(int sqlType)
	{
		if (sqlType == Types.BOOLEAN)
			return Boolean.class.getName();
		if (sqlType == Types.BINARY)
			return Byte[].class.getName();
		if (sqlType == Types.DATE)
			return Date.class.getName();
		if (sqlType == Types.DOUBLE || sqlType == Types.FLOAT || sqlType == Types.DECIMAL)
			return Double.class.getName();
		if (sqlType == Types.BIGINT || sqlType == Types.INTEGER || sqlType == Types.TINYINT)
			return Long.class.getName();
		if (sqlType == Types.NVARCHAR)
			return String.class.getName();
		if (sqlType == Types.TIMESTAMP)
			return Timestamp.class.getName();
		if (sqlType == Types.ARRAY)
			return Object.class.getName();

		return null;
	}

	public static String getClassName(Type type)
	{
		if (type == Type.bool())
			return Boolean.class.getName();
		if (type == Type.bytes())
			return Byte[].class.getName();
		if (type == Type.date())
			return Date.class.getName();
		if (type == Type.float64())
			return Double.class.getName();
		if (type == Type.int64())
			return Long.class.getName();
		if (type == Type.string())
			return String.class.getName();
		if (type == Type.timestamp())
			return Timestamp.class.getName();
		if (type.getCode() == Code.ARRAY)
		{
			if (type.getArrayElementType() == Type.bool())
				return Boolean[].class.getName();
			if (type.getArrayElementType() == Type.bytes())
				return Byte[][].class.getName();
			if (type.getArrayElementType() == Type.date())
				return Date[].class.getName();
			if (type.getArrayElementType() == Type.float64())
				return Double[].class.getName();
			if (type.getArrayElementType() == Type.int64())
				return Long[].class.getName();
			if (type.getArrayElementType() == Type.string())
				return String[].class.getName();
			if (type.getArrayElementType() == Type.timestamp())
				return Timestamp[].class.getName();
		}
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		if (isWrapperFor(getClass()))
		{
			return iface.cast(this);
		}
		throw new CloudSpannerSQLException("Cannot unwrap to " + iface.getName(), com.google.rpc.Code.INVALID_ARGUMENT);
	}

}
