package nl.topicus.jdbc.metadata;

import java.sql.SQLException;
import java.sql.Types;
import java.sql.Wrapper;

import com.google.cloud.spanner.Type;

public class AbstractCloudSpannerWrapper implements Wrapper
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
			return Types.VARCHAR;
		if (type.equals(Type.timestamp()))
			return Types.TIMESTAMP;
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
		if (sqlType == Types.VARCHAR)
			return Type.string().getCode().name();
		if (sqlType == Types.TIMESTAMP)
			return Type.timestamp().getCode().name();

		return "Other";
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

}
