package nl.topicus.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * 
 * @author loite
 *
 */
public abstract class AbstractCloudSpannerMetaData implements DatabaseMetaData
{
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
