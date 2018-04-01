package nl.topicus.jdbc.transaction;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

/**
 * Savepoint implementation for Google Cloud Spanner JDBC driver
 * 
 * @author loite
 *
 */
public class CloudSpannerSavepoint implements Savepoint
{
	private static final AtomicInteger CURRENT_ID = new AtomicInteger(0);

	private final String name;

	private final Integer id;

	static CloudSpannerSavepoint named(String name)
	{
		Preconditions.checkNotNull(name);
		return new CloudSpannerSavepoint(name);
	}

	static CloudSpannerSavepoint generated()
	{
		return new CloudSpannerSavepoint(CURRENT_ID.incrementAndGet());
	}

	private CloudSpannerSavepoint(int id)
	{
		this.id = id;
		this.name = null;
	}

	private CloudSpannerSavepoint(String name)
	{
		this.id = null;
		this.name = name;
	}

	private boolean isNamed()
	{
		return name != null;
	}

	@Override
	public int hashCode()
	{
		return isNamed() ? name.hashCode() : id.hashCode();
	}

	@Override
	public int getSavepointId() throws SQLException
	{
		if (isNamed())
			throw new CloudSpannerSQLException("This is a named savepoint", Code.FAILED_PRECONDITION);
		return id;
	}

	@Override
	public String getSavepointName() throws SQLException
	{
		if (!isNamed())
			throw new CloudSpannerSQLException("This is not a named savepoint", Code.FAILED_PRECONDITION);
		return name;
	}

}
