package nl.topicus.jdbc.emulator;

import java.sql.SQLException;

import com.google.cloud.spanner.SpannerExceptionFactory;

class SQLExceptionWrapper
{
	@FunctionalInterface
	interface SQLSupplier<T>
	{
		T get() throws SQLException;
	}

	@FunctionalInterface
	interface SQLFunction<T, R>
	{
		R apply(T t) throws SQLException;
	}

	<T> T get(SQLSupplier<T> supplier)
	{
		try
		{
			return supplier.get();
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(e);
		}
	}

	<T, R> R get(SQLFunction<T, R> function, T t)
	{
		try
		{
			return function.apply(t);
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(e);
		}
	}

}
