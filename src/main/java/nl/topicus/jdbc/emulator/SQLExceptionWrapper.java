package nl.topicus.jdbc.emulator;

import java.sql.SQLException;

import com.google.cloud.spanner.SpannerExceptionFactory;

class SQLExceptionWrapper
{
	@FunctionalInterface
<<<<<<< HEAD
	interface SQLConsumer
	{
		void consume() throws SQLException;
	}

	@FunctionalInterface
=======
>>>>>>> 48d3c4cef9b199e654b0f227adc981dd827326aa
	interface SQLSupplier<T>
	{
		T get() throws SQLException;
	}

	@FunctionalInterface
	interface SQLFunction<T, R>
	{
		R apply(T t) throws SQLException;
	}

<<<<<<< HEAD
	void consume(SQLConsumer consumer)
	{
		try
		{
			consumer.consume();
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(e);
		}
	}

=======
>>>>>>> 48d3c4cef9b199e654b0f227adc981dd827326aa
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
