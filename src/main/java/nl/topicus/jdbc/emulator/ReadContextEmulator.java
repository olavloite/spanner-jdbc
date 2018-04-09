package nl.topicus.jdbc.emulator;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;

class ReadContextEmulator extends AbstractReadContextEmulator
{

	ReadContextEmulator(String url)
	{
		super(url);
	}

	@Override
	public ResultSet executeQuery(Statement statement, QueryOption... options)
	{
		try
		{
			PreparedStatement ps = getConnection().prepareStatement(statement.getSql());
			java.sql.ResultSet rs = ps.executeQuery();
		}
		catch (SQLException e)
		{
			throw SpannerExceptionFactory.newSpannerException(e);
		}
	}

}
