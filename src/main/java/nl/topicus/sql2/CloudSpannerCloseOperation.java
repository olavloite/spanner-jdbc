package nl.topicus.sql2;

import java.sql.SQLException;
import java.util.concurrent.Executor;

import nl.topicus.sql2.operation.CloudSpannerOperation;

public class CloudSpannerCloseOperation extends CloudSpannerOperation<Void>
{
	private final CloudSpannerConnection connection;

	CloudSpannerCloseOperation(Executor exec, CloudSpannerConnection connection)
	{
		super(exec);
		this.connection = connection;
	}

	@Override
	public Void get()
	{
		try
		{
			connection.doClose();
		}
		catch (SQLException e)
		{
			handle(e);
		}
		return null;
	}

}
