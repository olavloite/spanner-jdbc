package nl.topicus.jdbc.emulator;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

public class SpannerEmulator implements Spanner
{
	private SpannerOptions options;

	@Override
	public SpannerOptions getOptions()
	{
		return options;
	}

	@Override
	public DatabaseAdminClient getDatabaseAdminClient()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InstanceAdminClient getInstanceAdminClient()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatabaseClient getDatabaseClient(DatabaseId db)
	{
		String url = "jdbc:postgresql://localhost/" + db.getDatabase();
		return new DatabaseClientEmulator(url);
	}

	@Override
	public BatchClient getBatchClient(DatabaseId db)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close()
	{
		// TODO Auto-generated method stub

	}

}
