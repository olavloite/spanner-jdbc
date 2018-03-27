package nl.topicus.sql2;

import static nl.topicus.sql2.connectionproperty.CloudSpannerCredentialsPathConnectionProperty.PVTKEYPATH;
import static nl.topicus.sql2.connectionproperty.CloudSpannerDatabaseConnectionProperty.DATABASE;
import static nl.topicus.sql2.connectionproperty.CloudSpannerInstanceConnectionProperty.INSTANCE;
import static nl.topicus.sql2.connectionproperty.CloudSpannerProjectConnectionProperty.PROJECT;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import nl.topicus.java.sql2.Submission;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDataSourceFactoryTest
{
	private static final class PrintErrorMessageHandler implements Function<Throwable, Void>
	{
		public static final PrintErrorMessageHandler INSTANCE = new PrintErrorMessageHandler();

		private PrintErrorMessageHandler()
		{
		}

		@Override
		public Void apply(Throwable t)
		{
			t.printStackTrace(System.err);
			return null;
		}

	}

	private static final boolean CREATE_DB = false;
	private static final String PROJECT_ID = "gothic-calling-193809";
	private static final String INSTANCE_ID = "test-instance";
	private static final String DATABASE_ID = "test";

	@Test
	public void testFactory() throws InterruptedException, ExecutionException, SQLException
	{
		CloudSpannerDataSourceFactory factory = CloudSpannerDataSourceFactory.INSTANCE;
		CloudSpannerDataSource ds = factory.builder().connectionProperty(PROJECT, PROJECT_ID)
				.connectionProperty(INSTANCE, INSTANCE_ID).connectionProperty(DATABASE, DATABASE_ID)
				.connectionProperty(PVTKEYPATH, "cloudspanner-key.json").build();
		Spanner spanner = null;
		try (CloudSpannerConnection connection = ds.getConnection(PrintErrorMessageHandler.INSTANCE))
		{
			if (CREATE_DB)
			{
				spanner = connection.getSpanner();
				createInstance(spanner, PROJECT_ID, INSTANCE_ID);
				createDatabase(spanner, INSTANCE_ID, DATABASE_ID);
			}

			Submission<Long> createResult = connection.<Long> countOperation(
					"create table if not exists wordcount (word string(100) not null, count int64 not null) primary key (word)")
					.submit();
			// assertEquals(1l,
			// createResult.toCompletableFuture().get().longValue());

			// Clear table
			connection.<Long> countOperation("delete from wordcount").submit();

			for (int i = 0; i < 100; i++)
			{
				Submission<Long> res = connection
						.<Long> countOperation("insert into wordcount (word, count) values (?, ?)").set("1", "test" + i)
						.set("2", 100 + i).submit();
				// assertEquals(1l,
				// res.toCompletableFuture().get().longValue());
			}

			Submission<List<Map<String, Object>>> selectSubmission = connection
					.<List<Map<String, Object>>> rowOperation("select word, count from wordcount order by count")
					.initialValue(ArrayList::new).rowAggregator((list, row) -> {
						Map<String, Object> map = new HashMap<>();
						for (String key : row.getIdentifiers())
						{
							map.put(key, row.get(key, Object.class));
						}
						list.add(map);
						return list;
					}).submit();
			List<Map<String, Object>> queryResult = selectSubmission.toCompletableFuture().get();
			assertEquals(100l, queryResult.size());
			for (int i = 0; i < 100; i++)
			{
				assertEquals("test" + i, queryResult.get(i).get("word"));
				assertEquals(100L + i, queryResult.get(i).get("count"));
			}

			int maxCount = 150;
			selectSubmission = connection
					.<List<Map<String, Object>>> rowOperation(
							"select word, count from wordcount where count<? order by count")
					.set("1", maxCount).initialValue(ArrayList::new).rowAggregator((list, row) -> {
						Map<String, Object> map = new HashMap<>();
						for (String key : row.getIdentifiers())
						{
							map.put(key, row.get(key, Object.class));
						}
						list.add(map);
						return list;
					}).submit();
			queryResult = selectSubmission.toCompletableFuture().get();
			assertEquals(200l - maxCount, queryResult.size());
			for (int i = 0; i < (200 - maxCount); i++)
			{
				assertEquals("test" + i, queryResult.get(i).get("word"));
				assertEquals(100L + i, queryResult.get(i).get("count"));
			}
		}
		finally
		{
			if (spanner != null)
			{
				cleanUpDatabase(spanner, INSTANCE_ID, DATABASE_ID);
				cleanUpInstance(spanner, INSTANCE_ID);
			}
		}
	}

	private void createInstance(Spanner spanner, String projectId, String instanceId)
	{
		InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
		InstanceConfig config = instanceAdminClient.getInstanceConfig("regional-europe-west1");
		Instance instance = instanceAdminClient.newInstanceBuilder(InstanceId.of(projectId, instanceId))
				.setDisplayName("Test Instance").setInstanceConfigId(config.getId()).setNodeCount(1).build();
		Operation<Instance, CreateInstanceMetadata> createInstance = instanceAdminClient.createInstance(instance);
		createInstance = createInstance.waitFor();
	}

	private void createDatabase(Spanner spanner, String instanceId, String databaseId)
	{
		Operation<Database, CreateDatabaseMetadata> createDatabase = spanner.getDatabaseAdminClient()
				.createDatabase(instanceId, databaseId, Arrays.asList());
		createDatabase = createDatabase.waitFor();
	}

	private void cleanUpInstance(Spanner spanner, String instanceId)
	{
		spanner.getInstanceAdminClient().deleteInstance(instanceId);
	}

	private void cleanUpDatabase(Spanner spanner, String instanceId, String databaseId)
	{
		spanner.getDatabaseAdminClient().dropDatabase(instanceId, databaseId);
	}

}
