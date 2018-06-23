package nl.topicus.jdbc.test.integration.specific;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import nl.topicus.jdbc.CloudSpannerConnection;

public abstract class AbstractSpecificIntegrationTest
{
	protected static final String CLOUDSPANNER_HOST = "https://spanner.googleapis.com";

	private static final String DEFAULT_HOST = "https://emulator.googlecloudspanner.com:8443";
	private static final String DEFAULT_PROJECT = "test-project";
	private static final String DEFAULT_KEY_FILE = "cloudspanner-emulator-key.json";
	private static final String DATABASE_ID = "test-database";

	private static Spanner spanner;
	private static String projectId;
	private static String instanceId;
	private static DatabaseId databaseId;
	private static String credentialsPath;

	private Connection connection;

	public static String getHost()
	{
		return System.getProperty("host", DEFAULT_HOST);
	}

	public static String getKeyFile()
	{
		return System.getProperty("keyfile", DEFAULT_KEY_FILE);
	}

	public static String getProject()
	{
		return System.getProperty("project", DEFAULT_PROJECT);
	}

	protected static Spanner getSpanner()
	{
		return spanner;
	}

	protected static DatabaseId getDatabaseId()
	{
		return databaseId;
	}

	protected DatabaseClient getDatabaseClient()
	{
		return spanner.getDatabaseClient(getDatabaseId());
	}

	protected DatabaseAdminClient getDatabaseAdminClient()
	{
		return spanner.getDatabaseAdminClient();
	}

	protected boolean isRunningOnEmulator()
	{
		return !CLOUDSPANNER_HOST.equalsIgnoreCase(getHost());
	}

	@BeforeClass
	public static void setup() throws IOException, InterruptedException
	{
		createSpanner();
		createInstance();
		createDatabase();
	}

	private static void createSpanner() throws IOException
	{
		// generate a unique instance id for this test run
		Random rnd = new Random();
		instanceId = "test-instance-" + rnd.nextInt(1000000);
		credentialsPath = getKeyFile();
		projectId = getProject();
		GoogleCredentials credentials = CloudSpannerConnection.getCredentialsFromFile(credentialsPath);
		Builder builder = SpannerOptions.newBuilder();
		builder.setProjectId(projectId);
		builder.setCredentials(credentials);
		builder.setHost(getHost());

		SpannerOptions options = builder.build();
		spanner = options.getService();
	}

	@AfterClass
	public static void teardown()
	{
		cleanUpDatabase();
		cleanUpInstance();
		spanner.close();
	}

	private static void createInstance()
	{
		InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
		InstanceConfig config = instanceAdminClient.getInstanceConfig("regional-europe-west1");
		Instance instance = instanceAdminClient.newInstanceBuilder(InstanceId.of(projectId, instanceId))
				.setDisplayName("Test Instance").setInstanceConfigId(config.getId()).setNodeCount(1).build();
		Operation<Instance, CreateInstanceMetadata> createInstance = instanceAdminClient.createInstance(instance);
		createInstance = createInstance.waitFor();
	}

	private static void createDatabase()
	{
		Operation<Database, CreateDatabaseMetadata> createDatabase = spanner.getDatabaseAdminClient()
				.createDatabase(instanceId, DATABASE_ID, Arrays.asList());
		createDatabase = createDatabase.waitFor();
		databaseId = DatabaseId.of(InstanceId.of(projectId, instanceId), DATABASE_ID);
	}

	private static void cleanUpInstance()
	{
		spanner.getInstanceAdminClient().deleteInstance(instanceId);
	}

	private static void cleanUpDatabase()
	{
		spanner.getDatabaseAdminClient().dropDatabase(instanceId, DATABASE_ID);
	}

	protected Connection getConnection()
	{
		return connection;
	}

	@Before
	public void setupConnection() throws SQLException
	{
		StringBuilder url = new StringBuilder("jdbc:cloudspanner:");
		url.append(getHost());
		url.append(";Project=").append(projectId);
		url.append(";Instance=").append(instanceId);
		url.append(";Database=").append(DATABASE_ID);
		url.append(";PvtKeyPath=").append(credentialsPath);
		url.append(";UseCustomHost=true");
		connection = DriverManager.getConnection(url.toString());
		connection.setAutoCommit(false);
	}

	@After
	public void closeConnection() throws SQLException
	{
		if (connection != null)
		{
			connection.close();
			connection = null;
		}
	}

}
