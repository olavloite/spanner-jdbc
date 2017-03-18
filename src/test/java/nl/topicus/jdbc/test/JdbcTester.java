package nl.topicus.jdbc.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.ddl.TableDDLTester;
import nl.topicus.jdbc.test.dml.DMLTester;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

public class JdbcTester
{
	private static final Logger log = Logger.getLogger(JdbcTester.class.getName());

	private static final boolean CREATE_INSTANCE = true;

	private static final boolean CREATE_DATABASE = true;

	private static final String INSTANCE_ID = "test-instance";

	private static final String DATABASE_ID = "test-database";

	private Spanner spanner;

	private final String projectId;

	private final String credentialsPath;

	public JdbcTester(String projectId, String credentialsPath)
	{
		this.projectId = projectId;
		this.credentialsPath = credentialsPath;
		GoogleCredentials credentials = null;
		try
		{
			credentials = CloudSpannerConnection.getCredentialsFromFile(credentialsPath);
		}
		catch (IOException e)
		{
			throw new RuntimeException("Could not read key file " + credentialsPath, e);
		}
		Builder builder = SpannerOptions.newBuilder();
		builder.setProjectId(projectId);
		builder.setCredentials(credentials);

		SpannerOptions options = builder.build();
		spanner = options.getService();
	}

	/**
	 * Run the different tests on the configured database.
	 */
	void performTests()
	{
		try
		{
			log.info("Starting tests, about to create database");
			if (CREATE_INSTANCE)
				createInstance();
			if (CREATE_DATABASE)
				createDatabase();
			log.info("Database created");
			// Do the testing
			log.info("Starting JDBC tests");
			performJdbcTests();
			log.info("JDBC tests completed");
		}
		catch (Exception e)
		{
			throw new RuntimeException("Tests failed", e);
		}
		finally
		{
			// Clean up test instance and test database.
			log.info("Cleaning up database");
			if (CREATE_INSTANCE)
				cleanUpInstance();
			if (CREATE_DATABASE)
				cleanUpDatabase();
			spanner.closeAsync();
			log.info("Clean up completed");
		}
	}

	private void performJdbcTests() throws SQLException, IOException, URISyntaxException
	{
		// Get a JDBC connection
		try (Connection connection = createConnection())
		{
			// Test Table DDL statements
			TableDDLTester tableDDLTester = new TableDDLTester(connection);
			tableDDLTester.runCreateTests();
			// Test DML statements
			DMLTester dmlTester = new DMLTester(connection);
			dmlTester.runInsertAndUpdateTests();

			// Test drop statements
			tableDDLTester.runDropTests();
		}
		catch (SQLException e)
		{
			log.log(Level.WARNING, "Error during JDBC tests", e);
			throw e;
		}
	}

	private Connection createConnection() throws SQLException
	{
		StringBuilder url = new StringBuilder("jdbc:cloudspanner://localhost");
		url.append(";Project=").append(projectId);
		url.append(";Instance=").append(INSTANCE_ID);
		url.append(";Database=").append(DATABASE_ID);
		url.append(";PvtKeyPath=").append(credentialsPath);
		return DriverManager.getConnection(url.toString());
	}

	private void createInstance()
	{
		InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
		Iterator<InstanceConfig> configs = instanceAdminClient.listInstanceConfigs().iterateAll();
		InstanceConfigId configId = null;
		while (configs.hasNext())
		{
			InstanceConfig cfg = configs.next();
			if (cfg.getDisplayName().toLowerCase().contains("europe"))
			{
				configId = cfg.getId();
				break;
			}
		}
		Instance instance = instanceAdminClient.newInstanceBuilder(InstanceId.of(projectId, INSTANCE_ID))
				.setDisplayName("Test Instance").setInstanceConfigId(configId).setNodeCount(1).build();
		Operation<Instance, CreateInstanceMetadata> createInstance = instanceAdminClient.createInstance(instance);
		createInstance = createInstance.waitFor();
	}

	private void createDatabase()
	{
		Operation<Database, CreateDatabaseMetadata> createDatabase = spanner.getDatabaseAdminClient().createDatabase(
				INSTANCE_ID, DATABASE_ID, Arrays.asList());
		createDatabase = createDatabase.waitFor();
	}

	private void cleanUpInstance()
	{
		spanner.getInstanceAdminClient().deleteInstance(INSTANCE_ID);
	}

	private void cleanUpDatabase()
	{
		spanner.getDatabaseAdminClient().dropDatabase(INSTANCE_ID, DATABASE_ID);
	}

}
