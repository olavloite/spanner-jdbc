package nl.topicus.jdbc.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.rpc.Code;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.IntegrationTest;
import nl.topicus.jdbc.test.integration.ddl.MetaDataTester;
import nl.topicus.jdbc.test.integration.ddl.TableDDLTester;
import nl.topicus.jdbc.test.integration.dml.DMLTester;
import nl.topicus.jdbc.test.integration.xa.XATester;

@Category(IntegrationTest.class)
public class CloudSpannerIT
{
	private static final Logger log = Logger.getLogger(CloudSpannerIT.class.getName());

	private static final boolean CREATE_INSTANCE = true;

	private static final boolean CREATE_DATABASE = true;

	private final String instanceId;

	private static final String DATABASE_ID = "test-database";

	private Spanner spanner;

	private final String projectId;

	private final String credentialsPath;

	public CloudSpannerIT()
	{
		// generate a unique instance id for this test run
		Random rnd = new Random();
		this.instanceId = "test-instance-" + rnd.nextInt(1000000);
		this.credentialsPath = "cloudspanner-key.json";
		this.projectId = CloudSpannerConnection.getServiceAccountProjectId(credentialsPath);
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
	@Test
	public void performDatabaseTests() throws Exception
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
		finally
		{
			// Clean up test instance and test database.
			log.info("Cleaning up database");
			if (CREATE_DATABASE)
				cleanUpDatabase();
			if (CREATE_INSTANCE)
				cleanUpInstance();
			spanner.close();
			log.info("Clean up completed");
		}
	}

	private void performJdbcTests() throws Exception
	{
		// Get a JDBC connection
		try (Connection connection = createConnection())
		{
			connection.setAutoCommit(false);
			// Test connection validity
			assertTrue(connection.isValid(0));
			assertTrue(connection.isValid(1));
			assertTrue(connection.isValid(1000));
			// Check node count
			assertEquals(1, ((CloudSpannerConnection) connection).getNodeCount());
			// Test connection pooling
			ConnectionPoolingTester poolingTester = new ConnectionPoolingTester();
			poolingTester.testPooling((CloudSpannerConnection) connection);

			// Test Table DDL statements
			TableDDLTester tableDDLTester = new TableDDLTester(connection);
			tableDDLTester.runCreateTests();
			// Test DML statements
			DMLTester dmlTester = new DMLTester(connection);
			dmlTester.runDMLTests();
			// Test meta data functions
			MetaDataTester metaDataTester = new MetaDataTester((CloudSpannerConnection) connection);
			metaDataTester.runMetaDataTests();
			// Test transaction functions
			TransactionTester txTester = new TransactionTester(connection);
			txTester.runTransactionTests();
			// Test select statements
			SelectStatementsTester selectTester = new SelectStatementsTester(connection);
			selectTester.runSelectTests();
			// Test XA transactions
			XATester xaTester = new XATester();
			xaTester.testXA(projectId, instanceId, DATABASE_ID, credentialsPath);

			// Test drop statements
			tableDDLTester.runDropTests();
		}
		catch (SQLException | PropertyVetoException | AssertionError e)
		{
			log.log(Level.WARNING, "Error during JDBC tests", e);
			throw e;
		}
	}

	private Connection createConnection() throws SQLException
	{
		try
		{
			Class.forName(CloudSpannerDriver.class.getName());
		}
		catch (ClassNotFoundException e)
		{
			throw new CloudSpannerSQLException("Could not load JDBC driver", Code.UNKNOWN, e);
		}
		StringBuilder url = new StringBuilder("jdbc:cloudspanner://localhost");
		url.append(";Project=").append(projectId);
		url.append(";Instance=").append(instanceId);
		url.append(";Database=").append(DATABASE_ID);
		url.append(";PvtKeyPath=").append(credentialsPath);
		return DriverManager.getConnection(url.toString());
	}

	private void createInstance()
	{
		InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
		InstanceConfig config = instanceAdminClient.getInstanceConfig("regional-europe-west1");
		Instance instance = instanceAdminClient.newInstanceBuilder(InstanceId.of(projectId, instanceId))
				.setDisplayName("Test Instance").setInstanceConfigId(config.getId()).setNodeCount(1).build();
		Operation<Instance, CreateInstanceMetadata> createInstance = instanceAdminClient.createInstance(instance);
		createInstance = createInstance.waitFor();
	}

	private void createDatabase()
	{
		Operation<Database, CreateDatabaseMetadata> createDatabase = spanner.getDatabaseAdminClient()
				.createDatabase(instanceId, DATABASE_ID, Arrays.asList());
		createDatabase = createDatabase.waitFor();
	}

	private void cleanUpInstance()
	{
		spanner.getInstanceAdminClient().deleteInstance(instanceId);
	}

	private void cleanUpDatabase()
	{
		spanner.getDatabaseAdminClient().dropDatabase(instanceId, DATABASE_ID);
	}

}
