package nl.topicus.sql2;

import java.sql.SQLException;
import java.util.concurrent.Executor;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.rpc.Code;

import nl.topicus.java.sql2.Connection.Lifecycle;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.sql2.connectionproperty.CloudSpannerCredentialsPathConnectionProperty;
import nl.topicus.sql2.connectionproperty.CloudSpannerDatabaseConnectionProperty;
import nl.topicus.sql2.connectionproperty.CloudSpannerInstanceConnectionProperty;
import nl.topicus.sql2.connectionproperty.CloudSpannerOAuthTokenConnectionProperty;
import nl.topicus.sql2.connectionproperty.CloudSpannerProjectConnectionProperty;
import nl.topicus.sql2.operation.CloudSpannerOperation;

public class CloudSpannerConnectOperation extends CloudSpannerOperation<Void>
{
	private final ConnectionProperties connectionProperties;

	CloudSpannerConnectOperation(Executor exec, CloudSpannerConnection connection,
			ConnectionProperties connectionProperties)
	{
		super(exec, connection);
		this.connectionProperties = connectionProperties;
	}

	@Override
	public Void get()
	{
		try
		{
			Spanner spanner;
			String clientId = null;
			DatabaseClient dbClient;
			DatabaseAdminClient adminClient;

			String project = (String) connectionProperties.get(CloudSpannerProjectConnectionProperty.class);
			String instance = (String) connectionProperties.get(CloudSpannerInstanceConnectionProperty.class);
			String database = (String) connectionProperties.get(CloudSpannerDatabaseConnectionProperty.class);
			String credentialsPath = (String) connectionProperties
					.get(CloudSpannerCredentialsPathConnectionProperty.class);
			String oauthToken = (String) connectionProperties.get(CloudSpannerOAuthTokenConnectionProperty.class);
			Builder builder = SpannerOptions.newBuilder();
			builder.setProjectId(project);
			GoogleCredentials credentials = null;
			if (credentialsPath != null)
			{
				credentials = nl.topicus.jdbc.CloudSpannerConnection.getCredentialsFromFile(credentialsPath);
				builder.setCredentials(credentials);
			}
			else if (oauthToken != null)
			{
				credentials = nl.topicus.jdbc.CloudSpannerConnection.getCredentialsFromOAuthToken(oauthToken);
				builder.setCredentials(credentials);
			}
			if (credentials != null)
			{
				if (credentials instanceof UserCredentials)
				{
					clientId = ((UserCredentials) credentials).getClientId();
				}
				if (credentials instanceof ServiceAccountCredentials)
				{
					clientId = ((ServiceAccountCredentials) credentials).getClientId();
				}
			}

			SpannerOptions options = builder.build();
			spanner = options.getService();
			dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instance, database));
			adminClient = spanner.getDatabaseAdminClient();
			getConnection().doConnect(spanner, clientId, dbClient, adminClient);
		}
		catch (SpannerException e)
		{
			getConnection().setLifecycle(Lifecycle.CLOSED);
			handle(new CloudSpannerSQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(),
					e));
		}
		catch (SQLException e)
		{
			getConnection().setLifecycle(Lifecycle.CLOSED);
			handle(e);
		}
		catch (Exception e)
		{
			getConnection().setLifecycle(Lifecycle.CLOSED);
			handle(new CloudSpannerSQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(),
					Code.UNKNOWN, e));
		}
		return null;
	}

}
