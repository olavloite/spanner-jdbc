package nl.topicus.jdbc.test.integration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class InstanceConfigIT
{

	@Test
	public void testEuropeWestSingleNodeConfig()
	{
		String credentialsPath = "cloudspanner-key.json";
		String projectId = CloudSpannerConnection.getServiceAccountProjectId(credentialsPath);
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
		Spanner spanner = options.getService();

		InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
		InstanceConfig config = instanceAdminClient.getInstanceConfig("regional-europe-west1");
		assertEquals("regional-europe-west1", config.getId().getInstanceConfig());
		spanner.close();
	}

}
