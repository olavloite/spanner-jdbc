package nl.topicus.sql2;

import static nl.topicus.sql2.connectionproperty.CloudSpannerDatabaseConnectionProperty.DATABASE;
import static nl.topicus.sql2.connectionproperty.CloudSpannerInstanceConnectionProperty.INSTANCE;
import static nl.topicus.sql2.connectionproperty.CloudSpannerProjectConnectionProperty.PROJECT;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.java.sql2.Connection;
import nl.topicus.java.sql2.DataSource;
import nl.topicus.java.sql2.DataSourceFactory;
import nl.topicus.java.sql2.Submission;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDataSourceFactoryTest
{

	@Test
	public void testFactory() throws InterruptedException, ExecutionException
	{
		DataSourceFactory factory = CloudSpannerDataSourceFactory.INSTANCE;
		DataSource ds = factory.builder().connectionProperty(PROJECT, "test-project-123456")
				.connectionProperty(INSTANCE, "test-instance").connectionProperty(DATABASE, "test").build();
		try (Connection connection = ds.getConnection())
		{
			Submission<Long> res = connection.<Long> countOperation("insert into wordcount (word, count) values (?, ?)")
					.set("1", "test").set("2", 100).submit();
			long count = res.toCompletableFuture().get();
			assertEquals(1l, count);
		}
	}

}
