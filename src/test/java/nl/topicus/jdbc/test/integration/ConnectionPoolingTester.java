package nl.topicus.jdbc.test.integration;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Assert;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import nl.topicus.jdbc.CloudSpannerConnection;

public class ConnectionPoolingTester
{
	private static final Logger log = Logger.getLogger(ConnectionPoolingTester.class.getName());

	public void testPooling(CloudSpannerConnection original)
			throws SQLException, PropertyVetoException, InterruptedException
	{
		log.info("Starting connection pooling tests");
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setDriverClass("nl.topicus.jdbc.CloudSpannerDriver");
		cpds.setJdbcUrl(original.getUrl());
		cpds.setProperties(original.getSuppliedProperties());

		cpds.setInitialPoolSize(5);
		cpds.setMinPoolSize(5);
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(20);
		cpds.setCheckoutTimeout(1000);

		log.info("Connection pool created and configured. Acquiring connection from pool");
		Assert.assertEquals(0, cpds.getNumBusyConnectionsDefaultUser());
		Connection connection = cpds.getConnection();
		Assert.assertNotNull(connection);
		Assert.assertEquals(1, cpds.getNumBusyConnectionsDefaultUser());
		connection.close();
		while (cpds.getNumBusyConnections() == 1)
		{
			TimeUnit.MILLISECONDS.sleep(100L);
		}
		Assert.assertEquals(0, cpds.getNumBusyConnectionsDefaultUser());

		log.info("About to acquire 10 connections");
		Connection[] connections = new Connection[10];
		for (int i = 0; i < connections.length; i++)
		{
			connections[i] = cpds.getConnection();
			Assert.assertEquals(i + 1, cpds.getNumBusyConnectionsDefaultUser());
		}
		log.info("10 connections acquired, closing connections...");
		for (int i = 0; i < connections.length; i++)
		{
			connections[i].close();
		}
		log.info("10 connections closed");
		log.info("Acquiring 20 connections");
		// Check that we can get 20 connections
		connections = new Connection[20];
		for (int i = 0; i < connections.length; i++)
		{
			connections[i] = cpds.getConnection();
			connections[i].prepareStatement("SELECT 1").executeQuery();
		}
		log.info("20 connections acquired, trying to get one more");
		// Verify that we can't get a connection now
		connection = null;
		try
		{
			connection = cpds.getConnection();
		}
		catch (SQLException e)
		{
			// timeout exception
			log.info("Exception when trying to get one more connection (this is expected)");
		}
		log.info("Closing 20 connections");
		Assert.assertNull(connection);
		for (int i = 0; i < connections.length; i++)
		{
			connections[i].close();
		}
		log.info("Closing connection pool");
		cpds.close();
		log.info("Finished tests");
	}

}
