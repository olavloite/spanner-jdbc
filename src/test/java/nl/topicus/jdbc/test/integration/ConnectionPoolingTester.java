package nl.topicus.jdbc.test.integration;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Assert;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import nl.topicus.jdbc.CloudSpannerConnection;

public class ConnectionPoolingTester
{
	public void testPooling(CloudSpannerConnection original)
			throws SQLException, PropertyVetoException, InterruptedException
	{
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setDriverClass("nl.topicus.jdbc.CloudSpannerDriver");
		cpds.setJdbcUrl(original.getUrl());
		cpds.setProperties(original.getSuppliedProperties());

		cpds.setInitialPoolSize(5);
		cpds.setMinPoolSize(5);
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(20);
		cpds.setCheckoutTimeout(1000);

		Assert.assertEquals(0, cpds.getNumBusyConnectionsDefaultUser());
		Connection connection = cpds.getConnection();
		Assert.assertNotNull(connection);
		Assert.assertEquals(1, cpds.getNumBusyConnectionsDefaultUser());
		connection.close();
		while (cpds.getNumBusyConnections() == 1)
		{
			Thread.sleep(100l);
		}
		Assert.assertEquals(0, cpds.getNumBusyConnectionsDefaultUser());

		Connection[] connections = new Connection[10];
		for (int i = 0; i < connections.length; i++)
		{
			connections[i] = cpds.getConnection();
			Assert.assertEquals(i + 1, cpds.getNumBusyConnectionsDefaultUser());
		}
		for (int i = 0; i < connections.length; i++)
		{
			connections[i].close();
		}
		// Check that we can get 20 connections
		connections = new Connection[20];
		for (int i = 0; i < connections.length; i++)
		{
			connections[i] = cpds.getConnection();
			connections[i].prepareStatement("SELECT 1").executeQuery();
		}
		// Verify that we can't get a connection now
		connection = null;
		try
		{
			connection = cpds.getConnection();
		}
		catch (SQLException e)
		{
			// timeout exception
		}
		Assert.assertNull(connection);
		for (int i = 0; i < connections.length; i++)
		{
			connections[i].close();
		}
		cpds.close();
	}

}
