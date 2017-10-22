package nl.topicus.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class LoggerTest
{
	private static final String DEBUG_STRING = "DEBUG";

	private static final String INFO_STRING = "INFO";

	private static final String THROWABLE_STRING = "THROWABLE";

	private static final Integer CONNECTION_ID = 9999;

	@Test
	public void testNoLogger()
	{
		// Test that no exception occurs although no log has been set
		Logger logger = new Logger();
		logger.info("TEST");
	}

	@Test
	public void testDriverLogger()
	{
		testLogger(null, "driver");
	}

	@Test
	public void testConnectionLogger()
	{
		testLogger(CONNECTION_ID, "9999");
	}

	@Test
	public void testThrowableLogger()
	{
		Exception e = new Exception("TEST_EXCEPTION");
		testLogger(CONNECTION_ID, "9999", e);
	}

	private void testLogger(Integer connectionId, String expectedConnectionId)
	{
		testLogger(connectionId, expectedConnectionId, null);
	}

	private void testLogger(Integer connectionId, String expectedConnectionId, Throwable t)
	{
		StringWriter writer = new StringWriter();
		PrintWriter out = new PrintWriter(writer);
		DriverManager.setLogWriter(out);
		Logger logger = connectionId == null ? new Logger() : new Logger(connectionId);
		logger.setLogLevel(CloudSpannerDriver.INFO);
		logger.info(INFO_STRING);
		logger.debug(DEBUG_STRING);
		assertTrue(writer.toString().contains(INFO_STRING));
		assertFalse(writer.toString().contains(DEBUG_STRING));
		logger.setLogLevel(CloudSpannerDriver.DEBUG);
		logger.debug(DEBUG_STRING);
		assertTrue(writer.toString().contains(DEBUG_STRING));
		assertTrue(writer.toString().contains(expectedConnectionId));
		logger.info(THROWABLE_STRING, t);
		assertTrue(writer.toString().contains(THROWABLE_STRING));
		if (t == null)
			assertFalse(writer.toString().contains("Exception"));
		else
			assertTrue(writer.toString().contains(t.getMessage()));
	}

}
