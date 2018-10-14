package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class LoggerIT extends AbstractSpecificIntegrationTest {

  @Test
  public void testLogInfoLongTransaction() throws SQLException, InterruptedException {
    testLogLongTransaction(CloudSpannerDriver.INFO);
  }

  @Test
  public void testLogDebugLongTransaction() throws SQLException, InterruptedException {
    testLogLongTransaction(CloudSpannerDriver.DEBUG);
  }

  private void testLogLongTransaction(int logLevel) throws SQLException, InterruptedException {
    StringWriter writer = new StringWriter();
    DriverManager.setLogWriter(new PrintWriter(writer));
    CloudSpannerDriver.setLongTransactionTrigger(1000L);
    CloudSpannerConnection connection = (CloudSpannerConnection) getConnection();
    connection.setAutoCommit(false);
    connection.getLogger().setLogLevel(logLevel);
    try (ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
      while (rs.next()) {
      }
    }
    // Wait for 7 seconds to ensure the keep-alive query is triggered
    Thread.sleep(7000L);
    connection.commit();
    assertTrue(writer.toString().contains(
        "Transaction has been inactive for more than 5 seconds and will do a keep-alive query"));
    assertEquals(logLevel >= CloudSpannerDriver.DEBUG,
        writer.toString().contains("Transaction was started by: "));
  }

}
