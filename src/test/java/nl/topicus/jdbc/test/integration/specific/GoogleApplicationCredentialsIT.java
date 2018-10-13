package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;
import nl.topicus.jdbc.test.util.EnvironmentVariablesUtil;

/**
 * Check that connecting to Cloud Spanner works when there is no key file specified in the URL, but
 * there is a default application credentials file defined in the environment. Injecting the
 * credentials environment variable is based on the blog post of Sebastian Daschner:
 * https://blog.sebastian-daschner.com/entries/changing_env_java.
 *
 * @author loite
 *
 */
@Category(IntegrationTest.class)
public class GoogleApplicationCredentialsIT extends AbstractSpecificIntegrationTest {
  @Rule
  public final EnvironmentVariables env = new EnvironmentVariables();

  @Override
  @Before
  public void setupConnection() throws SQLException {
    env.set("GOOGLE_APPLICATION_CREDENTIALS", getKeyFile());
    StringBuilder url = new StringBuilder("jdbc:cloudspanner:");
    url.append(getHost());
    url.append(";Project=").append(projectId);
    url.append(";Instance=").append(instanceId);
    url.append(";Database=").append(DATABASE_ID);
    url.append(";UseCustomHost=true");
    connection = DriverManager.getConnection(url.toString());
    connection.setAutoCommit(false);
  }

  @After
  public void clearDefaultCredentials() throws Exception {
    env.clear("GOOGLE_APPLICATION_CREDENTIALS");
    EnvironmentVariablesUtil.clearCachedDefaultCredentials();
  }

  @Test
  public void testWithGoogleApplicationCredentials() throws SQLException {
    String sql = "select 1 as one, 'two' as two, 3.0 as three";
    testSqlStatement(sql);
  }

  private void testSqlStatement(String sql) throws SQLException {
    testSqlStatement(sql, 1, true);
  }

  private void testSqlStatement(String sql, int expectedCount, boolean checkMetaData,
      Object... params) throws SQLException {
    PreparedStatement ps = getConnection().prepareStatement(sql);
    if (params != null) {
      int index = 1;
      for (Object param : params) {
        ps.setObject(index, param);
        index++;
      }
    }
    int count = 0;
    try (ResultSet rs = ps.executeQuery()) {
      if (checkMetaData) {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals("one", metadata.getColumnLabel(1));
        assertEquals("two", metadata.getColumnLabel(2));
        assertEquals("three", metadata.getColumnLabel(3));
        assertEquals(Types.BIGINT, metadata.getColumnType(1));
        assertEquals(Types.NVARCHAR, metadata.getColumnType(2));
        assertEquals(Types.DOUBLE, metadata.getColumnType(3));
      }
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(expectedCount, count);
  }
}
