package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

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

  @Override
  @Before
  public void setupConnection() throws SQLException {
    try {
      injectEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", getKeyFile());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    StringBuilder url = new StringBuilder("jdbc:cloudspanner:");
    url.append(getHost());
    url.append(";Project=").append(projectId);
    url.append(";Instance=").append(instanceId);
    url.append(";Database=").append(DATABASE_ID);
    url.append(";UseCustomHost=true");
    connection = DriverManager.getConnection(url.toString());
    connection.setAutoCommit(false);
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

  @SuppressWarnings("unchecked")
  private static void injectEnvironmentVariable(String key, String value) throws Exception {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

    Field unmodifiableMapField =
        getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);

    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(key, value);
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {

    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  @SuppressWarnings("unchecked")
  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {

    @SuppressWarnings("rawtypes")
    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    ((Map<String, String>) obj).put(key, value);
  }
}
