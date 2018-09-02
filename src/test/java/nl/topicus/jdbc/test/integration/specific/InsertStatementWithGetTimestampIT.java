package nl.topicus.jdbc.test.integration.specific;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.IntegrationTest;

/**
 * INSERT/UPDATE/DELETE statements that call a server side method like for example CURRENT_DATE() or
 * CURRENT_TIMESTAMP() must be run server side. That is, any INSERT/UPDATE/DELETE statement that
 * references one record and is only processed locally in the JDBC driver cannot include any server
 * side functions. Instead, these statements must be rewritten into doing a round-trip to the
 * database. For an INSERT statement, that means changing the insert statement into something like
 * this:
 * 
 * <pre>
 *   INSERT INTO FOO
 *   (COL1, COL2, COL3)
 *   SELECT 1, 'two', CURRENT_DATE()
 * </pre>
 * 
 * The following statement will fail, as it is translated into a mutation locally without a database
 * round trip:
 * 
 * <pre>
 *   INSERT INTO FOO
 *   (COL1, COL2, COL3)
 *   VALUES
 *   (1, 'two', CURRENT_DATE())
 * </pre>
 * 
 * @throws SQLException
 */
@Category(IntegrationTest.class)
public class InsertStatementWithGetTimestampIT extends AbstractSpecificIntegrationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() throws SQLException {
    String sql =
        "CREATE TABLE providers (provider_id string(100) not null, created date not null, device_id string(100) not null, "
            + "document_number int64, document_type_id int64, name string(100) not null, notification bool not null, "
            + "phone string(100), updated timestamp not null, last_access timestamp, deleted bool not null, token_fcm string(100)) primary key (provider_id)";
    getConnection().createStatement().execute(sql);
  }

  @After
  public void after() throws SQLException {
    String sql = "DROP TABLE providers";
    getConnection().createStatement().execute(sql);
  }

  @Test
  public void testInsertWithGetTimestamp() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String sql = "INSERT INTO providers\n"
        + "(provider_id, created, device_id, document_number, document_type_id, name, notification, phone, updated, last_access, deleted, token_fcm)\n"
        + "VALUES\n"
        + "('000000', CURRENT_DATE(), 'not-device-id', null, null, 'provider-promocion', false, null, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), false, 'not-token')";
    thrown.expect(CloudSpannerSQLException.class);
    thrown.expectMessage(
        "Function calls such as for example GET_TIMESTAMP() are not allowed in client side insert/update statements.");
    statement.execute(sql);
  }

  @Test
  public void testInsertWithGetTimestampWithSelect() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String sql = "INSERT INTO providers\n"
        + "(provider_id, created, device_id, document_number, document_type_id, name, notification, phone, updated, last_access, deleted, token_fcm)\n"
        + "SELECT\n"
        + "'000000', CURRENT_DATE(), 'not-device-id', null, null, 'provider-promocion', false, null, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), false, 'not-token'";
    statement.execute(sql);
  }

}
