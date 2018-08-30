package nl.topicus.jdbc.test.integration.specific;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class InsertStatementWithGetTimestampIT extends AbstractSpecificIntegrationTest {

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
  public void testInsertWithGetTimestampWithSelect() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String sql = "INSERT INTO providers\n"
        + "(provider_id, created, device_id, document_number, document_type_id, name, notification, phone, updated, last_access, deleted, token_fcm)\n"
        + "SELECT '000000', CURRENT_DATE(), 'not-device-id', null, null, 'provider-promocion', false, null, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), false, 'not-token'";
    statement.execute(sql);
  }

}
