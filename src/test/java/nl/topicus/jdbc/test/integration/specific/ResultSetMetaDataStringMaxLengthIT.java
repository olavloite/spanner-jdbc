package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class ResultSetMetaDataStringMaxLengthIT extends AbstractSpecificIntegrationTest {
  /**
   * STRING(MAX) columns have an actual max length of 2621440 (see
   * https://cloud.google.com/spanner/docs/data-definition-language#scalars)
   */
  private static final int STRING_MAX_LENGTH = 2621440;

  /**
   * BYTES(MAX) columns have an actual max length of 10485760 (see
   * https://cloud.google.com/spanner/docs/data-definition-language#scalars)
   */
  private static final int BYTES_MAX_LENGTH = 10485760;

  @Test
  public void testStringMaxColumnLength() throws SQLException {
    try (
        ResultSet rs = getConnection().createStatement().executeQuery("select * from test_table")) {
      ResultSetMetaData metadata = rs.getMetaData();
      assertEquals(100, metadata.getPrecision(2));
      assertEquals(STRING_MAX_LENGTH, metadata.getPrecision(3));
      assertEquals(100, metadata.getPrecision(4));
      assertEquals(BYTES_MAX_LENGTH, metadata.getPrecision(5));
    }
  }

  @Before
  public void before() throws SQLException {
    createTestTable();
    insertRecords();
  }

  @After
  public void after() throws SQLException {
    dropTable();
  }

  private void createTestTable() throws SQLException {
    getConnection().createStatement().executeUpdate(
        "create table test_table (id int64 not null, name string(100), description string(max), data1 bytes(100), data2 bytes(max)) primary key (id)");
  }

  private void insertRecords() throws SQLException {
    String sql = "insert into test_table (id, name) values (?, ?)";
    PreparedStatement ps = getConnection().prepareStatement(sql);

    for (long l = 1L; l <= 10; l++) {
      ps.setLong(1, l);
      ps.setString(2, String.format("record %d", l));
      ps.setString(3, String.format("description of record %d", l));
      ps.setBytes(4, String.format("record %d", l).getBytes());
      ps.setBytes(5, String.format("description of record %d", l).getBytes());
      ps.addBatch();
      ps.clearParameters();
    }
    ps.executeBatch();
    getConnection().commit();

    // check record count
    assertEquals(10L, getRecordCount());
  }

  private long getRecordCount() throws SQLException {
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select count(*) from test_table")) {
      if (rs.next())
        return rs.getLong(1);
    }
    return -1L;
  }

  private void dropTable() throws SQLException {
    getConnection().createStatement().executeUpdate("drop table test_table");
  }

}
