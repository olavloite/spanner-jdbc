package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class NullValueKeyIT extends AbstractSpecificIntegrationTest {
  @Test
  public void runNullValueKeyTests() throws SQLException {
    createTestTable();
    insertRecords();
    updateRecords();
    deleteRecords();
    dropTable();
  }

  private void createTestTable() throws SQLException {
    String sql =
        "create table table_with_null_keys (id1 int64 not null, id2 int64, name string(100)) primary key (id1, id2)";
    getConnection().createStatement().executeUpdate(sql);
    try (ResultSet rs =
        getConnection().getMetaData().getColumns(null, null, "table_with_null_keys", null)) {
      while (rs.next()) {
        if (rs.getString("COLUMN_NAME").equals("id1"))
          assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
        else
          assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));
      }
    }
  }

  private void insertRecords() throws SQLException {
    String sql = "insert into table_with_null_keys (id1, id2, name) values (?, ?, ?)";
    PreparedStatement ps = getConnection().prepareStatement(sql);

    // records without null values
    ps.setLong(1, 1L);
    ps.setLong(2, 1L);
    ps.setString(3, "record without null values");
    ps.addBatch();

    ps.clearParameters();
    ps.setLong(1, 1L);
    ps.setLong(2, 2L);
    ps.setString(3, "record without null values");
    ps.addBatch();

    ps.clearParameters();
    ps.setLong(1, 2L);
    ps.setLong(2, 2L);
    ps.setString(3, "record without null values");
    ps.addBatch();

    // records with null values
    ps.clearParameters();
    ps.setLong(1, 3L);
    ps.setNull(2, Types.BIGINT);
    ps.setString(3, "record with null values");
    ps.addBatch();

    ps.clearParameters();
    ps.setLong(1, 1L);
    ps.setNull(2, Types.BIGINT);
    ps.setString(3, "record with null values");
    ps.addBatch();

    ps.executeBatch();
    getConnection().commit();

    // check record count
    assertEquals(5L, getRecordCount());
    // assert both non-null and null values
    assertEquals((Object) 1L, (Object) getId2(1L, 1L));
    assertNull(getId2(1L, null));
    assertNull(getId2(3L, null));
  }

  private void updateRecords() throws SQLException {
    String sql = "update table_with_null_keys set name=? where id1=? and id2=?";
    PreparedStatement ps = getConnection().prepareStatement(sql);

    // records without null values
    ps.setString(1, "updated");
    ps.setLong(2, 1L);
    ps.setLong(3, 1L);
    int updateCount = ps.executeUpdate();
    getConnection().commit();
    assertEquals(1, updateCount);
    assertEquals("updated", getName(1L, 1L));

    // records with null values
    ps.clearParameters();
    ps.setString(1, "updated");
    ps.setLong(2, 3L);
    ps.setNull(3, Types.BIGINT);
    updateCount = ps.executeUpdate();
    getConnection().commit();
    assertEquals(1, updateCount);
    assertEquals("updated", getName(3L, null));
  }

  private void deleteRecords() throws SQLException {
    String sql = "delete from table_with_null_keys where id1=? and id2=?";
    PreparedStatement ps = getConnection().prepareStatement(sql);

    // records without null values
    ps.setLong(1, 1L);
    ps.setLong(2, 1L);
    int updateCount = ps.executeUpdate();
    getConnection().commit();
    assertEquals(1, updateCount);
    assertEquals(4, getRecordCount());

    // records with null values
    ps = getConnection().prepareStatement(sql);
    ps.setLong(1, 3L);
    // ps.setNull(2, Types.BIGINT);
    updateCount = ps.executeUpdate();
    getConnection().commit();
    assertEquals(1, updateCount);
    assertEquals(3, getRecordCount());
  }

  private void dropTable() throws SQLException {
    String sql = "drop table table_with_null_keys";
    getConnection().createStatement().executeUpdate(sql);
  }

  private long getRecordCount() throws SQLException {
    try (ResultSet rs = getConnection().createStatement()
        .executeQuery("select count(*) from table_with_null_keys")) {
      if (rs.next())
        return rs.getLong(1);
    }
    return -1L;
  }

  private String getName(Long id1, Long id2) throws SQLException {
    String sql = String.format("select name from table_with_null_keys where id1=? and %s",
        id2 == null ? "id2 is null" : "id2=?");
    PreparedStatement ps = getConnection().prepareStatement(sql);
    if (id1 != null)
      ps.setLong(1, id1);
    if (id2 != null)
      ps.setLong(2, id2);
    int count = 0;
    String res = null;
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        res = rs.getString(1);
        count++;
      }
    }
    assertEquals(1, count);
    return res;
  }

  private Long getId2(Long id1, Long id2) throws SQLException {
    String sql = String.format("select id2 from table_with_null_keys where id1=? and %s",
        id2 == null ? "id2 is null" : "id2=?");
    PreparedStatement ps = getConnection().prepareStatement(sql);
    if (id1 != null)
      ps.setLong(1, id1);
    if (id2 != null)
      ps.setLong(2, id2);
    int count = 0;
    Long res = null;
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        res = rs.getLong(1);
        if (rs.wasNull())
          res = null;
        count++;
      }
    }
    assertEquals(1, count);
    return res;
  }

}
