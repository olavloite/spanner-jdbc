package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UseServerDmlIT extends AbstractSpecificIntegrationTest {

  @Override
  protected void appendConnectionUrl(StringBuilder url) {
    url.append(";UseServerDML=true");
  }

  @Before
  public void setupTable() throws SQLException {
    getConnection().createStatement().execute(
        "create table if not exists test (id int64 not null, name string(100)) primary key (id)");
  }

  @Test
  public void test1_InsertWithoutParameters() throws SQLException {
    int updated = getConnection().createStatement()
        .executeUpdate("insert into test (id, name) values (1, 'one')");
    assertEquals(1, updated);
    getConnection().commit();
    try (ResultSet rs = getConnection().createStatement().executeQuery("select * from test")) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(1, count);
    }
  }

  @Test
  public void test2_InsertWithParameters() throws SQLException {
    try (PreparedStatement ps =
        getConnection().prepareStatement("insert into test (id, name) values (?, ?)")) {
      ps.setLong(1, 2L);
      ps.setString(2, "two");
      ps.execute();
      assertEquals(1, ps.getUpdateCount());
      getConnection().commit();
    }
    try (ResultSet rs = getConnection().createStatement().executeQuery("select * from test")) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(2, count);
    }
  }

  @Test
  public void test3_UpdateWithoutParameters() throws SQLException {
    int updated =
        getConnection().createStatement().executeUpdate("update test set name='to' where id=2");
    assertEquals(1, updated);
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test where id=2")) {
      assertTrue(rs.next());
      assertEquals("to", rs.getString("name"));
      assertFalse(rs.next());
    }
    getConnection().commit();
  }

  @Test
  public void test4_UpdateWithParameters() throws SQLException {
    try (PreparedStatement ps =
        getConnection().prepareStatement("update test set name=? where id=?")) {
      ps.setString(1, "en");
      ps.setLong(2, 1L);
      int updated = ps.executeUpdate();
      assertEquals(1, updated);
      try (ResultSet rs =
          getConnection().createStatement().executeQuery("select * from test where id=1")) {
        assertTrue(rs.next());
        assertEquals("en", rs.getString("name"));
        assertFalse(rs.next());
      }
      getConnection().commit();
    }
  }

  @Test
  public void test5_DeleteWithoutParameters() throws SQLException {
    int updated = getConnection().createStatement().executeUpdate("delete from test where id=2");
    assertEquals(1, updated);
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test where id=2")) {
      assertFalse(rs.next());
    }
    getConnection().commit();
  }

  @Test
  public void test6_DeleteWithParameters() throws SQLException {
    try (PreparedStatement ps = getConnection().prepareStatement("delete from test where id=?")) {
      ps.setLong(1, 1L);
      int updated = ps.executeUpdate();
      assertEquals(1, updated);
      try (ResultSet rs =
          getConnection().createStatement().executeQuery("select * from test where id=1")) {
        assertFalse(rs.next());
      }
      getConnection().commit();
    }
  }

}
