package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExtendedModeIT extends AbstractSpecificIntegrationTest {
  private static final int NUMBER_OF_ROWS = 100;

  @Before
  public void createAndFillTestTable() throws SQLException {
    if (tableExists("test"))
      return;
    getConnection().createStatement().execute(
        "create table test (id int64 not null, name string(100) not null) primary key (id)");
    getConnection().setAutoCommit(false);
    PreparedStatement ps =
        getConnection().prepareStatement("insert into test (id, name) values (?,?)");
    for (int i = 0; i < NUMBER_OF_ROWS; i++) {
      ps.setLong(1, i);
      ps.setString(2, String.valueOf(i));
      ps.addBatch();
    }
    ps.executeBatch();
    getConnection().commit();
  }

  @Test
  public void test1_ExtendedModeUpdate() throws SQLException {
    ((CloudSpannerConnection) getConnection()).setAllowExtendedMode(true);
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test order by id")) {
      int i = 0;
      while (rs.next()) {
        assertEquals(String.valueOf(i), rs.getString("name"));
        i++;
      }
    }
    PreparedStatement ps =
        getConnection().prepareStatement("update test set name=concat('one', name) where id < ?");
    ps.setLong(1, 10L);
    ps.executeUpdate();
    getConnection().commit();
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test order by id")) {
      int i = 0;
      while (rs.next()) {
        if (i < 10) {
          assertEquals("one" + String.valueOf(i), rs.getString("name"));
        } else {
          assertEquals(String.valueOf(i), rs.getString("name"));
        }
        i++;
      }
    }
  }

  @Test
  public void test2_ExtendedModeDelete() throws SQLException {
    ((CloudSpannerConnection) getConnection()).setAllowExtendedMode(true);
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select count(*) from test where id<10")) {
      while (rs.next()) {
        assertEquals(10L, rs.getLong(1));
      }
    }
    PreparedStatement ps = getConnection().prepareStatement("delete from test where id<?");
    ps.setLong(1, 10L);
    ps.executeUpdate();
    getConnection().commit();
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select count(*) from test where id<10")) {
      while (rs.next()) {
        assertEquals(0L, rs.getLong(1));
      }
    }
  }

  @Test
  public void test3_ExtendedModeDeleteWithId() throws SQLException {
    ((CloudSpannerConnection) getConnection()).setAllowExtendedMode(true);
    try (ResultSet rs = getConnection().createStatement()
        .executeQuery("select count(*) from test where id=30 and name like '3%'")) {
      while (rs.next()) {
        assertEquals(1L, rs.getLong(1));
      }
    }
    PreparedStatement ps =
        getConnection().prepareStatement("delete from test where id=? and name like ?");
    ps.setLong(1, 30L);
    ps.setString(2, "3%");
    ps.executeUpdate();
    getConnection().commit();
    try (ResultSet rs = getConnection().createStatement()
        .executeQuery("select count(*) from test where id=30 and name like '3%'")) {
      while (rs.next()) {
        assertEquals(0L, rs.getLong(1));
      }
    }
  }

}
