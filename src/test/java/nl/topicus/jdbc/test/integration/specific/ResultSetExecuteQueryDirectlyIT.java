package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;

/**
 * Version 1.0.x of the JDBC driver had the same behavior as the Cloud Spanner Java client when it
 * comes to when a query was actually executed by Cloud Spanner. That is, when a statement was
 * created and executed in order to get a result set, the query was actually executed at the first
 * call to the {@link ResultSet#next()} method instead of when calling
 * {@link Statement#executeQuery(String)}. This could cause unexpected behavior when an invalid
 * query was submitted to the JDBC driver. Instead of throwing an exception when calling the
 * executeQuery(...) method, the driver would throw an exception when the {@link ResultSet#next()}
 * was called.
 * 
 * As of version 1.1 and further, the driver exhibits the behavior that you would expect from a JDBC
 * driver: An exception will be thrown when calling the executeQuery(...) method instead of at the
 * {@link ResultSet#next()} method.
 * 
 * @author loite
 *
 */
public class ResultSetExecuteQueryDirectlyIT extends AbstractSpecificIntegrationTest {

  @Before
  public void setupTable() throws SQLException {
    getConnection().createStatement().execute(
        "create table if not exists test (id int64 not null, name string(100)) primary key (id)");
    getConnection().createStatement()
        .execute("insert into test (id, name) values (1, 'one') on duplicate key update");
    getConnection().commit();
  }

  @Test
  public void testNormalSelect() throws SQLException {
    try (ResultSet rs = getConnection().createStatement().executeQuery("select * from test")) {
      int count = 0;
      assertTrue(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());
      while (rs.next()) {
        count++;
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
      }
      assertFalse(rs.isBeforeFirst());
      assertTrue(rs.isAfterLast());
      assertEquals(1, count);
    }
  }

  @Test
  public void testInvalidSelect() throws SQLException {
    try (ResultSet rs = getConnection().createStatement().executeQuery("select * from foo")) {
      fail("The executeQuery method should directly");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testSelectWithNoResults() throws SQLException {
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test where id<0")) {
      assertTrue(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());
      // should not find any rows
      assertFalse(rs.next());
      assertFalse(rs.isBeforeFirst());
      assertTrue(rs.isAfterLast());
    }
  }

  @Test
  public void testNormalSelectWitGetMetaData() throws SQLException {
    try (ResultSet rs = getConnection().createStatement().executeQuery("select * from test")) {
      int count = 0;
      assertEquals(2, rs.getMetaData().getColumnCount());
      assertTrue(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());
      while (rs.next()) {
        count++;
        assertNotNull(rs.getMetaData());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
      }
      assertNotNull(rs.getMetaData());
      assertFalse(rs.isBeforeFirst());
      assertTrue(rs.isAfterLast());
      assertEquals(1, count);
    }
  }

  @Test
  public void testSelectWithNoResultsAndGetMetaData() throws SQLException {
    try (ResultSet rs =
        getConnection().createStatement().executeQuery("select * from test where id<0")) {
      assertEquals(2, rs.getMetaData().getColumnCount());
      assertTrue(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());
      // should not find any rows
      assertFalse(rs.next());
      assertNotNull(rs.getMetaData());
      assertFalse(rs.isBeforeFirst());
      assertTrue(rs.isAfterLast());
    }
  }

}
