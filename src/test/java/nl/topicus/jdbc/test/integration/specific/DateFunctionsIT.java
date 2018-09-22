package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.stream.LongStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class DateFunctionsIT extends AbstractSpecificIntegrationTest {

  private void createNewsTables() throws SQLException {
    // check whether the table already exists
    try (ResultSet rs = getConnection().getMetaData().getTables(null, null, "news_types", null)) {
      if (rs.next())
        return;
    }

    getConnection().createStatement().executeUpdate(
        "create table news_types (news_type_id int64 not null, name string(100), update_hours int64 not null, enabled bool not null, priority int64 not null, news_start timestamp) primary key (news_type_id)");
    getConnection().createStatement().executeUpdate(
        "create table news2 (news_id int64 not null, created timestamp not null, news_type_id int64 not null, seeker_id int64, text string(max)) primary key (news_id)");

    PreparedStatement psNewsType = getConnection().prepareStatement(
        "insert into news_types (news_type_id, name, update_hours, enabled, priority, news_start) values (?, ?, ?, ?, ?, ?)");
    psNewsType.setLong(1, 1L);
    psNewsType.setString(2, "test");
    psNewsType.setLong(3, 2L);
    psNewsType.setBoolean(4, true);
    psNewsType.setLong(5, 1L);
    psNewsType.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
    psNewsType.executeUpdate();

    PreparedStatement psNews = getConnection().prepareStatement(
        "insert into news2 (news_id, created, news_type_id, seeker_id, text) values (?, ?, ?, ?, ?)");
    LongStream.rangeClosed(1L, 10L).forEach(l -> {
      try {
        psNews.setLong(1, l);
        psNews.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        psNews.setLong(3, 1L);
        psNews.setLong(4, 1000L);
        psNews.setString(5, "News text " + l);
        psNews.executeUpdate();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    getConnection().commit();
  }

  @Test
  public void testSelectWithInterval() throws SQLException {
    createNewsTables();
    // @formatter:off
		String sql = "SELECT n.news_type_id, n.text "
				+ "FROM news2 AS n "
				+ "INNER JOIN news_types nt ON (nt.news_type_id = n.news_type_id) "
				+ "WHERE n.seeker_id = ? \n"
				+ "AND TIMESTAMP_ADD(n.created, INTERVAL nt.update_hours HOUR) > CURRENT_TIMESTAMP() \n"
				+ "AND nt.enabled = true \n"
				+ "ORDER BY nt.priority DESC "
				+ "LIMIT 100 ";
		// @formatter:on
    PreparedStatement ps = getConnection().prepareStatement(sql);
    ps.setLong(1, 1000L);
    int count = 0;
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        assertNotNull(metadata.getColumnLabel(i));
      }
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(10, count);
  }

  @Test
  public void testSelectWithExtract() throws SQLException {
    // @formatter:off
		String sql = "SELECT\r\n" + 
				"  timestamp,\r\n" + 
				"  EXTRACT(NANOSECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(MICROSECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(MILLISECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(SECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(MINUTE FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(HOUR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(DAYOFWEEK FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(DAY FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(DAYOFYEAR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(WEEK FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(DATE FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(ISOYEAR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT(ISOWEEK FROM timestamp) AS isoweek,\r\n" + 
				"  EXTRACT(YEAR FROM timestamp) AS year,\r\n" + 
				"  EXTRACT(WEEK FROM timestamp) AS week\r\n" + 
				"FROM (\r\n" + 
				"    SELECT TIMESTAMP '2005-01-03 12:34:56' AS timestamp UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2007-12-31' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2009-01-01' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2009-12-31' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2017-01-02' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2017-05-26'\r\n" + 
				"  ) AS Timestamps\r\n" + 
				"ORDER BY timestamp";
		// @formatter:on
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        assertNotNull(metadata.getColumnLabel(i));
      }
      while (rs.next()) {
      }
    }
  }

  @Test
  public void testSelectWithExtractAndSpaces() throws SQLException {
    // @formatter:off
		String sql = "SELECT\r\n" + 
				"  timestamp,\r\n" + 
				"  EXTRACT( NANOSECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( MICROSECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( MILLISECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( SECOND FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( MINUTE FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( HOUR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( DAYOFWEEK FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( DAY FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( DAYOFYEAR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( WEEK FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( DATE FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( ISOYEAR FROM timestamp) AS isoyear,\r\n" + 
				"  EXTRACT( ISOWEEK FROM timestamp) AS isoweek,\r\n" + 
				"  EXTRACT( YEAR FROM timestamp) AS year,\r\n" + 
				"  EXTRACT( WEEK FROM timestamp) AS week\r\n" + 
				"FROM (\r\n" + 
				"    SELECT TIMESTAMP '2005-01-03 12:34:56' AS timestamp UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2007-12-31' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2009-01-01' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2009-12-31' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2017-01-02' UNION ALL\r\n" + 
				"    SELECT TIMESTAMP '2017-05-26'\r\n" + 
				"  ) AS Timestamps\r\n" + 
				"ORDER BY timestamp";
		// @formatter:on
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        assertNotNull(metadata.getColumnLabel(i));
      }
      while (rs.next()) {
      }
    }
  }

  @Test
  public void testSelectWithTimestampAdd() throws SQLException {
    createNewsTables();
    String sql =
        "SELECT * FROM news_types WHERE TIMESTAMP_ADD(news_start, INTERVAL update_hours HOUR) > CURRENT_TIMESTAMP() and news_type_id=?";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    ps.setLong(1, 1L);
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      assertEquals("news_type_id", metadata.getColumnLabel(1));
      assertEquals("news_type_id", metadata.getColumnName(1));
      assertEquals(10, metadata.getColumnDisplaySize(1));
      assertEquals(Types.BIGINT, metadata.getColumnType(1));
      assertEquals("INT64", metadata.getColumnTypeName(1));
      assertEquals(10, metadata.getPrecision(1));
      assertEquals(0, metadata.getScale(1));
      // The following asserts do not work as the sql parser does not
      // recognize this sql statement
      // assertEquals(ResultSetMetaData.columnNoNulls,
      // metadata.isNullable(1));
      // assertEquals("", metadata.getCatalogName(1));
      // assertEquals("", metadata.getSchemaName(1));
      // assertEquals("news_types", metadata.getTableName(1));
      assertEquals(ResultSetMetaData.columnNullableUnknown, metadata.isNullable(1));
      assertNull(metadata.getCatalogName(1));
      assertNull(metadata.getSchemaName(1));
      assertEquals("", metadata.getTableName(1));
      while (rs.next()) {
      }
    }
  }

  @Test
  public void testSelectWithTimestampSub() throws SQLException {
    createNewsTables();
    String sql =
        "SELECT * FROM news_types WHERE TIMESTAMP_SUB(news_start, INTERVAL update_hours HOUR) > CURRENT_TIMESTAMP()";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      assertNotNull(rs.getMetaData().getColumnLabel(1));
      while (rs.next()) {
      }
    }
  }

  @Test
  public void testSelectWithTimestampToString() throws SQLException {
    String sql = "SELECT STRING(CURRENT_TIMESTAMP()) AS TS";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      assertEquals("TS", rs.getMetaData().getColumnLabel(1));
      while (rs.next()) {
        assertNotNull(rs.getString(1));
      }
    }
  }

  @Test
  public void testSelectCastStringToTimestamp() throws SQLException {
    // No timestamp information, so Cloud Spanner will treat this as being in Los Angeles (this is
    // UTC-08:00 as it is not during daylight saving time)
    String sql = "SELECT CAST('2008-12-25 15:30:00' AS TIMESTAMP) AS TS";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      assertEquals("TS", rs.getMetaData().getColumnLabel(1));
      while (rs.next()) {
        Calendar time = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        time.clear();
        time.set(2008, 11, 25, 23, 30, 0);
        assertEquals(new Timestamp(time.getTimeInMillis()), rs.getTimestamp("TS"));
      }
    }
  }

  @Test
  public void testSelectWithTimestampToStringWithTimezone() throws SQLException {
    // No timestamp information, so Cloud Spanner will treat this as being in Los Angeles (this is
    // UTC-08:00 as it is not during daylight saving time)
    String sql = "SELECT STRING(CAST('2008-12-25 15:30:00' AS TIMESTAMP), '-06:00') AS TS";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    try (ResultSet rs = ps.executeQuery()) {
      assertEquals("TS", rs.getMetaData().getColumnLabel(1));
      while (rs.next()) {
        assertTrue(Arrays.asList("2008-12-25 17:30:00-06", "2008-12-25 15:30:00-08")
            .contains(rs.getString(1)));
      }
    }
  }

  @Test
  public void testSelectWithSubSelectWithUnionAll() throws SQLException {
    createNewsTables();
    // @formatter:off
		String sql = "SELECT news.news_type_id, news.text FROM ( \n" + 
				"			SELECT n.news_type_id, n.text, nt.enabled, nt.priority, nt.news_start FROM news2 AS n INNER JOIN news_types nt ON nt.news_type_id = n.news_type_id WHERE n.seeker_id = ? \n" + 
				"			UNION ALL \n" + 
				"			SELECT nt.news_type_id, '' as text, nt.enabled, nt.priority, nt.news_start FROM news_types AS nt \n" + 
				"			) AS news \n" + 
				"			WHERE news.enabled = true \n" + 
				"			AND (news.news_start IS NULL OR news.news_start < CURRENT_TIMESTAMP()) \n" + 
//				"			AND (news.news_end IS NULL OR news.news_end > CURRENT_TIMESTAMP()) \n" + 
				"			AND news.news_type_id NOT IN (?) \n" + 
				"			ORDER BY news.priority DESC \n" + 
				"			LIMIT 1 ";
		// @formatter:on
    PreparedStatement ps = getConnection().prepareStatement(sql);
    ps.setLong(1, 1000L);
    ps.setLong(2, 500L);
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        assertNotNull(metadata.getColumnLabel(i));
      }
      while (rs.next()) {
      }
    }
  }

  @Test
  public void testSelectWithUnionAll() throws SQLException {
    String sql =
        "select 1,2,3 from (select 'test' as col1) a where a.col1=? union all select 4,5,6 from (select 'test' as col1) b where b.col1=?";
    PreparedStatement ps = getConnection().prepareStatement(sql);
    ps.setString(1, "test");
    ps.setString(2, "foo");
    int count = 0;
    try (ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        assertNotNull(metadata.getColumnLabel(i));
      }
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(1, count);
  }

}
