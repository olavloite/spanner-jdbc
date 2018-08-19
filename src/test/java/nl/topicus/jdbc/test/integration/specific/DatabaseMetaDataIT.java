package nl.topicus.jdbc.test.integration.specific;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class DatabaseMetaDataIT extends AbstractSpecificIntegrationTest {

  @Test
  public void testGetClientInfoProperties() throws SQLException {
    try (ResultSet rs = getConnection().getMetaData().getClientInfoProperties()) {
      while(rs.next()) {
      }
    }
  }

  @Test
  public void testGetTypeInfo() throws SQLException {
    try (ResultSet rs = getConnection().getMetaData().getTypeInfo()) {
      while(rs.next()) {
      }
    }
  }

  @Test
  public void testGetPseudoColumns() throws SQLException {
    try (ResultSet rs = getConnection().getMetaData().getPseudoColumns("", "", null, null)) {
      while(rs.next()) {
      }
    }
  }

}
