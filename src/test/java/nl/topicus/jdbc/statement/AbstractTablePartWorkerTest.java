package nl.topicus.jdbc.statement;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.statement.AbstractTablePartWorker.DMLOperation;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class AbstractTablePartWorkerTest {

  private AbstractTablePartWorker createWorker(String sql) throws JSQLParserException {
    return createWorker(sql, new ParameterStore());
  }

  private AbstractTablePartWorker createWorker(String sql, ParameterStore parameters)
      throws JSQLParserException {
    CloudSpannerConnection connection = mock(CloudSpannerConnection.class);
    Select select = (Select) CCJSqlParserUtil.parse(sql);
    DMLOperation operation = DMLOperation.INSERT;
    AbstractTablePartWorker worker = mock(AbstractTablePartWorker.class,
        withSettings().useConstructor(connection, select, parameters, true, operation)
            .defaultAnswer(CALLS_REAL_METHODS));
    return worker;
  }

  private String getTestCount(String sql, long batchSize) throws JSQLParserException {
    Select select = (Select) CCJSqlParserUtil.parse(sql);
    return createWorker(select.toString()).createCountQuery(select, batchSize);
  }

  @Test
  public void testGetEstimatedRecordCount() throws JSQLParserException {
    assertEquals("SELECT COUNT(*) AS C FROM ((SELECT * FROM FOO) LIMIT 1000) Q",
        getTestCount("SELECT * FROM FOO", 1000L));
    assertEquals("SELECT COUNT(*) AS C FROM ((SELECT * FROM FOO LIMIT 500) LIMIT 1000) Q",
        getTestCount("SELECT * FROM FOO LIMIT 500", 1000L));
    assertEquals("SELECT COUNT(*) AS C FROM ((SELECT BAR, `TEST`, bla FROM FOO) LIMIT 1000) Q",
        getTestCount("SELECT BAR, `TEST`, bla FROM FOO", 1000L));
    assertEquals(
        "SELECT COUNT(*) AS C FROM ((SELECT * FROM FOO UNION ALL SELECT * FROM bar) LIMIT 1000) Q",
        getTestCount("SELECT * FROM FOO union all select * from bar", 1000L));
    assertEquals(
        "SELECT COUNT(*) AS C FROM ((SELECT * FROM FOO UNION ALL SELECT * FROM bar LIMIT 500) LIMIT 1000) Q",
        getTestCount("SELECT * FROM FOO union all select * from bar limit 500", 1000L));
  }

  @Test
  public void testGetEstimatedRecordCountWithParameters() throws JSQLParserException {
    assertEquals("SELECT COUNT(*) AS C FROM ((SELECT * FROM FOO WHERE BAR > ?) LIMIT 1000) Q",
        getTestCount("SELECT * FROM FOO WHERE BAR > ?", 1000L));
  }

}
