package nl.topicus.jdbc.sql;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class SqlParserTest {

  /**
   * This test should not throw an exception, but it does as the SQL parser is not able to parse a
   * boolean expression. The workaround is to wrap the boolean expression in a case when ... then
   * expression.
   * 
   * @throws JSQLParserException
   */
  @Test(expected = JSQLParserException.class)
  public void testIfStatement() throws JSQLParserException {
    String sql = "SELECT IF(select 2>1, 'A', 'B') FROM notifications";
    Statement statement = CCJSqlParserUtil.parse(sql);
    assertNotNull(statement);
  }

  @Test()
  public void testIfStatementWithCaseStatement() throws JSQLParserException {
    String sql = "SELECT IF(case when 2>1 then true else false end, 'A', 'B') FROM notifications";
    Statement statement = CCJSqlParserUtil.parse(sql);
    assertNotNull(statement);
  }

}
