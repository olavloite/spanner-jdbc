package nl.topicus.jdbc.statement;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class AbstractCloudSpannerStatementTest
{
	private AbstractCloudSpannerStatement subject;

	public AbstractCloudSpannerStatementTest() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		DatabaseClient dbClient = null;
		subject = Mockito.mock(AbstractCloudSpannerStatement.class,
				Mockito.withSettings().useConstructor(connection, dbClient).defaultAnswer(Mockito.CALLS_REAL_METHODS));
	}

	@Test
	public void testSanitizeSQL()
	{
		Assert.assertEquals("SELECT * FROM FOO WHERE ID=?",
				subject.sanitizeSQL("SELECT * FROM FOO@{FORCE_INDEX=BAR_INDEX} WHERE ID=?"));
		Assert.assertEquals("SELECT *\nFROM FOO\nWHERE ID=?",
				subject.sanitizeSQL("SELECT *\nFROM FOO@{FORCE_INDEX=BAR_INDEX}\nWHERE ID=?"));
		Assert.assertEquals("SELECT *\n\tFROM FOO\n\tWHERE ID=?",
				subject.sanitizeSQL("SELECT *\n\tFROM FOO@{FORCE_INDEX=BAR_INDEX}\n\tWHERE ID=?"));
		Assert.assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{FORCE_INDEX = BAR_INDEX }\n\t   WHERE ID = ?"));
		Assert.assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ FORCE_INDEX = BAR_INDEX }\n\t   WHERE ID = ?"));
		Assert.assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ force_index = BAR_INDEX }\n\t   WHERE ID = ?"));
		Assert.assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ force_index =\n BAR_INDEX }\n\t   WHERE ID = ?"));

		Assert.assertEquals("INSERT INTO TAB (ID, COL1) VALUES (?, ?) ON DUPLICATE KEY UPDATE FOO=BAR",
				subject.sanitizeSQL("INSERT INTO TAB (ID, COL1) VALUES (?, ?) ON DUPLICATE KEY UPDATE"));
		Assert.assertEquals("INSERT INTO TAB (ID, COL1)\nVALUES (?, ?)\nON DUPLICATE KEY UPDATE FOO=BAR",
				subject.sanitizeSQL("INSERT INTO TAB (ID, COL1)\nVALUES (?, ?)\nON DUPLICATE KEY UPDATE"));
		Assert.assertEquals("\tINSERT INTO\n\tTAB (ID, COL1)\n\tVALUES (?, ?)\nON DUPLICATE KEY\nUPDATE\n\t   FOO=BAR",
				subject.sanitizeSQL(
						"\tINSERT INTO\n\tTAB (ID, COL1)\n\tVALUES (?, ?)\nON DUPLICATE KEY\nUPDATE\n\t  "));
	}

}
