package nl.topicus.jdbc.statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class AbstractCloudSpannerStatementTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

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
		assertEquals("SELECT * FROM FOO WHERE ID=?",
				subject.sanitizeSQL("SELECT * FROM FOO@{FORCE_INDEX=BAR_INDEX} WHERE ID=?"));
		assertEquals("SELECT *\nFROM FOO\nWHERE ID=?",
				subject.sanitizeSQL("SELECT *\nFROM FOO@{FORCE_INDEX=BAR_INDEX}\nWHERE ID=?"));
		assertEquals("SELECT *\n\tFROM FOO\n\tWHERE ID=?",
				subject.sanitizeSQL("SELECT *\n\tFROM FOO@{FORCE_INDEX=BAR_INDEX}\n\tWHERE ID=?"));
		assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{FORCE_INDEX = BAR_INDEX }\n\t   WHERE ID = ?"));
		assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ FORCE_INDEX = BAR_INDEX }\n\t   WHERE ID = ?"));
		assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ force_index = BAR_INDEX }\n\t   WHERE ID = ?"));
		assertEquals("SELECT *\n\t   FROM  FOO\n\t   WHERE ID = ?",
				subject.sanitizeSQL("SELECT *\n\t   FROM  FOO@{ force_index =\n BAR_INDEX }\n\t   WHERE ID = ?"));

		assertEquals("INSERT INTO TAB (ID, COL1) VALUES (?, ?) ON DUPLICATE KEY UPDATE FOO=BAR",
				subject.sanitizeSQL("INSERT INTO TAB (ID, COL1) VALUES (?, ?) ON DUPLICATE KEY UPDATE"));
		assertEquals("INSERT INTO TAB (ID, COL1)\nVALUES (?, ?)\nON DUPLICATE KEY UPDATE FOO=BAR",
				subject.sanitizeSQL("INSERT INTO TAB (ID, COL1)\nVALUES (?, ?)\nON DUPLICATE KEY UPDATE"));
		assertEquals("\tINSERT INTO\n\tTAB (ID, COL1)\n\tVALUES (?, ?)\nON DUPLICATE KEY\nUPDATE\n\t   FOO=BAR", subject
				.sanitizeSQL("\tINSERT INTO\n\tTAB (ID, COL1)\n\tVALUES (?, ?)\nON DUPLICATE KEY\nUPDATE\n\t  "));
	}

	@Test
	public void testSimpleGettersAndSetters() throws SQLException
	{
		subject.setMaxFieldSize(10);
		assertEquals(10, subject.getMaxFieldSize());
		subject.setMaxRows(10);
		assertEquals(10, subject.getMaxRows());
		subject.setEscapeProcessing(true);
		subject.setEscapeProcessing(false);
		subject.setQueryTimeout(10);
		assertEquals(10, subject.getQueryTimeout());
		assertNull(subject.getWarnings());
		subject.clearWarnings();
		assertEquals(ResultSet.CONCUR_READ_ONLY, subject.getResultSetConcurrency());
		assertEquals(ResultSet.TYPE_FORWARD_ONLY, subject.getResultSetType());
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, subject.getResultSetHoldability());
		assertFalse(subject.isPoolable());
		subject.setPoolable(true);
		assertTrue(subject.isPoolable());
		assertFalse(subject.isCloseOnCompletion());
		subject.closeOnCompletion();
		assertTrue(subject.isCloseOnCompletion());
	}

	@Test
	public void testCancel() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		subject.cancel();
	}

	@Test
	public void testSetCursorName() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		subject.setCursorName("TEST");
	}

	@Test
	public void testGetGeneratedKeys() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		subject.getGeneratedKeys();
	}

}
