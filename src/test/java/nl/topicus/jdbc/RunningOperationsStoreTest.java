package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.jdbc.statement.CloudSpannerStatement;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class RunningOperationsStoreTest
{
	private boolean reportDone = false;

	private RunningOperationsStore createSubject()
	{
		RunningOperationsStore res = new RunningOperationsStore();
		return res;
	}

	private Operation<Void, UpdateDatabaseDdlMetadata> mockOperation(boolean error)
	{
		@SuppressWarnings("unchecked")
		Operation<Void, UpdateDatabaseDdlMetadata> op = mock(Operation.class);
		when(op.getName()).then(new Returns("TEST_OPERATION"));
		when(op.isDone()).then(new Answer<Boolean>()
		{
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable
			{
				return reportDone;
			}
		});
		when(op.reload()).then(new Returns(op));
		if (error)
			when(op.getResult()).thenThrow(
					SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Some exception"));
		else
			when(op.getResult()).then(new Returns(null));
		UpdateDatabaseDdlMetadata metadata = UpdateDatabaseDdlMetadata.getDefaultInstance();
		when(op.getMetadata()).then(new Returns(metadata));

		return op;
	}

	@Test
	public void testAddGetAndClean() throws SQLException
	{
		RunningOperationsStore subject = createSubject();
		String sql = "CREATE TABLE FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)";
		for (int counter = 1; counter <= 2; counter++)
		{
			boolean exception = counter % 2 == 0;
			subject.addOperation(Arrays.asList(sql), mockOperation(exception));
			try (ResultSet rs = subject.getOperations(mock(CloudSpannerStatement.class)))
			{
				assertNotNull(rs);
				int count = 0;
				while (rs.next())
				{
					count++;
					assertEquals("TEST_OPERATION", rs.getString("NAME"));
					assertNotNull(rs.getTimestamp("TIME_STARTED"));
					assertEquals(sql, rs.getString("STATEMENT"));
					assertFalse(rs.getBoolean("DONE"));
					if (count % 2 == 0)
					{
						assertEquals("INVALID_ARGUMENT: Some exception", rs.getString("EXCEPTION"));
					}
					else
					{
						assertNull(rs.getString("EXCEPTION"));
					}
				}
				assertEquals(counter, count);
			}
			subject.clearFinishedOperations();
		}
		reportDone = true;
		subject.clearFinishedOperations();
		try (ResultSet rs = subject.getOperations(mock(CloudSpannerStatement.class)))
		{
			assertFalse(rs.next());
		}
	}

}
