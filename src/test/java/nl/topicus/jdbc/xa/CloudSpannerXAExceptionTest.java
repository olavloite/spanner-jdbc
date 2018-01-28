package nl.topicus.jdbc.xa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerXAExceptionTest
{

	@Test
	public void testConstructors()
	{
		validateException(new CloudSpannerXAException("TEST", Code.FAILED_PRECONDITION, XAException.XAER_INVAL), null);
		validateException(new CloudSpannerXAException("TEST", new SQLException("TEST"), Code.FAILED_PRECONDITION,
				XAException.XAER_INVAL), SQLException.class);
		validateException(new CloudSpannerXAException("TEST",
				new CloudSpannerSQLException("TEST",
						SpannerExceptionFactory.newSpannerException(ErrorCode.FAILED_PRECONDITION, "TEST")),
				XAException.XAER_INVAL), CloudSpannerSQLException.class);
	}

	private <T extends Throwable> void validateException(CloudSpannerXAException e, Class<T> cause)
	{
		if (cause == null)
			assertNull(e.getCause());
		else
			assertEquals(cause, e.getCause().getClass());
		assertEquals("TEST", e.getMessage());
		assertEquals(XAException.XAER_INVAL, e.errorCode);
		assertEquals(Code.FAILED_PRECONDITION, e.getCode());
	}

}
