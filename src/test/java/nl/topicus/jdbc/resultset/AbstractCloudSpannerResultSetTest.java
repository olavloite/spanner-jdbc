package nl.topicus.jdbc.resultset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.google.common.base.Defaults;

import nl.topicus.jdbc.statement.CloudSpannerStatement;
import nl.topicus.jdbc.test.category.UnitTest;

@RunWith(Enclosed.class)
@Category(UnitTest.class)
public class AbstractCloudSpannerResultSetTest
{

	public static class SupportedMethodsTest
	{
		private CloudSpannerResultSet subject = new CloudSpannerResultSet(Mockito.mock(CloudSpannerStatement.class),
				null);

		@Test
		public void testGetType() throws SQLException
		{
			assertEquals(ResultSet.TYPE_FORWARD_ONLY, subject.getType());
		}

		@Test
		public void testGetConcurrency() throws SQLException
		{
			assertEquals(ResultSet.CONCUR_READ_ONLY, subject.getConcurrency());
		}

		@Test
		public void testGetCursorName() throws SQLException
		{
			assertEquals("", subject.getCursorName());
		}

		@Test
		public void testRowUpdated() throws SQLException
		{
			assertFalse(subject.rowUpdated());
		}

		@Test
		public void testRowInserted() throws SQLException
		{
			assertFalse(subject.rowInserted());
		}

		@Test
		public void testRowDeleted() throws SQLException
		{
			assertFalse(subject.rowDeleted());
		}
	}

	public static class UnsupportedMethodsTest
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		private CloudSpannerResultSet subject = new CloudSpannerResultSet(Mockito.mock(CloudSpannerStatement.class),
				null);

		private static final Set<Method> SUPPORTED_METHODS = new HashSet<>();
		static
		{
			try
			{
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("getType"));
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("getConcurrency"));
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("getCursorName"));
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("rowUpdated"));
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("rowInserted"));
				SUPPORTED_METHODS.add(AbstractCloudSpannerResultSet.class.getMethod("rowDeleted"));
			}
			catch (NoSuchMethodException e)
			{
				throw new IllegalArgumentException("Unknown method specified: " + e.getMessage(), e);
			}
		}

		@Test
		public void testUnsupportedMethods()
		{
			Method[] methods = AbstractCloudSpannerResultSet.class.getDeclaredMethods();
			for (Method method : methods)
			{
				if (!SUPPORTED_METHODS.contains(method) && Modifier.isPublic(method.getModifiers()))
				{
					try
					{
						Class<?>[] parameterTypes = method.getParameterTypes();
						Object[] params = new Object[parameterTypes.length];
						for (int i = 0; i < params.length; i++)
						{
							params[i] = createDefaultParamValue(parameterTypes[i]);
						}
						method.invoke(subject, params);
						fail("Expected SQLFeatureNotSupportedException");
					}
					catch (IllegalAccessException | IllegalArgumentException e)
					{
						throw new IllegalStateException(
								"Error while executing tests on unsupported methods: " + e.getMessage(), e);
					}
					catch (InvocationTargetException e)
					{
						assertTrue(e.getCause() != null
								&& e.getCause().getClass().equals(SQLFeatureNotSupportedException.class));
					}
				}
			}
		}

		private Object createDefaultParamValue(Class<?> clazz)
		{
			if (clazz.isPrimitive())
				return Defaults.defaultValue(clazz);
			if (byte[].class.equals(clazz))
				return "FOO".getBytes();
			return null;
		}

	}

}
