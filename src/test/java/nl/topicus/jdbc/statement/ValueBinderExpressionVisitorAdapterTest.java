package nl.topicus.jdbc.statement;

import java.math.BigDecimal;
import java.sql.Types;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ValueBinder;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class ValueBinderExpressionVisitorAdapterTest
{

	private ValueBinderExpressionVisitorAdapter<WriteBuilder> create()
	{
		ParameterStore parameterStore = new ParameterStore();
		WriteBuilder builder = Mutation.newInsertBuilder("INSERT INTO FOO (ID, COL1, COL2) VALUES (?, ?, ?)");
		ValueBinder<WriteBuilder> binder = builder.set("COL1");
		ValueBinderExpressionVisitorAdapter<WriteBuilder> res = new ValueBinderExpressionVisitorAdapter<>(
				parameterStore, binder, "COL1");
		return res;
	}

	@Test
	public void testSetValueWithoutException()
	{
		create().setValue(null, Types.BOOLEAN);
		create().setValue((byte) 1, Types.TINYINT);
		create().setValue((short) 1, Types.SMALLINT);
		create().setValue(1, Types.INTEGER);
		create().setValue(1l, Types.BIGINT);
		create().setValue(1f, Types.FLOAT);
		create().setValue(1d, Types.DOUBLE);
		create().setValue('a', Types.CHAR);
		create().setValue(true, Types.BOOLEAN);
		create().setValue("TEST", Types.NVARCHAR);
		create().setValue(BigDecimal.ONE, Types.DECIMAL);
		create().setValue(new byte[] { (byte) 1 }, Types.BINARY);

		create().setValue(new short[] { (short) 1 }, Types.ARRAY);
		create().setValue(new Short[] { (short) 1 }, Types.ARRAY);
		create().setValue(new int[] { 1 }, Types.ARRAY);
		create().setValue(new Integer[] { 1 }, Types.ARRAY);
		create().setValue(new long[] { 1l }, Types.ARRAY);
		create().setValue(new Long[] { 1l }, Types.ARRAY);
		create().setValue(new float[] { 1f }, Types.ARRAY);
		create().setValue(new Float[] { 1f }, Types.ARRAY);
		create().setValue(new double[] { 1d }, Types.ARRAY);
		create().setValue(new Double[] { 1d }, Types.ARRAY);
		create().setValue(new char[] { 'a' }, Types.ARRAY);
		create().setValue(new Character[] { 'a' }, Types.ARRAY);
		create().setValue(new boolean[] { true }, Types.ARRAY);
		create().setValue(new Boolean[] { true }, Types.ARRAY);
		create().setValue(new String[] { "TEST" }, Types.ARRAY);
		create().setValue(new BigDecimal[] { BigDecimal.ONE }, Types.ARRAY);
		create().setValue(new byte[][] { { (byte) 1 } }, Types.ARRAY);

	}

}
