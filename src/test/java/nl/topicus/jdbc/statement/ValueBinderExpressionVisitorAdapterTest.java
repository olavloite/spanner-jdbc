package nl.topicus.jdbc.statement;

import java.math.BigDecimal;

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
		create().setValue(null);
		create().setValue((byte) 1);
		create().setValue((short) 1);
		create().setValue(1);
		create().setValue(1l);
		create().setValue(1f);
		create().setValue(1d);
		create().setValue('a');
		create().setValue(true);
		create().setValue("TEST");
		create().setValue(BigDecimal.ONE);
		create().setValue(new byte[] { (byte) 1 });

		create().setValue(new short[] { (short) 1 });
		create().setValue(new Short[] { (short) 1 });
		create().setValue(new int[] { 1 });
		create().setValue(new Integer[] { 1 });
		create().setValue(new long[] { 1l });
		create().setValue(new Long[] { 1l });
		create().setValue(new float[] { 1f });
		create().setValue(new Float[] { 1f });
		create().setValue(new double[] { 1d });
		create().setValue(new Double[] { 1d });
		create().setValue(new char[] { 'a' });
		create().setValue(new Character[] { 'a' });
		create().setValue(new boolean[] { true });
		create().setValue(new Boolean[] { true });
		create().setValue(new String[] { "TEST" });
		create().setValue(new BigDecimal[] { BigDecimal.ONE });
		create().setValue(new byte[][] { { (byte) 1 } });

	}

}
