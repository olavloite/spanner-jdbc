package nl.topicus.jdbc.transaction;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class XATransactionTest
{

	@Test
	public void testMutationSerialization() throws IOException, ClassNotFoundException, NoSuchMethodException,
			SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		Mutation original = Mutation.newInsertBuilder("FOO").set("BAR").to("test").build();
		Mutation deserialized = serializeDeserialize(original);
		assertEquals(original, deserialized);

		original = Mutation.newUpdateBuilder("FOO").set("BAR").to("bla").build();
		deserialized = serializeDeserialize(original);
		assertEquals(original, deserialized);

		original = Mutation.delete("FOO", Key.of("bla"));
		deserialized = serializeDeserialize(original);
		assertEquals(original, deserialized);

		original = Mutation.delete("FOO", KeySet.all());
		deserialized = serializeDeserialize(original);
		assertEquals(original, deserialized);

		original = Mutation.delete("FOO", KeySet.range(KeyRange.closedClosed(Key.of("foo"), Key.of("bar"))));
		deserialized = serializeDeserialize(original);
		assertEquals(original, deserialized);
	}

	private Mutation serializeDeserialize(Mutation original) throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		Method serialize = XATransaction.class.getDeclaredMethod("serializeMutation", Mutation.class);
		serialize.setAccessible(true);
		String serialized = (String) serialize.invoke(null, original);

		Method deserialize = XATransaction.class.getDeclaredMethod("deserializeMutation", String.class);
		deserialize.setAccessible(true);
		Mutation deserialized = (Mutation) deserialize.invoke(null, serialized);

		return deserialized;
	}
}
