package nl.topicus.jdbc.transaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.xa.CloudSpannerXAConnection;

/**
 * Support class for distributed transactions
 * 
 * @author loite
 *
 */
class XATransaction
{

	/**
	 * Avoid instantiation
	 */
	private XATransaction()
	{
	}

	private static final String SELECT_MUTATIONS = "SELECT " + CloudSpannerXAConnection.XA_NUMBER_COLUMN + ", "
			+ CloudSpannerXAConnection.XA_MUTATION_COLUMN + " FROM "
			+ CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE + " WHERE " + CloudSpannerXAConnection.XA_XID_COLUMN
			+ "=@xid ORDER BY " + CloudSpannerXAConnection.XA_NUMBER_COLUMN;

	static void prepareMutations(TransactionContext transaction, String xid, List<Mutation> mutations)
			throws SQLException
	{
		int index = 0;
		for (Mutation mutation : mutations)
		{
			WriteBuilder prepared = Mutation.newInsertBuilder(CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE);
			prepared.set(CloudSpannerXAConnection.XA_XID_COLUMN).to(xid);
			prepared.set(CloudSpannerXAConnection.XA_NUMBER_COLUMN).to(index);
			prepared.set(CloudSpannerXAConnection.XA_MUTATION_COLUMN).to(serializeMutation(mutation));
			transaction.buffer(prepared.build());
			index++;
		}
	}

	static void commitPrepared(TransactionContext transaction, String xid) throws SQLException
	{
		try (ResultSet rs = transaction.executeQuery(getPreparedMutationsStatement(xid)))
		{
			while (rs.next())
			{
				String serialized = rs.getString(1);
				Mutation mutation = deserializeMutation(serialized);
				transaction.buffer(mutation);
			}
		}
		cleanupPrepared(transaction, xid);
	}

	static void rollbackPrepared(TransactionContext transaction, String xid)
	{
		cleanupPrepared(transaction, xid);
	}

	private static void cleanupPrepared(TransactionContext transaction, String xid)
	{
		boolean foundRecords = false;
		KeySet.Builder builder = KeySet.newBuilder();
		try (ResultSet rs = transaction.executeQuery(getPreparedMutationsStatement(xid)))
		{
			while (rs.next())
			{
				foundRecords = true;
				long number = rs.getLong(0);
				builder.addKey(Key.of(xid, number));
			}
		}
		if (foundRecords)
		{
			Mutation delete = Mutation.delete(CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE, builder.build());
			transaction.buffer(delete);
		}
	}

	static Statement getPreparedMutationsStatement(String xid)
	{
		return Statement.newBuilder(SELECT_MUTATIONS).bind("xid").to(xid).build();
	}

	@VisibleForTesting
	static String serializeMutation(Mutation mutation) throws SQLException
	{
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream stream = new ObjectOutputStream(bos))
		{
			stream.writeObject(mutation);
			return Base64.getEncoder().encodeToString(bos.toByteArray());
		}
		catch (IOException e)
		{
			throw new CloudSpannerSQLException("Could not serialize mutation", Code.INTERNAL, e);
		}
	}

	@VisibleForTesting
	static Mutation deserializeMutation(String mutation) throws SQLException
	{
		try (ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(mutation));
				ObjectInputStream input = new ObjectInputStream(bis))
		{
			return (Mutation) input.readObject();
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new CloudSpannerSQLException("Could not deserialize mutation", Code.INTERNAL, e);
		}
	}

}
