package nl.topicus.jdbc.transaction;

import java.util.List;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import nl.topicus.jdbc.xa.CloudSpannerXAConnection;

/**
 * Support class for distributed transactions
 * 
 * @author loite
 *
 */
class XATransaction
{

	private static final String SELECT_MUTATIONS = "SELECT " + CloudSpannerXAConnection.XA_NUMBER_COLUMN + ", "
			+ CloudSpannerXAConnection.XA_MUTATION_COLUMN + " FROM "
			+ CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE + " WHERE " + CloudSpannerXAConnection.XA_XID_COLUMN
			+ "=@xid ORDER BY " + CloudSpannerXAConnection.XA_NUMBER_COLUMN;

	static void prepareMutations(TransactionContext transaction, String xid, List<Mutation> mutations)
	{
		Gson gson = new GsonBuilder().create();
		int index = 0;
		for (Mutation mutation : mutations)
		{
			WriteBuilder prepared = Mutation.newInsertBuilder(CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE);
			prepared.set(CloudSpannerXAConnection.XA_XID_COLUMN).to(xid);
			prepared.set(CloudSpannerXAConnection.XA_NUMBER_COLUMN).to(index);
			prepared.set(CloudSpannerXAConnection.XA_MUTATION_COLUMN).to(gson.toJson(mutation));
			transaction.buffer(prepared.build());
			index++;
		}
	}

	static void commitPrepared(TransactionContext transaction, String xid)
	{
		Gson gson = new GsonBuilder().create();
		try (ResultSet rs = transaction.executeQuery(getPreparedMutationsStatement(xid)))
		{
			while (rs.next())
			{
				String json = rs.getString(1);
				Mutation mutation = gson.fromJson(json, Mutation.class);
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
		KeySet.Builder builder = KeySet.newBuilder();
		try (ResultSet rs = transaction.executeQuery(getPreparedMutationsStatement(xid)))
		{
			while (rs.next())
			{
				long number = rs.getLong(0);
				builder.addKey(Key.of(xid, number));
			}
		}
		Mutation delete = Mutation.delete(CloudSpannerXAConnection.XA_PREPARED_MUTATIONS_TABLE, builder.build());
		transaction.buffer(delete);
	}

	private static Statement getPreparedMutationsStatement(String xid)
	{
		return Statement.newBuilder(SELECT_MUTATIONS).bind("xid").to(xid).build();
	}

}
