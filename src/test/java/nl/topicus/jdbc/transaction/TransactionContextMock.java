package nl.topicus.jdbc.transaction;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionContext;

import nl.topicus.jdbc.xa.RecoveredXid;

class TransactionContextMock
{
	static final Xid XID_WITHOUT_MUTATIONS = RecoveredXid.stringToXid("1_Z3RyaWQ=_YnF1YWw=");

	static final Xid XID_WITH_MUTATIONS = RecoveredXid.stringToXid("2_Z3RyaWQ=_YnF1YWw=");

	private List<Mutation> buffer = new ArrayList<>();

	List<Mutation> getMutations()
	{
		return buffer;
	}

	void clearMutations()
	{
		buffer.clear();
	}

	@SuppressWarnings("unchecked")
	TransactionContext createTransactionContextMock()
	{
		TransactionContext res = mock(TransactionContext.class);
		when(res.executeQuery(
				XATransaction.getPreparedMutationsStatement(RecoveredXid.xidToString(XID_WITHOUT_MUTATIONS))))
						.then(new Answer<ResultSet>()
						{
							@Override
							public ResultSet answer(InvocationOnMock invocation) throws Throwable
							{
								return mockPreparedWithoutMutationsResultSet();
							}
						});
		when(res.executeQuery(
				XATransaction.getPreparedMutationsStatement(RecoveredXid.xidToString(XID_WITH_MUTATIONS))))
						.then(new Answer<ResultSet>()
						{
							@Override
							public ResultSet answer(InvocationOnMock invocation) throws Throwable
							{
								return mockPreparedWithMutationsResultSet();
							}
						});
		doAnswer(new Answer<Void>()
		{
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable
			{
				buffer.add((Mutation) invocation.getArgument(0));
				return null;
			}
		}).when(res).buffer(ArgumentMatchers.any(Mutation.class));
		doAnswer(new Answer<Void>()
		{
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable
			{
				Iterable<Mutation> it = (Iterable<Mutation>) invocation.getArgument(0);
				it.forEach(e -> buffer.add(e));
				return null;
			}
		}).when(res).buffer(ArgumentMatchers.any(Iterable.class));

		return res;
	}

	private ResultSet mockPreparedWithoutMutationsResultSet()
	{
		ResultSet rs = mock(ResultSet.class);
		when(rs.next()).thenReturn(false);
		return rs;
	}

	private ResultSet mockPreparedWithMutationsResultSet() throws SQLException
	{
		List<String> mutations = createSerializedMutations();
		ResultSet rs = mock(ResultSet.class);
		when(rs.next()).thenReturn(true, true, true, false);
		when(rs.getLong(0)).thenReturn(1l, 2l, 3l);
		when(rs.getString(1)).thenReturn(mutations.get(0), mutations.get(1), mutations.get(2));
		return rs;
	}

	private List<String> createSerializedMutations() throws SQLException
	{
		List<String> res = new ArrayList<>(3);
		res.add(XATransaction.serializeMutation(Mutation.newInsertBuilder("FOO").set("ID").to(1L).build()));
		res.add(XATransaction.serializeMutation(Mutation.newInsertBuilder("FOO").set("ID").to(2L).build()));
		res.add(XATransaction.serializeMutation(Mutation.newInsertBuilder("FOO").set("ID").to(3L).build()));

		return res;
	}

}
