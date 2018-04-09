package nl.topicus.jdbc.emulator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionRunner;

public class DatabaseClientEmulator implements com.google.cloud.spanner.DatabaseClient
{

	@Override
	public Timestamp write(Iterable<Mutation> mutations) throws SpannerException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp writeAtLeastOnce(Iterable<Mutation> mutations) throws SpannerException
	{
		return write(mutations);
	}

	@Override
	public ReadContext singleUse()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadContext singleUse(TimestampBound bound)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadOnlyTransaction singleUseReadOnlyTransaction()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadOnlyTransaction singleUseReadOnlyTransaction(TimestampBound bound)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadOnlyTransaction readOnlyTransaction()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadOnlyTransaction readOnlyTransaction(TimestampBound bound)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TransactionRunner readWriteTransaction()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
