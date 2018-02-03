package nl.topicus.jdbc.statement;

import java.util.Arrays;
import java.util.List;

import com.google.cloud.spanner.Mutation;

class Mutations
{
	private final List<Mutation> buffer;

	private final AbstractTablePartWorker worker;

	/**
	 * Single mutation
	 * 
	 * @param mutation
	 */
	Mutations(Mutation mutation)
	{
		this.buffer = Arrays.asList(mutation);
		this.worker = null;
	}

	Mutations(List<Mutation> mutations)
	{
		this.buffer = mutations;
		this.worker = null;
	}

	Mutations(AbstractTablePartWorker worker)
	{
		this.buffer = null;
		this.worker = worker;
	}

	List<Mutation> getMutations()
	{
		if (isWorker())
			throw new IllegalStateException(
					"Cannot call getMutations() on a Mutations-object that returns its results as a worker");
		return buffer;
	}

	AbstractTablePartWorker getWorker()
	{
		return worker;
	}

	boolean isWorker()
	{
		return worker != null;
	}

	long getNumberOfResults()
	{
		if (isWorker())
			return worker.getRecordCount();
		return buffer.size();
	}

}
