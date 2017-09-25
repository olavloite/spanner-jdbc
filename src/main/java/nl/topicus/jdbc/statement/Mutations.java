package nl.topicus.jdbc.statement;

import java.util.Arrays;
import java.util.List;

import com.google.cloud.spanner.Mutation;

import nl.topicus.jdbc.extended.AbstractTablePartWorker;

class Mutations
{
	private final List<Mutation> mutations;

	private final AbstractTablePartWorker worker;

	/**
	 * Single mutation
	 * 
	 * @param mutation
	 */
	Mutations(Mutation mutation)
	{
		this.mutations = Arrays.asList(mutation);
		this.worker = null;
	}

	Mutations(List<Mutation> mutations)
	{
		this.mutations = mutations;
		this.worker = null;
	}

	Mutations(AbstractTablePartWorker worker)
	{
		this.mutations = null;
		this.worker = worker;
	}

	List<Mutation> getMutations()
	{
		if (isWorker())
			throw new IllegalStateException(
					"Cannot call getMutations() on a Mutations-object that returns its results as a worker");
		return mutations;
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
		return mutations.size();
	}

}
