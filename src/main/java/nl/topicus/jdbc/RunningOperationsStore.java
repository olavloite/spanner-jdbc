package nl.topicus.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.rpc.Code;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.statement.CloudSpannerStatement;

/**
 * This class maintains a list of all long running (DDL-)operations of a
 * connection.
 * 
 * @author loite
 *
 */
class RunningOperationsStore
{
	private static final class DdlOperation
	{
		private final Timestamp timeStarted;

		private final List<String> sql;

		private Operation<Void, UpdateDatabaseDdlMetadata> operation;

		private DdlOperation(Timestamp timeStarted, List<String> sql,
				Operation<Void, UpdateDatabaseDdlMetadata> operation)
		{
			this.timeStarted = timeStarted;
			this.sql = new ArrayList<>(sql);
			this.operation = operation;
		}

	}

	private List<DdlOperation> operations = new ArrayList<>();

	RunningOperationsStore()
	{
	}

	void addOperation(List<String> sql, Operation<Void, UpdateDatabaseDdlMetadata> operation)
	{
		operations.add(new DdlOperation(Timestamp.now(), sql, operation));
	}

	int clearFinishedOperations()
	{
		int count = 0;
		int index = 0;
		while (index < operations.size())
		{
			DdlOperation op = operations.get(index);
			op.operation = op.operation.reload();
			if (op.operation.isDone())
			{
				operations.remove(index);
				count++;
			}
			else
			{
				index++;
			}
		}
		return count;
	}

	void waitForOperations() throws SQLException
	{
		boolean finished = false;
		while (!finished)
		{
			finished = true;
			for (DdlOperation op : operations)
			{
				op.operation = op.operation.reload();
				if (!op.operation.isDone())
				{
					finished = false;
					break;
				}
			}
			try
			{
				Thread.sleep(1000l);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new CloudSpannerSQLException("Wait for DDL-operations interrupted", Code.CANCELLED, e);
			}
		}
	}

	/**
	 * 
	 * @return A result set of all DDL operations that have been issued on this
	 *         connection since the last clear operation.
	 */
	ResultSet getOperations(CloudSpannerStatement statement)
	{
		List<Struct> rows = new ArrayList<>(operations.size());
		for (DdlOperation op : operations)
		{
			op.operation = op.operation.reload();
			String exception = null;
			try
			{
				op.operation.getResult();
			}
			catch (Exception e)
			{
				exception = e.getMessage();
			}
			for (String ddl : op.sql)
			{
				rows.add(Struct.newBuilder().add("NAME", Value.string(op.operation.getName()))
						.add("TIME_STARTED", Value.timestamp(op.timeStarted)).add("STATEMENT", Value.string(ddl))
						.add("DONE", Value.bool(op.operation.isDone())).add("EXCEPTION", Value.string(exception))
						.build());
			}
		}
		com.google.cloud.spanner.ResultSet rs = ResultSets.forRows(Type.struct(StructField.of("NAME", Type.string()),
				StructField.of("TIME_STARTED", Type.timestamp()), StructField.of("STATEMENT", Type.string()),
				StructField.of("DONE", Type.bool()), StructField.of("EXCEPTION", Type.string())), rows);
		return new CloudSpannerResultSet(statement, rs, null);
	}

}
