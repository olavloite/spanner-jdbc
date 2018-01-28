package nl.topicus.jdbc.xa;

import javax.transaction.xa.XAException;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public class CloudSpannerXAException extends XAException
{
	private static final long serialVersionUID = 1L;

	static final String COMMIT_METHODS_NOT_ALLOWED_WHEN_ACTIVE = "Transaction control methods setAutoCommit(true), commit, rollback and setSavePoint not allowed while an XA transaction is active.";
	static final String INVALID_FLAGS = "Invalid flags";
	static final String XID_NOT_NULL = "xid must not be null";
	static final String CONNECTION_BUSY = "Connection is busy with another transaction";
	static final String SUSPEND_NOT_IMPLEMENTED = "suspend/resume not implemented";
	static final String INTERLEAVING_NOT_IMPLEMENTED = "Transaction interleaving not implemented";
	static final String ERROR_DISABLING_AUTOCOMMIT = "Error disabling autocommit";
	static final String END_WITHOUT_START = "tried to call end without corresponding start call";
	static final String PREPARE_WITH_SAME = "Not implemented: Prepare must be issued using the same connection that started the transaction";
	static final String PREPARE_BEFORE_END = "Prepare called before end";
	static final String ERROR_PREPARING = "Error preparing transaction";
	static final String ERROR_RECOVER = "Error during recover";
	static final String ERROR_ROLLBACK_PREPARED = "Error rolling back prepared transaction";
	static final String ONE_PHASE_SAME = "Not implemented: one-phase commit must be issued using the same connection that was used to start it";
	static final String COMMIT_BEFORE_END = "commit called before end";
	static final String ERROR_ONE_PHASE = "Error during one-phase commit";
	static final String TWO_PHASE_IDLE = "Not implemented: 2nd phase commit must be issued using an idle connection";
	static final String ERROR_COMMIT_PREPARED = "Error committing prepared transaction";
	static final String HEURISTIC_NOT_IMPLEMENTED = "Heuristic commit/rollback not supported";

	private final Code code;

	public CloudSpannerXAException(String message, Code code, int errorCode)
	{
		super(message);
		this.errorCode = errorCode;
		this.code = code;
	}

	public CloudSpannerXAException(String message, Throwable cause, Code code, int errorCode)
	{
		super(message);
		initCause(cause);
		this.errorCode = errorCode;
		this.code = code;
	}

	public CloudSpannerXAException(String message, CloudSpannerSQLException cause, int errorCode)
	{
		super(message);
		initCause(cause);
		this.errorCode = errorCode;
		this.code = cause.getCode();
	}

	public Code getCode()
	{
		return code;
	}

}
