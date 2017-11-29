package nl.topicus.jdbc.xa;

import javax.transaction.xa.XAException;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public class CloudSpannerXAException extends XAException
{
	private static final long serialVersionUID = 1L;

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
