package nl.topicus.jdbc.exception;

import java.sql.SQLException;

import com.google.cloud.spanner.SpannerException;
import com.google.rpc.Code;

/**
 * Specific {@link SQLException} for Google Cloud Spanner
 * 
 * @author loite
 *
 */
public class CloudSpannerSQLException extends SQLException
{
	private static final long serialVersionUID = 1L;

	private final Code code;

	public CloudSpannerSQLException(String message, Code code)
	{
		super(message, null, code.getNumber(), null);
		this.code = code;
	}

	public CloudSpannerSQLException(String message, Code code, Throwable cause)
	{
		super(message, null, code.getNumber(), cause);
		this.code = code;
	}

	public CloudSpannerSQLException(SpannerException e)
	{
		super(e.getMessage(), null, e.getCode(), e);
		this.code = Code.forNumber(e.getCode());
	}

	public CloudSpannerSQLException(String message, SpannerException e)
	{
		super(message, null, e.getCode(), e);
		this.code = Code.forNumber(e.getCode());
	}

	public Code getCode()
	{
		return code;
	}

}
