package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.google.cloud.Timestamp;

/**
 * Interface containing all extra methods that are provided by
 * CloudSpannerConnection
 * 
 * @author loite
 *
 */
public interface ICloudSpannerConnection extends Connection
{
	public String getUrl();

	public String getProductName();

	public void setSimulateProductName(String productName);

	public void setSimulateMajorVersion(Integer majorVersion);

	public void setSimulateMinorVersion(Integer minorVersion);

	public Properties getSuppliedProperties();

	public boolean isAllowExtendedMode();

	public int setAllowExtendedMode(boolean allowExtendedMode);

	public boolean isAsyncDdlOperations();

	public int setAsyncDdlOperations(boolean asyncDdlOperations);

	public boolean isAutoBatchDdlOperations();

	public int setAutoBatchDdlOperations(boolean autoBatchDdlOperations);

	public boolean isReportDefaultSchemaAsNull();

	public int setReportDefaultSchemaAsNull(boolean reportDefaultSchemaAsNull);

	public String getClientId();

	public Timestamp getLastCommitTimestamp();

	public Timestamp getReadTimestamp();

	public boolean isBatchReadOnly();

	public int setBatchReadOnly(boolean batchReadOnly) throws SQLException;

}
