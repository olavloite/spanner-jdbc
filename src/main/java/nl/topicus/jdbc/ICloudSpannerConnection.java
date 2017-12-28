package nl.topicus.jdbc;

import java.sql.Connection;
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

	public void setAllowExtendedMode(boolean allowExtendedMode);

	public boolean isAsyncDdlOperations();

	public void setAsyncDdlOperations(boolean asyncDdlOperations);

	public String getClientId();

	public Timestamp getLastCommitTimestamp();

}
