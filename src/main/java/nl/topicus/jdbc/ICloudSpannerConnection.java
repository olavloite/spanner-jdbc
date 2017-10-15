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

	public Properties getSuppliedProperties();

	public boolean isAllowExtendedMode();

	public String getClientId();

	public Timestamp getLastCommitTimestamp();

}
