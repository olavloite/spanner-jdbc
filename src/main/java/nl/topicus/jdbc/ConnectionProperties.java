package nl.topicus.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

final class ConnectionProperties
{
	public static final int NUMBER_OF_PROPERTIES = 13;

	static String getPropertyName(String propertyPart)
	{
		return propertyPart.substring(0, propertyPart.length() - 1);
	}

	static final String PROJECT_URL_PART = "Project=";

	static final String INSTANCE_URL_PART = "Instance=";

	static final String DATABASE_URL_PART = "Database=";

	static final String KEY_FILE_URL_PART = "PvtKeyPath=";

	static final String OAUTH_ACCESS_TOKEN_URL_PART = "OAuthAccessToken=";

	static final String SIMULATE_PRODUCT_NAME = "SimulateProductName=";
	static final String SIMULATE_PRODUCT_MAJOR_VERSION = "SimulateProductMajorVersion=";
	static final String SIMULATE_PRODUCT_MINOR_VERSION = "SimulateProductMinorVersion=";

	static final String ALLOW_EXTENDED_MODE = "AllowExtendedMode=";
	static final String ASYNC_DDL_OPERATIONS = "AsyncDdlOperations=";
	static final String AUTO_BATCH_DDL_OPERATIONS = "AutoBatchDdlOperations=";
	static final String REPORT_DEFAULT_SCHEMA_AS_NULL = "ReportDefaultSchemaAsNull=";
	static final String BATCH_READ_ONLY_MODE = "BatchReadOnlyMode=";

	String project = null;
	String instance = null;
	String database = null;
	String keyFile = null;
	String oauthToken = null;
	String productName = null;
	Integer majorVersion = null;
	Integer minorVersion = null;
	boolean allowExtendedMode = false;
	boolean asyncDdlOperations = false;
	boolean autoBatchDdlOperations = false;
	boolean reportDefaultSchemaAsNull = true;
	boolean batchReadOnlyMode = false;

	static ConnectionProperties parse(String url) throws SQLException
	{
		ConnectionProperties res = new ConnectionProperties();
		if (url != null)
		{
			String[] parts = url.split(":", 3);
			String[] connectionParts = parts[2].split(";");
			// Get connection properties from connection string
			for (int i = 1; i < connectionParts.length; i++)
			{
				String conPart = connectionParts[i].replace(" ", "");
				String conPartLower = conPart.toLowerCase();
				if (conPartLower.startsWith(PROJECT_URL_PART.toLowerCase()))
					res.project = conPart.substring(PROJECT_URL_PART.length());
				else if (conPartLower.startsWith(INSTANCE_URL_PART.toLowerCase()))
					res.instance = conPart.substring(INSTANCE_URL_PART.length());
				else if (conPartLower.startsWith(DATABASE_URL_PART.toLowerCase()))
					res.database = conPart.substring(DATABASE_URL_PART.length());
				else if (conPartLower.startsWith(KEY_FILE_URL_PART.toLowerCase()))
					res.keyFile = conPart.substring(KEY_FILE_URL_PART.length());
				else if (conPartLower.startsWith(OAUTH_ACCESS_TOKEN_URL_PART.toLowerCase()))
					res.oauthToken = conPart.substring(OAUTH_ACCESS_TOKEN_URL_PART.length());
				else if (conPartLower.startsWith(SIMULATE_PRODUCT_NAME.toLowerCase()))
					res.productName = conPart.substring(SIMULATE_PRODUCT_NAME.length());
				else if (conPartLower.startsWith(SIMULATE_PRODUCT_MAJOR_VERSION.toLowerCase()))
					res.majorVersion = parseInteger(conPart.substring(SIMULATE_PRODUCT_MAJOR_VERSION.length()));
				else if (conPartLower.startsWith(SIMULATE_PRODUCT_MINOR_VERSION.toLowerCase()))
					res.minorVersion = parseInteger(conPart.substring(SIMULATE_PRODUCT_MINOR_VERSION.length()));
				else if (conPartLower.startsWith(ALLOW_EXTENDED_MODE.toLowerCase()))
					res.allowExtendedMode = Boolean.valueOf(conPart.substring(ALLOW_EXTENDED_MODE.length()));
				else if (conPartLower.startsWith(ASYNC_DDL_OPERATIONS.toLowerCase()))
					res.asyncDdlOperations = Boolean.valueOf(conPart.substring(ASYNC_DDL_OPERATIONS.length()));
				else if (conPartLower.startsWith(AUTO_BATCH_DDL_OPERATIONS.toLowerCase()))
					res.autoBatchDdlOperations = Boolean.valueOf(conPart.substring(AUTO_BATCH_DDL_OPERATIONS.length()));
				else if (conPartLower.startsWith(REPORT_DEFAULT_SCHEMA_AS_NULL.toLowerCase()))
					res.reportDefaultSchemaAsNull = Boolean
							.valueOf(conPart.substring(REPORT_DEFAULT_SCHEMA_AS_NULL.length()));
				else if (conPartLower.startsWith(BATCH_READ_ONLY_MODE.toLowerCase()))
					res.batchReadOnlyMode = Boolean.valueOf(conPart.substring(BATCH_READ_ONLY_MODE.length()));
				else
					throw new CloudSpannerSQLException("Unknown URL parameter " + conPart, Code.INVALID_ARGUMENT);
			}
		}
		return res;
	}

	private static Integer parseInteger(String val)
	{
		try
		{
			return Integer.valueOf(val);
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	private static String defaultString(Integer val)
	{
		return val == null ? null : val.toString();
	}

	void setAdditionalConnectionProperties(Properties info)
	{
		if (info != null)
		{
			// Make all lower case
			Properties lowerCaseInfo = new Properties();
			for (String key : info.stringPropertyNames())
			{
				lowerCaseInfo.setProperty(key.toLowerCase(), info.getProperty(key));
			}

			project = lowerCaseInfo
					.getProperty(PROJECT_URL_PART.substring(0, PROJECT_URL_PART.length() - 1).toLowerCase(), project);
			instance = lowerCaseInfo.getProperty(
					INSTANCE_URL_PART.substring(0, INSTANCE_URL_PART.length() - 1).toLowerCase(), instance);
			database = lowerCaseInfo.getProperty(
					DATABASE_URL_PART.substring(0, DATABASE_URL_PART.length() - 1).toLowerCase(), database);
			keyFile = lowerCaseInfo
					.getProperty(KEY_FILE_URL_PART.substring(0, KEY_FILE_URL_PART.length() - 1).toLowerCase(), keyFile);
			oauthToken = lowerCaseInfo.getProperty(
					OAUTH_ACCESS_TOKEN_URL_PART.substring(0, OAUTH_ACCESS_TOKEN_URL_PART.length() - 1).toLowerCase(),
					oauthToken);
			productName = lowerCaseInfo.getProperty(
					SIMULATE_PRODUCT_NAME.substring(0, SIMULATE_PRODUCT_NAME.length() - 1).toLowerCase(), productName);
			majorVersion = parseInteger(lowerCaseInfo.getProperty(SIMULATE_PRODUCT_MAJOR_VERSION
					.substring(0, SIMULATE_PRODUCT_MAJOR_VERSION.length() - 1).toLowerCase(),
					defaultString(majorVersion)));
			minorVersion = parseInteger(lowerCaseInfo.getProperty(SIMULATE_PRODUCT_MINOR_VERSION
					.substring(0, SIMULATE_PRODUCT_MINOR_VERSION.length() - 1).toLowerCase(),
					defaultString(minorVersion)));
			allowExtendedMode = Boolean.valueOf(lowerCaseInfo.getProperty(
					ALLOW_EXTENDED_MODE.substring(0, ALLOW_EXTENDED_MODE.length() - 1).toLowerCase(),
					String.valueOf(allowExtendedMode)));
			asyncDdlOperations = Boolean.valueOf(lowerCaseInfo.getProperty(
					ASYNC_DDL_OPERATIONS.substring(0, ASYNC_DDL_OPERATIONS.length() - 1).toLowerCase(),
					String.valueOf(asyncDdlOperations)));
			autoBatchDdlOperations = Boolean.valueOf(lowerCaseInfo.getProperty(
					AUTO_BATCH_DDL_OPERATIONS.substring(0, AUTO_BATCH_DDL_OPERATIONS.length() - 1).toLowerCase(),
					String.valueOf(autoBatchDdlOperations)));
			reportDefaultSchemaAsNull = Boolean
					.valueOf(
							lowerCaseInfo.getProperty(
									REPORT_DEFAULT_SCHEMA_AS_NULL
											.substring(0, REPORT_DEFAULT_SCHEMA_AS_NULL.length() - 1).toLowerCase(),
									String.valueOf(reportDefaultSchemaAsNull)));
			batchReadOnlyMode = Boolean.valueOf(lowerCaseInfo.getProperty(
					BATCH_READ_ONLY_MODE.substring(0, BATCH_READ_ONLY_MODE.length() - 1).toLowerCase(),
					String.valueOf(batchReadOnlyMode)));
			if (!CloudSpannerDriver.logLevelSet)
				CloudSpannerDriver.setLogLevel(CloudSpannerDriver.OFF);
		}
	}

	DriverPropertyInfo[] getPropertyInfo()
	{
		DriverPropertyInfo[] res = new DriverPropertyInfo[NUMBER_OF_PROPERTIES];
		res[0] = new DriverPropertyInfo(PROJECT_URL_PART.substring(0, PROJECT_URL_PART.length() - 1), project);
		res[0].description = "Google Cloud Project id";
		res[1] = new DriverPropertyInfo(INSTANCE_URL_PART.substring(0, INSTANCE_URL_PART.length() - 1), instance);
		res[1].description = "Google Cloud Spanner Instance id";
		res[2] = new DriverPropertyInfo(DATABASE_URL_PART.substring(0, DATABASE_URL_PART.length() - 1), database);
		res[2].description = "Google Cloud Spanner Database name";
		res[3] = new DriverPropertyInfo(KEY_FILE_URL_PART.substring(0, KEY_FILE_URL_PART.length() - 1), keyFile);
		res[3].description = "Path to json key file to be used for authentication";
		res[4] = new DriverPropertyInfo(
				OAUTH_ACCESS_TOKEN_URL_PART.substring(0, OAUTH_ACCESS_TOKEN_URL_PART.length() - 1), oauthToken);
		res[4].description = "OAuth access token to be used for authentication (optional, only to be used when no key file is specified)";
		res[5] = new DriverPropertyInfo(SIMULATE_PRODUCT_NAME.substring(0, SIMULATE_PRODUCT_NAME.length() - 1),
				productName);
		res[5].description = "Use this property to make the driver return a different database product name than Google Cloud Spanner, for example if you are using a framework like Spring that use this property to determine how to a generate data model for Spring Batch";
		res[6] = new DriverPropertyInfo(
				SIMULATE_PRODUCT_MAJOR_VERSION.substring(0, SIMULATE_PRODUCT_MAJOR_VERSION.length() - 1),
				defaultString(majorVersion));
		res[6].description = "Use this property to make the driver return a different major version number, for example if you are using a framework like Spring that use this property to determine how to a generate data model for Spring Batch";
		res[7] = new DriverPropertyInfo(
				SIMULATE_PRODUCT_MINOR_VERSION.substring(0, SIMULATE_PRODUCT_MINOR_VERSION.length() - 1),
				defaultString(minorVersion));
		res[7].description = "Use this property to make the driver return a different minor version number, for example if you are using a framework like Spring that use this property to determine how to a generate data model for Spring Batch";
		res[8] = new DriverPropertyInfo(ALLOW_EXTENDED_MODE.substring(0, ALLOW_EXTENDED_MODE.length() - 1),
				String.valueOf(allowExtendedMode));
		res[8].description = "Allow the driver to enter 'extended' mode for bulk operations. A value of false (default) indicates that the driver should never enter extended mode. If this property is set to true, the driver will execute all bulk DML-operations in a separate transaction when the number of records affected is greater than what will exceed the limitations of Cloud Spanner.";
		res[9] = new DriverPropertyInfo(ASYNC_DDL_OPERATIONS.substring(0, ASYNC_DDL_OPERATIONS.length() - 1),
				String.valueOf(asyncDdlOperations));
		res[9].description = "Run DDL-operations (CREATE TABLE, ALTER TABLE, DROP TABLE, etc.) in asynchronous mode. When set to true, DDL-statements will be checked for correct syntax and other basic checks before the call returns. It can take up to several minutes before the statement has actually finished executing. The status of running DDL-operations can be queried by issuing a SHOW_DDL_OPERATIONS statement. DDL-operations that have finished can be cleared from this view by issuing a CLEAN_DDL_OPERATIONS statement.";
		res[10] = new DriverPropertyInfo(AUTO_BATCH_DDL_OPERATIONS.substring(0, AUTO_BATCH_DDL_OPERATIONS.length() - 1),
				String.valueOf(autoBatchDdlOperations));
		res[10].description = "Automatically batch DDL-operations (CREATE TABLE, ALTER TABLE, DROP TABLE, etc.). When set to true, DDL-statements that are submitted through a Statement (not PreparedStatement) will automatically be batched together and only executed after an EXECUTE_DDL_BATCH statement. This property can be used in combination with the AsyncDdlOperations property to run a batch asynchronously or synchronously.";
		res[11] = new DriverPropertyInfo(
				REPORT_DEFAULT_SCHEMA_AS_NULL.substring(0, REPORT_DEFAULT_SCHEMA_AS_NULL.length() - 1),
				String.valueOf(reportDefaultSchemaAsNull));
		res[11].description = "Report the default schema and catalog as null (true) or as an empty string (false).";
		res[12] = new DriverPropertyInfo(BATCH_READ_ONLY_MODE.substring(0, BATCH_READ_ONLY_MODE.length() - 1),
				String.valueOf(batchReadOnlyMode));
		res[12].description = "Run queries in batch-read-only-mode. Use this mode when downloading large amounts of data from Cloud Spanner in combination with the methods Statement#execute(String) or PreparedStatement#execute()";

		return res;
	}
}