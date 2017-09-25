package nl.topicus.jdbc.extended;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConverterConfiguration
{
	public static enum DatabaseType
	{
		CloudSpanner
		{
			@Override
			public boolean isType(String url)
			{
				return url.toLowerCase().startsWith("jdbc:cloudspanner");
			}

			@Override
			public boolean isPrimaryKeyDefinitionInsideColumnList()
			{
				return false;
			}

			@Override
			public String getDefaultSchemaName()
			{
				return "";
			}

			@Override
			public boolean isSystemSchema(String schema)
			{
				return schema != null && schema.equalsIgnoreCase("INFORMATION_SCHEMA");
			}
		},
		PostgreSQL
		{
			@Override
			public boolean isType(String url)
			{
				return url.toLowerCase().startsWith("jdbc:postgresql");
			}

			@Override
			public boolean isPrimaryKeyDefinitionInsideColumnList()
			{
				return true;
			}

			@Override
			public String getDefaultSchemaName()
			{
				return "public";
			}

			@Override
			public boolean isSystemSchema(String schema)
			{
				return schema != null
						&& (schema.equalsIgnoreCase("INFORMATION_SCHEMA") || schema.toUpperCase().startsWith("PG_"));
			}
		};

		public abstract boolean isType(String url);

		public abstract boolean isPrimaryKeyDefinitionInsideColumnList();

		public abstract String getDefaultSchemaName();

		public abstract boolean isSystemSchema(String schema);

		public static DatabaseType getType(String url)
		{
			for (DatabaseType type : DatabaseType.values())
				if (type.isType(url))
					return type;
			return null;
		}
	}

	private final Properties properties = new Properties();

	private ConvertMode tableConvertMode;

	private ConvertMode dataConvertMode;

	private Integer numberOfTableWorkers;

	private Integer batchSize;

	private Integer maxNumberOfWorkers;

	/**
	 * Maximum time to wait for a table worker to finish in minutes
	 */
	private Integer tableWorkerMaxWaitInMinutes;

	/**
	 * Maximum time to wait for an upload worker to finish in minutes
	 */
	private Integer uploadWorkerMaxWaitInMinutes;

	private Boolean useJdbcBatching;

	private final String urlSource;

	private final String urlDestination;

	private String catalog;

	private String schema;

	/**
	 * Cloud Spanner has a strict limit on the size of one transaction. When
	 * deleting large tables, the delete needs to be chopped into several delete
	 * statements to avoid exceeding the transaction limit
	 */
	private Long maxRecordsInSingleDeleteStatement;

	/**
	 * Create a default converter configuration
	 */
	public ConverterConfiguration(String urlSource, String urlDestination)
	{
		this.urlSource = urlSource;
		this.urlDestination = urlDestination;
		tableConvertMode = ConvertMode.SkipExisting;
		dataConvertMode = ConvertMode.SkipExisting;
		setDefaults();
	}

	/**
	 * Create a converter configuration from a properties file
	 */
	public ConverterConfiguration(String urlSource, String urlDestination, URI file) throws IOException
	{
		this.urlSource = urlSource;
		this.urlDestination = urlDestination;
		properties.load(Files.newBufferedReader(Paths.get(file)));
		setDefaults();
	}

	private void setDefaults()
	{
		if (getDestinationDatabaseType() == DatabaseType.CloudSpanner && maxRecordsInSingleDeleteStatement == null)
		{
			maxRecordsInSingleDeleteStatement = getBatchSize().longValue();
		}
	}

	public ConvertMode getTableConvertMode()
	{
		if (tableConvertMode == null)
		{
			tableConvertMode = ConvertMode.valueOf(ConvertMode.class,
					properties.getProperty("TableConverter.convertMode", ConvertMode.SkipExisting.name()));
		}
		return tableConvertMode;
	}

	public ConvertMode getDataConvertMode()
	{
		if (dataConvertMode == null)
		{
			dataConvertMode = ConvertMode.valueOf(ConvertMode.class,
					properties.getProperty("DataConverter.convertMode", ConvertMode.SkipExisting.name()));
		}
		return dataConvertMode;
	}

	public Integer getBatchSize()
	{
		if (batchSize == null)
		{
			batchSize = Integer.valueOf(properties.getProperty("DataConverter.batchSize", "1500000"));
		}
		return batchSize;
	}

	public Integer getNumberOfTableWorkers()
	{
		if (numberOfTableWorkers == null)
		{
			numberOfTableWorkers = Integer.valueOf(properties.getProperty("DataConverter.numberOfTableWorkers", "10"));
		}
		return numberOfTableWorkers;
	}

	public Integer getMaxNumberOfWorkers()
	{
		if (maxNumberOfWorkers == null)
		{
			maxNumberOfWorkers = Integer.valueOf(properties.getProperty("DataConverter.maxNumberOfWorkers", "10"));
		}
		return maxNumberOfWorkers;
	}

	public Integer getTableWorkerMaxWaitInMinutes()
	{
		if (tableWorkerMaxWaitInMinutes == null)
		{
			tableWorkerMaxWaitInMinutes = Integer
					.valueOf(properties.getProperty("DataConverter.tableWorkerMaxWaitInMinutes", "60"));
		}
		return tableWorkerMaxWaitInMinutes;
	}

	public Integer getUploadWorkerMaxWaitInMinutes()
	{
		if (uploadWorkerMaxWaitInMinutes == null)
		{
			uploadWorkerMaxWaitInMinutes = Integer
					.valueOf(properties.getProperty("DataConverter.uploadWorkerMaxWaitInMinutes", "60"));
		}
		return uploadWorkerMaxWaitInMinutes;
	}

	public boolean isUseJdbcBatching()
	{
		if (useJdbcBatching == null)
		{
			useJdbcBatching = Boolean.valueOf(properties.getProperty("DataConverter.useJdbcBatching", "true"));
		}
		return useJdbcBatching.booleanValue();
	}

	public String getCatalog()
	{
		if (catalog == null)
		{
			catalog = properties.getProperty("catalog", null);
		}
		return catalog;
	}

	public String getSchema()
	{
		if (schema == null)
		{
			schema = properties.getProperty("schema", null);
		}
		return schema;
	}

	public Map<String, String> getSpecificColumnMappings()
	{
		Map<String, String> res = new HashMap<>();
		for (String key : properties.stringPropertyNames())
		{
			if (key.startsWith("TableConverter.specificColumnMapping."))
			{
				String column = key.substring("TableConverter.specificColumnMapping.".length());
				String value = properties.getProperty(key);
				res.put(column, value);
			}
		}

		return res;
	}

	public String getUrlSource()
	{
		return urlSource;
	}

	public String getUrlDestination()
	{
		return urlDestination;
	}

	public DatabaseType getSourceDatabaseType()
	{
		return DatabaseType.getType(urlSource);
	}

	public DatabaseType getDestinationDatabaseType()
	{
		return DatabaseType.getType(urlDestination);
	}

	public boolean isPrimaryKeyDefinitionInsideColumnList()
	{
		return getDestinationDatabaseType().isPrimaryKeyDefinitionInsideColumnList();
	}

	protected Long getMaxRecordsInSingleDeleteStatement()
	{
		return maxRecordsInSingleDeleteStatement;
	}

}
