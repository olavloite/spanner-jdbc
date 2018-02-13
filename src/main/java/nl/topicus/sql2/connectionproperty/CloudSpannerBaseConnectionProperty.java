package nl.topicus.sql2.connectionproperty;

import nl.topicus.java.sql2.ConnectionProperty;

public class CloudSpannerBaseConnectionProperty implements ConnectionProperty
{
	private String name;

	private String defaultValue;

	public CloudSpannerBaseConnectionProperty(String name)
	{
		this(name, null);
	}

	public CloudSpannerBaseConnectionProperty(String name, String defaultValue)
	{
		this.name = name;
		this.defaultValue = defaultValue;
	}

	@Override
	public String name()
	{
		return name;
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

	@Override
	public String defaultValue()
	{
		return defaultValue;
	}

	@Override
	public boolean isSensitive()
	{
		return false;
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof String)
			return ((String) o).equalsIgnoreCase(name);
		if (o instanceof ConnectionProperty)
			return ((ConnectionProperty) o).name().equalsIgnoreCase(name);
		return false;
	}

}
