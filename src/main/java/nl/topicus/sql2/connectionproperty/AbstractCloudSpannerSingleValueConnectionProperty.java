package nl.topicus.sql2.connectionproperty;

import nl.topicus.java.sql2.ConnectionProperty;

public abstract class AbstractCloudSpannerSingleValueConnectionProperty<T> implements ConnectionProperty
{
	private String name;

	private T defaultValue;

	protected AbstractCloudSpannerSingleValueConnectionProperty(String name)
	{
		this(name, null);
	}

	protected AbstractCloudSpannerSingleValueConnectionProperty(String name, T defaultValue)
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
	public abstract Class<T> range();

	@Override
	public T defaultValue()
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
		if (o instanceof Class)
			return o.equals(this.getClass());
		return false;
	}

}
