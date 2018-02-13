package nl.topicus.sql2;

import java.util.HashMap;
import java.util.Map;

import nl.topicus.java.sql2.ConnectionProperty;

class ConnectionProperties
{
	private final Map<ConnectionProperty, Object> connectionProperties = new HashMap<>();

	ConnectionProperties()
	{
	}

	Object set(ConnectionProperty connectionProperty, Object value)
	{
		return connectionProperties.put(connectionProperty, value);
	}

	@SuppressWarnings("unlikely-arg-type")
	<T extends ConnectionProperty> Object get(Class<T> connectionProperty)
	{
		return connectionProperties.get(connectionProperty);
	}

	@SuppressWarnings("unlikely-arg-type")
	Object get(String name)
	{
		return connectionProperties.get(name);
	}

}
