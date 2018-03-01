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

	<T extends ConnectionProperty> Object get(Class<T> connectionProperty)
	{
		ConnectionProperty key = connectionProperties.keySet().stream()
				.filter(cp -> cp.getClass().equals(connectionProperty)).findFirst().orElse(null);
		return key == null ? null : connectionProperties.get(key);
	}

	Object get(String name)
	{
		ConnectionProperty key = connectionProperties.keySet().stream().filter(cp -> cp.name().equals(name)).findFirst()
				.orElse(null);
		return key == null ? null : connectionProperties.get(key);
	}

}
