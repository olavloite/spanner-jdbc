package nl.topicus.jdbc.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

class TestUtil
{

	static String getResource(Class<?> clazz, String name) throws IOException, URISyntaxException
	{
		return String.join("\n", Files.readAllLines(Paths.get(clazz.getResource(name).toURI())));
	}

}
