package nl.topicus.jdbc.test.integration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtil
{

	public static String getSingleStatement(Class<?> clazz, String name) throws IOException, URISyntaxException
	{
		return String.join("\n", Files.readAllLines(Paths.get(clazz.getResource(name).toURI())));
	}

	public static String[] getMultipleStatements(Class<?> clazz, String name) throws IOException, URISyntaxException
	{
		return String.join("\n", Files.readAllLines(Paths.get(clazz.getResource(name).toURI()))).split(";");
	}

}
