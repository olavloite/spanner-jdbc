package nl.topicus.jdbc.test;

public class JdbcTestStarter
{

	/**
	 * Expected parameters: projectId, keyFile
	 * 
	 * @param args
	 *            Array containing information of the test database to use.
	 */
	public static void main(String... args)
	{
		if (args == null || args.length != 2)
			throw new IllegalArgumentException(
					"Unexpected number of arguments found. Usage: JdbcTester projectId keyFile");
		String projectId = args[0];
		String keyPath = args[1];
		JdbcTester tester = new JdbcTester(projectId, keyPath);
		tester.performTests();
	}
}
