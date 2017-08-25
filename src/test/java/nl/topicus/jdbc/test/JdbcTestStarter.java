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
		JdbcIntegrationTest tester = new JdbcIntegrationTest();
		tester.performDatabaseTests();
	}
}
