package nl.topicus.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(

{ CloudSpannerDatabaseMetaDataTest.class, CloudSpannerDriverTest.class })
public class TestSuite
{ // nothing
}
