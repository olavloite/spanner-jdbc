package nl.topicus.jdbc.emulator;

import org.junit.Test;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

public class SpannerEmulatorTest
{

	@Test
	public void testCreateSpannerEmulator()
	{
		SpannerOptions options = SpannerOptions.newBuilder().build();
		Spanner spanner = options.getService();
	}

}
