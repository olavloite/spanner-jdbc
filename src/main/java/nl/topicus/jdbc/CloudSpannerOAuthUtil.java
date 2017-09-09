package nl.topicus.jdbc;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;

public class CloudSpannerOAuthUtil
{
	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	static final HttpTransportFactory HTTP_TRANSPORT_FACTORY = new DefaultHttpTransportFactory();

	static class DefaultHttpTransportFactory implements HttpTransportFactory
	{

		@Override
		public HttpTransport create()
		{
			return HTTP_TRANSPORT;
		}
	}

	private CloudSpannerOAuthUtil()
	{
	}

}
