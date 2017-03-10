package nl.topicus.jdbc.statement;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ValueBinder;

class ValueBinderExpressionVisitorAdapter<R> extends AbstractSpannerSetValueVisitor<R>
{
	private ValueBinder<R> binder;

	ValueBinderExpressionVisitorAdapter(ParameterStore parameterStore, ValueBinder<R> binder)
	{
		super(parameterStore);
		this.binder = binder;
	}

	@Override
	protected void setValue(Object value)
	{
		if (value == null)
		{
			// Set to null, type does not matter
			binder.to((Boolean) null);
		}
		else if (Boolean.class.isAssignableFrom(value.getClass()))
		{
			binder.to((Boolean) value);
		}
		else if (Byte.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((Byte) value).longValue());
		}
		else if (Integer.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((Integer) value).longValue());
		}
		else if (Long.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((Long) value).longValue());
		}
		else if (Float.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((Float) value).doubleValue());
		}
		else if (Double.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((Double) value).doubleValue());
		}
		else if (BigDecimal.class.isAssignableFrom(value.getClass()))
		{
			binder.to(((BigDecimal) value).doubleValue());
		}
		else if (Date.class.isAssignableFrom(value.getClass()))
		{
			Date dateValue = (Date) value;
			binder.to(CloudSpannerConversionUtil.toCloudSpannerDate(dateValue));
		}
		else if (Timestamp.class.isAssignableFrom(value.getClass()))
		{
			Timestamp timeValue = (Timestamp) value;
			binder.to(CloudSpannerConversionUtil.toCloudSpannerTimestamp(timeValue));
		}
		else if (String.class.isAssignableFrom(value.getClass()))
		{
			binder.to((String) value);
		}
		else if (byte[].class.isAssignableFrom(value.getClass()))
		{
			binder.to(ByteArray.copyFrom((byte[]) value));
		}
		else
		{
			throw new IllegalArgumentException("Unsupported parameter type: " + value.getClass().getName() + " - "
					+ value.toString());
		}
	}

}
