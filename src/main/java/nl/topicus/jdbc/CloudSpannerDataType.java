package nl.topicus.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum CloudSpannerDataType
{
	BOOL
	{
		@Override
		public int getSqlType()
		{
			return Types.BOOLEAN;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return Boolean.class;
		}
	},
	BYTES
	{
		@Override
		public int getSqlType()
		{
			return Types.BINARY;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return byte[].class;
		}
	},
	DATE
	{
		@Override
		public int getSqlType()
		{
			return Types.DATE;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return Date.class;
		}
	},
	FLOAT64
	{
		private Set<Class<?>> classes = new HashSet<>(Arrays.asList(BigDecimal.class, Float.class, Double.class));

		@Override
		public int getSqlType()
		{
			return Types.DOUBLE;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return Double.class;
		}

		@Override
		public Set<Class<?>> getSupportedJavaClasses()
		{
			return classes;
		}
	},
	INT64
	{
		private Set<Class<?>> classes = new HashSet<>(Arrays.asList(Byte.class, Integer.class, Long.class));

		@Override
		public int getSqlType()
		{
			return Types.BIGINT;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return Long.class;
		}

		@Override
		public Set<Class<?>> getSupportedJavaClasses()
		{
			return classes;
		}
	},
	STRING
	{
		@Override
		public int getSqlType()
		{
			return Types.NVARCHAR;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return String.class;
		}
	},
	TIMESTAMP
	{
		@Override
		public int getSqlType()
		{
			return Types.TIMESTAMP;
		}

		@Override
		public Class<?> getJavaClass()
		{
			return Timestamp.class;
		}
	};

	public abstract int getSqlType();

	public String getTypeName()
	{
		return name();
	}

	public abstract Class<?> getJavaClass();

	public Set<Class<?>> getSupportedJavaClasses()
	{
		return Collections.singleton(getJavaClass());
	}

	public static CloudSpannerDataType getType(Class<?> clazz)
	{
		return null;
	}

}
