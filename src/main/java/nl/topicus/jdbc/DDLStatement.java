package nl.topicus.jdbc;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

class DDLStatement
{
	private static final Pattern IF_EXISTS_PATTERN = Pattern.compile("(?i)if\\s+exists");
	private static final Pattern IF_NOT_EXISTS_PATTERN = Pattern.compile("(?i)if\\s+not\\s+exists");

	enum Command
	{
		UNKNOWN, DROP, CREATE;
	}

	enum ObjectType
	{
		UNKNOWN
		{
			@Override
			boolean exists(CloudSpannerConnection connection, String objectName) throws SQLException
			{
				return false;
			}
		},
		TABLE
		{
			@Override
			boolean exists(CloudSpannerConnection connection, String objectName) throws SQLException
			{
				try (ResultSet rs = connection.getMetaData().getTables("", "", objectName, null))
				{
					return rs.next();
				}
			}
		},
		INDEX
		{
			@Override
			boolean exists(CloudSpannerConnection connection, String objectName) throws SQLException
			{
				try (ResultSet rs = connection.getMetaData().getIndexInfo("", "", objectName))
				{
					return rs.next();
				}
			}
		};

		abstract boolean exists(CloudSpannerConnection connection, String objectName) throws SQLException;
	}

	enum ExistsStatement
	{
		NONE
		{
			@Override
			String removeExistsStatement(String sql)
			{
				return sql;
			}

			@Override
			boolean shouldExecute(boolean exists)
			{
				return false;
			}
		},
		IF_EXISTS
		{
			@Override
			String removeExistsStatement(String sql)
			{
				return IF_EXISTS_PATTERN.matcher(sql).replaceFirst(" ");
			}

			@Override
			boolean shouldExecute(boolean exists)
			{
				return exists;
			}
		},
		IF_NOT_EXISTS
		{
			@Override
			String removeExistsStatement(String sql)
			{
				return IF_NOT_EXISTS_PATTERN.matcher(sql).replaceFirst(" ");
			}

			@Override
			boolean shouldExecute(boolean exists)
			{
				return !exists;
			}
		};
		abstract String removeExistsStatement(String sql);

		abstract boolean shouldExecute(boolean exists);
	}

	private final Command command;

	private final ObjectType objectType;

	private final ExistsStatement existsStatement;

	private final String objectName;

	private final String sql;

	private DDLStatement(Command command, ObjectType objectType, ExistsStatement existsStatement, String objectName,
			String sql)
	{
		this.command = command;
		this.objectType = objectType;
		this.existsStatement = existsStatement;
		this.objectName = objectName;
		this.sql = sql;
	}

	static List<DDLStatement> parseDdlStatements(List<String> sqlList) throws SQLException
	{
		List<DDLStatement> res = new ArrayList<>(sqlList.size());
		for (String sql : sqlList)
		{
			// Max possible length that we want to check is:
			// 'CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS index_name'
			List<String> tokens = getTokens(sql, 8);
			// Shortest possible string that we want to check is:
			// 'DROP TABLE IF EXISTS table_name
			if (tokens.size() >= 3)
			{
				boolean create = tokens.get(0).equalsIgnoreCase("create");
				boolean drop = tokens.get(0).equalsIgnoreCase("drop");
				boolean table = tokens.get(1).equalsIgnoreCase("table");
				boolean index = tokens.get(1).equalsIgnoreCase("index");
				boolean uniqueIndex = tokens.get(1).equalsIgnoreCase("unique")
						&& tokens.get(2).equalsIgnoreCase("index");
				boolean nullFilteredIndex = tokens.get(1).equalsIgnoreCase("null_filtered")
						&& tokens.get(2).equalsIgnoreCase("index");
				boolean uniqueNullFilteredIndex = tokens.size() >= 4 && ((tokens.get(1).equalsIgnoreCase("unique")
						&& tokens.get(2).equalsIgnoreCase("null_filtered") && tokens.get(3).equalsIgnoreCase("index"))
						|| (tokens.get(1).equalsIgnoreCase("null_filtered") && tokens.get(2).equalsIgnoreCase("unique")
								&& tokens.get(3).equalsIgnoreCase("index")));
				int currentIndex = 2;
				if (uniqueIndex || nullFilteredIndex)
					currentIndex = 3;
				else if (uniqueNullFilteredIndex)
					currentIndex = 4;
				// Check for exists statement
				boolean exists = tokens.size() > currentIndex + 2 && tokens.get(currentIndex).equalsIgnoreCase("if")
						&& tokens.get(currentIndex + 1).equalsIgnoreCase("exists");
				boolean notExists = tokens.size() > currentIndex + 3 && tokens.get(currentIndex).equalsIgnoreCase("if")
						&& tokens.get(currentIndex + 1).equalsIgnoreCase("not")
						&& tokens.get(currentIndex + 2).equalsIgnoreCase("exists");
				if (exists)
					currentIndex += 2;
				if (notExists)
					currentIndex += 3;

				String name = tokens.size() > currentIndex ? tokens.get(currentIndex) : "";
				Command command = create ? Command.CREATE : drop ? Command.DROP : Command.UNKNOWN;
				ObjectType objectType = table ? ObjectType.TABLE
						: index || uniqueIndex || nullFilteredIndex || uniqueNullFilteredIndex ? ObjectType.INDEX
								: ObjectType.UNKNOWN;
				ExistsStatement existsStatement = exists ? ExistsStatement.IF_EXISTS
						: notExists ? ExistsStatement.IF_NOT_EXISTS : ExistsStatement.NONE;

				String commandSql = existsStatement.removeExistsStatement(sql);
				res.add(new DDLStatement(command, objectType, existsStatement, name, commandSql));
			}

		}
		return res;
	}

	private static List<String> getTokens(String sql, int maxTokens) throws SQLException
	{
		List<String> res = new ArrayList<>(maxTokens);
		int tokenNumber = 0;
		StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(sql));
		tokenizer.quoteChar('`');
		try
		{
			while (tokenizer.nextToken() != StreamTokenizer.TT_EOF
					&& (tokenizer.ttype == StreamTokenizer.TT_WORD || tokenizer.ttype == '`')
					&& tokenNumber < maxTokens)
			{
				res.add(tokenizer.sval);
				tokenNumber++;
			}
		}
		catch (IOException e)
		{
			throw new CloudSpannerSQLException("Could not parse DDL statement '" + sql + "'. Error: " + e.getMessage(),
					Code.INVALID_ARGUMENT, e);
		}
		return res;
	}

	boolean shouldExecute(CloudSpannerConnection connection) throws SQLException
	{
		if (getExistsStatement() == null || getExistsStatement() == ExistsStatement.NONE)
			return true;
		if (getExistsStatement() == ExistsStatement.IF_NOT_EXISTS && getCommand() == Command.DROP)
			throw new CloudSpannerSQLException("Invalid argument: Cannot use 'IF NOT EXISTS' when dropping an object",
					Code.INVALID_ARGUMENT);
		if (getExistsStatement() == ExistsStatement.IF_EXISTS && getCommand() == Command.CREATE)
			throw new CloudSpannerSQLException("Invalid argument: Cannot use 'IF EXISTS' when creating an object",
					Code.INVALID_ARGUMENT);

		return getExistsStatement().shouldExecute(getObjectType().exists(connection, getObjectName()));
	}

	Command getCommand()
	{
		return command;
	}

	ObjectType getObjectType()
	{
		return objectType;
	}

	ExistsStatement getExistsStatement()
	{
		return existsStatement;
	}

	String getObjectName()
	{
		return objectName;
	}

	String getSql()
	{
		return sql;
	}

}
