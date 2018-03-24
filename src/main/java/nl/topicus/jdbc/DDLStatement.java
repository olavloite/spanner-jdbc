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

	private static final String CREATE_KEYWORD = "create";
	private static final String DROP_KEYWORD = "drop";
	private static final String TABLE_KEYWORD = "table";
	private static final String INDEX_KEYWORD = "index";
	private static final String UNIQUE_KEYWORD = "unique";
	private static final String NULL_FILTERED_KEYWORD = "null_filtered";
	private static final String EXISTS_KEYWORD = "exists";
	private static final String IF_KEYWORD = "if";
	private static final String NOT_KEYWORD = "not";

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
				boolean create = tokens.get(0).equalsIgnoreCase(CREATE_KEYWORD);
				boolean drop = tokens.get(0).equalsIgnoreCase(DROP_KEYWORD);
				boolean table = tokens.get(1).equalsIgnoreCase(TABLE_KEYWORD);
				boolean index = tokens.get(1).equalsIgnoreCase(INDEX_KEYWORD);
				boolean uniqueIndex = tokens.get(1).equalsIgnoreCase(UNIQUE_KEYWORD)
						&& tokens.get(2).equalsIgnoreCase(INDEX_KEYWORD);
				boolean nullFilteredIndex = tokens.get(1).equalsIgnoreCase(NULL_FILTERED_KEYWORD)
						&& tokens.get(2).equalsIgnoreCase(INDEX_KEYWORD);
				boolean uniqueNullFilteredIndex = tokens.size() >= 4 && ((tokens.get(1).equalsIgnoreCase(UNIQUE_KEYWORD)
						&& tokens.get(2).equalsIgnoreCase(NULL_FILTERED_KEYWORD)
						&& tokens.get(3).equalsIgnoreCase(INDEX_KEYWORD))
						|| (tokens.get(1).equalsIgnoreCase(NULL_FILTERED_KEYWORD)
								&& tokens.get(2).equalsIgnoreCase(UNIQUE_KEYWORD)
								&& tokens.get(3).equalsIgnoreCase(INDEX_KEYWORD)));
				int currentIndex = 2;
				if (uniqueIndex || nullFilteredIndex)
					currentIndex = 3;
				else if (uniqueNullFilteredIndex)
					currentIndex = 4;
				// Check for exists statement
				boolean exists = tokens.size() > currentIndex + 2
						&& tokens.get(currentIndex).equalsIgnoreCase(IF_KEYWORD)
						&& tokens.get(currentIndex + 1).equalsIgnoreCase(EXISTS_KEYWORD);
				boolean notExists = tokens.size() > currentIndex + 3
						&& tokens.get(currentIndex).equalsIgnoreCase(IF_KEYWORD)
						&& tokens.get(currentIndex + 1).equalsIgnoreCase(NOT_KEYWORD)
						&& tokens.get(currentIndex + 2).equalsIgnoreCase(EXISTS_KEYWORD);
				if (exists)
					currentIndex += 2;
				if (notExists)
					currentIndex += 3;
				String name = tokens.size() > currentIndex ? tokens.get(currentIndex) : "";

				Command command;
				if (create)
					command = Command.CREATE;
				else if (drop)
					command = Command.DROP;
				else
					command = Command.UNKNOWN;

				ObjectType objectType;
				if (table)
					objectType = ObjectType.TABLE;
				else if (index || uniqueIndex || nullFilteredIndex || uniqueNullFilteredIndex)
					objectType = ObjectType.INDEX;
				else
					objectType = ObjectType.UNKNOWN;

				ExistsStatement existsStatement;
				if (exists)
					existsStatement = ExistsStatement.IF_EXISTS;
				else if (notExists)
					existsStatement = ExistsStatement.IF_NOT_EXISTS;
				else
					existsStatement = ExistsStatement.NONE;

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
		tokenizer.eolIsSignificant(false);
		tokenizer.wordChars('_', '_');
		tokenizer.wordChars('"', '"');
		tokenizer.wordChars('\'', '\'');
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
