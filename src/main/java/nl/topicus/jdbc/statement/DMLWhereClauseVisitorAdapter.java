package nl.topicus.jdbc.statement;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Pivot;
import net.sf.jsqlparser.statement.select.PivotXml;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

abstract class DMLWhereClauseVisitorAdapter extends ExpressionVisitorAdapter
{
	private boolean invalid = false;

	@Override
	public void visit(NullValue value)
	{
		invalid = true;
		super.visit(value);
	}

	@Override
	public void visit(Function function)
	{
		invalid = true;
		super.visit(function);
	}

	@Override
	public void visit(JdbcNamedParameter parameter)
	{
		invalid = true;
		super.visit(parameter);
	}

	@Override
	public void visit(Addition expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Division expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Multiplication expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Subtraction expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(OrExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Between expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(GreaterThan expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(GreaterThanEquals expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(InExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(IsNullExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(LikeExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(MinorThan expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(MinorThanEquals expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(NotEqualsTo expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(SubSelect subSelect)
	{
		invalid = true;
		super.visit(subSelect);
	}

	@Override
	public void visit(CaseExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(WhenClause expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(ExistsExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(AllComparisonExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(AnyComparisonExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Concat expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Matches expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(BitwiseAnd expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(BitwiseOr expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(BitwiseXor expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(CastExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(Modulo expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(AnalyticExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(ExtractExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(IntervalExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(OracleHierarchicalExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(RegExpMatchOperator expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(ExpressionList expressionList)
	{
		invalid = true;
		super.visit(expressionList);
	}

	@Override
	public void visit(MultiExpressionList multiExprList)
	{
		invalid = true;
		super.visit(multiExprList);
	}

	@Override
	public void visit(NotExpression notExpr)
	{
		invalid = true;
		super.visit(notExpr);
	}

	@Override
	public void visit(JsonExpression jsonExpr)
	{
		invalid = true;
		super.visit(jsonExpr);
	}

	@Override
	public void visit(JsonOperator expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(RegExpMySQLOperator expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(WithinGroupExpression wgexpr)
	{
		invalid = true;
		super.visit(wgexpr);
	}

	@Override
	public void visit(UserVariable var)
	{
		invalid = true;
		super.visit(var);
	}

	@Override
	public void visit(NumericBind bind)
	{
		invalid = true;
		super.visit(bind);
	}

	@Override
	public void visit(KeepExpression expr)
	{
		invalid = true;
		super.visit(expr);
	}

	@Override
	public void visit(MySQLGroupConcat groupConcat)
	{
		invalid = true;
		super.visit(groupConcat);
	}

	@Override
	public void visit(Pivot pivot)
	{
		invalid = true;
		super.visit(pivot);
	}

	@Override
	public void visit(PivotXml pivot)
	{
		invalid = true;
		super.visit(pivot);
	}

	@Override
	public void visit(AllColumns allColumns)
	{
		invalid = true;
		super.visit(allColumns);
	}

	@Override
	public void visit(AllTableColumns allTableColumns)
	{
		invalid = true;
		super.visit(allTableColumns);
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem)
	{
		invalid = true;
		super.visit(selectExpressionItem);
	}

	@Override
	public void visit(RowConstructor rowConstructor)
	{
		invalid = true;
		super.visit(rowConstructor);
	}

	@Override
	public void visit(OracleHint hint)
	{
		invalid = true;
		super.visit(hint);
	}

	@Override
	public void visit(TimeKeyExpression timeKeyExpression)
	{
		invalid = true;
		super.visit(timeKeyExpression);
	}

	@Override
	public void visit(DateTimeLiteralExpression literal)
	{
		invalid = true;
		super.visit(literal);
	}

	protected boolean isInvalid()
	{
		return invalid;
	}

}
