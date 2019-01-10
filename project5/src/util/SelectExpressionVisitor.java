package util;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import javax.management.RuntimeErrorException;

import entity.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * @author Chen Li, QinXuan Pian
 * SelectExpressionVisitor for filting tuple according to expression
 */

public class SelectExpressionVisitor implements ExpressionVisitor {

	private Map<String, Integer> currentSchema;
	private Deque<Long> data;
	private Tuple currentTuple;
	private boolean result = true;


	public SelectExpressionVisitor(Tuple tuple, Map<String, Integer> schema) {

		currentSchema = schema;
		data = new LinkedList<>();
		currentTuple = tuple;

	}

	/**
	 * @return result of the expression
	 */
	public boolean getResult() {

		return this.result;
	}

	@Override
	public void visit(AndExpression andExpression) {

		andExpression.getLeftExpression().accept(this);
		andExpression.getRightExpression().accept(this);

	}

	@Override
	public void visit(Column column) {

		String columnName = column.getWholeColumnName();
		try {
			int ind = currentSchema.get(columnName);

			data.push((long)currentTuple.getData()[ind]);
		}catch(NullPointerException ex) {
			System.out.println("yes");
			System.out.println(currentSchema);
			System.out.println(columnName);
		}

	}

	@Override
	public void visit(LongValue longValue) {
		data.push(longValue.getValue());
	}

	@Override
	public void visit(EqualsTo equalsTo) {

		equalsTo.getLeftExpression().accept(this);
		equalsTo.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue == rightValue);

	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {

		notEqualsTo.getLeftExpression().accept(this);
		notEqualsTo.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue != rightValue);

	}

	@Override
	public void visit(GreaterThan greaterThan) {

		greaterThan.getLeftExpression().accept(this);
		greaterThan.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue > rightValue);

	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {

		greaterThanEquals.getLeftExpression().accept(this);
		greaterThanEquals.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue >= rightValue);


	}

	@Override
	public void visit(MinorThan minorThan) {

		minorThan.getLeftExpression().accept(this);
		minorThan.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue < rightValue);

	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {

		minorThanEquals.getLeftExpression().accept(this);
		minorThanEquals.getRightExpression().accept(this);
		long rightValue = data.removeFirst();
		long leftValue = data.removeFirst();
		this.result = result&&(leftValue <= rightValue);

	}

	@Override
	public void visit(NullValue nullValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function function) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression inverseExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue doubleValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue dateValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue timeValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue timestampValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis parenthesis) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue stringValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(OrExpression orExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between between) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InExpression inExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression likeExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub

	}
}
