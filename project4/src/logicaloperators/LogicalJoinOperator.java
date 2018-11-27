package logicaloperators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.PhysicalPlanBuilder;

public class LogicalJoinOperator extends LogicalOperator {
	
	LogicalOperator left;
	LogicalOperator right;
	Expression expr;
	public PlainSelect plainSelect;
	
	public LogicalJoinOperator(LogicalOperator left, LogicalOperator right,PlainSelect plainSelect, Expression expr) {
		// TODO Auto-generated constructor stub
		this.left = left;
		this.right = right;
		this.expr = expr;
		this.plainSelect = plainSelect;
	}
	@Override
	public void accept(PhysicalPlanBuilder ppb) {
		// TODO Auto-generated method stub
		ppb.visit(this);
	}
	
	public LogicalOperator getLeft() {
		return left;
	}

	public LogicalOperator getRight() {
		return right;
	}

	public Expression getExpr() {
		return expr;
	}
	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		return null;
	}

}