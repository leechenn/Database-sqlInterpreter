package logicaloperators;

import java.io.PrintStream;

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
	public String printLogicalTree()  {
		String expression = (expr != null) ? expr.toString() : "";
        return String.format("Join[" + expression + "]");
	};
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		left.printTree(ps, lv + 1);
		right.printTree(ps, lv + 1);
	}

}