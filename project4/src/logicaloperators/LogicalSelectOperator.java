package logicaloperators;

import java.io.PrintStream;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.PhysicalPlanBuilder;

public class LogicalSelectOperator extends LogicalOperator{
	LogicalOperator child;
	Expression expr;
	public PlainSelect plainSelect;
	
	public LogicalSelectOperator(LogicalOperator child,PlainSelect plainSelect, Expression expr) {

		this.child = child;
		this.expr = expr;
		this.plainSelect = plainSelect;
	}
	
	public LogicalOperator getChild() {
		return child;
	}
	
	public Expression getExpr() {
		return expr;
	}
	
	@Override
	public void accept(PhysicalPlanBuilder ppb) {

		ppb.visit(this);
	}

	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		return String.format("Select[%s]", 
				((expr == null) ? "null" : expr.toString()));

	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		child.printTree(ps, lv + 1);
	}

}