package logicaloperators;

import java.io.PrintStream;

import util.PhysicalPlanBuilder;

public class LogicalDuplicateEliminationOperator extends LogicalOperator {
	
	LogicalOperator child;
	
	public LogicalDuplicateEliminationOperator(LogicalOperator child) {
		// TODO Auto-generated constructor stub
		this.child = child;
	}
	@Override
	public void accept(PhysicalPlanBuilder ppb) {
		// TODO Auto-generated method stub
		ppb.visit(this);
	}
	public LogicalOperator getChild() {
		return child;
	}
	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		return "DupElim";
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		child.printTree(ps, lv + 1);
	}
}
