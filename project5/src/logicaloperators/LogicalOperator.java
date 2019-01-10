package logicaloperators;

import java.io.PrintStream;

import util.PhysicalPlanBuilder;

public abstract class LogicalOperator {
	
	public abstract void accept(PhysicalPlanBuilder ppb);
	public abstract String printLogicalTree();
	public void printTree(PrintStream ps, int lv) {
		// TODO Auto-generated method stub
		
		
	}
	protected static void printIndent(PrintStream ps, int lv) {
		while (lv-- > 0)
			ps.print('-');
	}
}
