package logicaloperators;

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
}
