package logicaloperators;

import util.PhysicalPlanBuilder;

public abstract class LogicalOperator {
	
	public abstract void accept(PhysicalPlanBuilder ppb);
}
