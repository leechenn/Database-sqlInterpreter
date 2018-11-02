package logicaloperators;

import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import util.PhysicalPlanBuilder;

public class LogicalSortOperator extends LogicalOperator {
	
	LogicalOperator child;
	public PlainSelect plainSelect;

	public LogicalSortOperator(LogicalOperator child, PlainSelect plainSelect) {
		// TODO Auto-generated constructor stub
		this.child = child;
		this.plainSelect = plainSelect;
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