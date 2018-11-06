package logicaloperators;

import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.PhysicalPlanBuilder;

public class LogicalProjectOperator extends LogicalOperator {
	
	LogicalOperator child;
	public PlainSelect plainSelect;
	
	public LogicalProjectOperator(LogicalOperator child, PlainSelect plainSelect) {
		this.child = child;
		this.plainSelect = plainSelect;
	}
	@Override
	public void accept(PhysicalPlanBuilder ppb) {

		ppb.visit(this);
	}
	public LogicalOperator getChild() {
		return child;
	}
	

}
