package logicaloperators;

import java.io.PrintStream;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;
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


	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		List<OrderByElement> orders = plainSelect.getOrderByElements();
		return String.format("Sort%s", 
				((orders == null) ? "[null]" : orders.toString()));
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		child.printTree(ps, lv + 1);
	}

	
}