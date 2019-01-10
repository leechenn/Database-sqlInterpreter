package logicaloperators;

import java.io.PrintStream;
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
	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		List<SelectItem> itemList = plainSelect.getSelectItems();
		return String.format("Project%s", 
				((itemList == null) ? "[null]" : itemList.toString()));
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		child.printTree(ps, lv + 1);
	}
	

}
