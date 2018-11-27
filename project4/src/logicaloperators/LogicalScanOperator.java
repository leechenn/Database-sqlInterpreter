package logicaloperators;


import java.io.PrintStream;

import net.sf.jsqlparser.statement.select.PlainSelect;
import util.PhysicalPlanBuilder;

public class LogicalScanOperator extends LogicalOperator {
	
	public PlainSelect plainSelect;
	public boolean joinedTable = false;
	public int joinedTableIndex;
	
	public LogicalScanOperator(PlainSelect plainSelect) {		
		this.plainSelect = plainSelect;
	}
	public LogicalScanOperator(PlainSelect plainSelect,int joinedTableIndex) {		
		this.plainSelect = plainSelect;
		this.joinedTable = true;
		this.joinedTableIndex = joinedTableIndex;
	}
	
	@Override
	public void accept(PhysicalPlanBuilder ppb) {
		ppb.visit(this);
	}
	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
		String tableScanned = null;
		String aliasName = null;
		if(!this.joinedTable) {	
		tableScanned = plainSelect.getFromItem().toString();// first table in from item	
		String[] strs = tableScanned.split("\\s+");//if there is aliases
		tableScanned = strs[0];
		aliasName = strs[strs.length-1];
		}else {
			tableScanned = null;
			tableScanned = plainSelect.getJoins().get(joinedTableIndex).toString();
			String[] strs = tableScanned.split("\\s+");//if there is aliases
			tableScanned = strs[0];
			aliasName = strs[strs.length-1];
		}
		return String.format("Leaf[%s]", tableScanned);
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
	}

}
