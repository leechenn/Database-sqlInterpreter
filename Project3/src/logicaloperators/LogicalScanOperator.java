package logicaloperators;


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

}
