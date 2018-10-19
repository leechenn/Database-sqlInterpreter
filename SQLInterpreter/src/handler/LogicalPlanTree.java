package handler;

import net.sf.jsqlparser.statement.select.FromItem;
import operator.Operator;

public class LogicalPlanTree {
	
	LogicalPlanTreeNode root;
	
	public LogicalPlanTree(Operator o, FromItem f) { 
        root = new LogicalPlanTreeNode(o, f);
    } 
  
    public LogicalPlanTree() { 
        root = null; 
    }

}
