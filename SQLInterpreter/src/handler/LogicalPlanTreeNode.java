package handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.Operator;

public class LogicalPlanTreeNode {
	
	public LogicalPlanTreeNode root;
	public LogicalPlanTreeNode leftNode, rightNode;
	public String columnName;
	public String alias;
	
	public Operator op;
	private FromItem item;
	
	public LogicalPlanTreeNode(Operator o, FromItem f) {
		root = this;
		leftNode = null;
		rightNode = null;
		columnName = null;
		alias = null;
		FromItem item = f;
		op = o;
	}
	
	public LogicalPlanTreeNode() {
		leftNode = rightNode = null;
	}
	
	
	public LogicalPlanTree initTree(PlainSelect ps) {
		HashMap<String, FromItem> aliasTable = new HashMap<String, FromItem>();
		FromItem firstRelation = ps.getFromItem();
		String alias = firstRelation.getAlias();
		aliasTable.put(alias, firstRelation);
		List<Join> relations = ps.getJoins();
		for (int i = 0; i < relations.size(); i++) {
			FromItem x = relations.get(i).getRightItem();
			if (x.getAlias() != null) {
				aliasTable.put(x.getAlias(), x);
			}
			
			if (this.leftNode == null) {
				this.leftNode = new LogicalPlanTree();
			}
			else if (this.rightNode == null) {
				this.rightNode = new LogicalPlanTree();
			}
			LogicalPlanTree result = new LogicalPlanTree();
			
		}
		
		
		
		return root;
			
		}
		
		
//		List order = ps.getOrderByElements();
//		
//		
//		
//		
//		List<> joins = ps.getJoins();
//		String[] strs = firstTable.split("\\s+");//if there is aliases
//		String tableName = strs[0];
//		String aliasName = strs[strs.length-1];
//		return node;
//	}
//	
	public LogicalPlanTree buildTree() {
		return null;
	}
	
	public static void main (String[] args) {
		LogicalPlanTree x = new LogicalPlanTree();
		String sql = "select t1.a, t2.b from t1, t2 where t1.id = t2.id";
//		Statement parse = 
	}
}
