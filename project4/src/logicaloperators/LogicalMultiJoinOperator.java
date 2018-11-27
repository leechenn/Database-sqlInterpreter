package logicaloperators;

import java.io.PrintStream;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import util.Element;
import util.PhysicalPlanBuilder;
import util.UnionFind;

public class LogicalMultiJoinOperator extends LogicalOperator {
private Expression residualJoinCon;
private UnionFind uf;
public List<String> tableList;
private List<LogicalOperator> children;

public LogicalMultiJoinOperator(List<String> tableList,List<LogicalOperator> children,Expression res,UnionFind uf) {
	this.tableList = tableList;
	this.children = children;
	this.residualJoinCon = res;
	this.uf = uf;
}
	@Override
	public void accept(PhysicalPlanBuilder ppb) {
		// TODO Auto-generated method stub
		ppb.visit(this);
	}
	@Override
	public String printLogicalTree() {
		// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("Join");
			if (residualJoinCon != null)
				sb.append(String.format("[%s]", residualJoinCon.toString()));
			else
				sb.append("[]");
			for (Element element : uf.getElementList())
				sb.append('\n' + element.elementInfo());
			
			return sb.toString();
		
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printLogicalTree());
		for (LogicalOperator lop : children)
			lop.printTree(ps, lv + 1);
	}
	

}
