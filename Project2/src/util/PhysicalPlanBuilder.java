package util;

import java.util.ArrayList;
import java.util.List;

import handler.App;
import logicaloperators.LogicalDuplicateEliminationOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalProjectOperator;
import logicaloperators.LogicalScanOperator;
import logicaloperators.LogicalSelectOperator;
import logicaloperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import operator.BlockNestedJoinOperator;
import operator.DuplicateEliminationOperator;
import operator.ExternalSortOperator;
//import operator.ExternalSortOperator;
import operator.TupleNestedLoopJoinOperator;
import operator.Operator;
import operator.ProjectOperator;
import operator.ScanOperator;
import operator.SelectOperator;
import operator.SortMergeJoinOperator;
//import operator.SortMergeJoinOperator;
import operator.InMemorySortOperator;

/**
 * PhysicalPlanBuilder for building physical plan from logical operator
 * @author Chen Li, QinXuan Pian
 *
 */
public class PhysicalPlanBuilder {
	private Operator op;
	private List<String> leftOderList = new ArrayList<String>();
	private List<String> rightOrderList = new ArrayList<String>();
	public void visit(LogicalSelectOperator logicalSelectOperator) {
		// TODO Auto-generated method stub
		op = null;
		logicalSelectOperator.getChild().accept(this);
		op.getSchema();
		op = new SelectOperator((ScanOperator)op,logicalSelectOperator.plainSelect,logicalSelectOperator.getExpr());
	}

	public void visit(LogicalProjectOperator logicalProjectOperator) {
		// TODO Auto-generated method stub
		op = null;
		logicalProjectOperator.getChild().accept(this);
		op = new ProjectOperator(op,logicalProjectOperator.plainSelect);
	}

	public void visit(LogicalScanOperator logicalScanOperator) {
		// TODO Auto-generated method stub
		if (logicalScanOperator.joinedTable) {
			op = new ScanOperator(logicalScanOperator.plainSelect,logicalScanOperator.joinedTableIndex);
		}
		else {
			op = new ScanOperator(logicalScanOperator.plainSelect);
		}
	}


	public void visit(LogicalDuplicateEliminationOperator logicalDuplicateEliminationOperator) {
		// TODO Auto-generated method stub
		op = null;
		logicalDuplicateEliminationOperator.getChild().accept(this);
		op = new DuplicateEliminationOperator(op);

	}

	public void visit(LogicalSortOperator logicalSortOperator) {
		// TODO Auto-generated method stub
		op = null;
		logicalSortOperator.getChild().accept(this);
		op = new InMemorySortOperator(op,logicalSortOperator.plainSelect);
		switch (App.model.sortConfig) {
		case 0:
			op = new InMemorySortOperator(op,logicalSortOperator.plainSelect);
			break;
		case 1:
			op = new ExternalSortOperator(op,logicalSortOperator.plainSelect,App.model.temDir,App.model.sortBuffer);
			break;

		default:

		}

	}
	public void visit(LogicalJoinOperator logicalJoinOperator) {

		pair p = new pair();
		op = null;
		logicalJoinOperator.getLeft().accept(this);
		p.left = op;
		op = null;
		logicalJoinOperator.getRight().accept(this);
		p.right = op;
		switch (App.model.joinConfig) {
		case 0:
			op = new TupleNestedLoopJoinOperator(p.left,p.right,logicalJoinOperator.plainSelect,logicalJoinOperator.getExpr());

			break;
		case 1:
			op = new BlockNestedJoinOperator(p.left, p.right, logicalJoinOperator.getExpr());
			break;
		case 2:
			Expression expression = logicalJoinOperator.getExpr();
			this.parseOrder(expression);
			if( App.model.sortConfig == 0 ) {
				p.left = new InMemorySortOperator(p.left,logicalJoinOperator.plainSelect,this.leftOderList);
				p.right = new InMemorySortOperator(p.right,logicalJoinOperator.plainSelect,this.rightOrderList);
			} else {
				p.left = new ExternalSortOperator(p.left,logicalJoinOperator.plainSelect,App.model.temDir,App.model.sortBuffer,this.leftOderList);
				p.right = new ExternalSortOperator(p.right,logicalJoinOperator.plainSelect,App.model.temDir,App.model.sortBuffer,this.rightOrderList);
			}
			op = new SortMergeJoinOperator(p.left,p.right,logicalJoinOperator.plainSelect,logicalJoinOperator.getExpr(), this.leftOderList,this.rightOrderList);
			this.leftOderList = new ArrayList<String>();
			this.rightOrderList = new ArrayList<String>();
			break;


		default:

		}
	}
	/**
	 * Assign orderList to the left child and right child of SMJ, assuming all queries used to test SMJ will contain equijoins only
	 * @param expression
	 */
	public void parseOrder(Expression expression) {

		if(expression instanceof EqualsTo) {
			this.leftOderList.add(((EqualsTo) expression).getLeftExpression().toString());
			this.rightOrderList.add(((EqualsTo) expression).getRightExpression().toString());
		}else {
			if(expression instanceof AndExpression) {
				parseOrder(((AndExpression) expression).getLeftExpression());
				parseOrder(((AndExpression) expression).getRightExpression());
			}
		}
	}

	public Operator getOp() {
		return op;
	}
	class pair {
		Operator left;
		Operator right;

	}


}
