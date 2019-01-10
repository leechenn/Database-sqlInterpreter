package util;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class Tool {

	private List<Expression> indexKeyExpList = new ArrayList<Expression>();
	private List<Expression> otherExp = new ArrayList<Expression>();
	private Expression otherExpCombined;
	private boolean canUseIndex;
	private Integer lowKey;
	private Integer highKey;
	private IndexInfo indexUsed;


	/**
	 * @param tableFile
	 * @param index
	 * If Btree is clustered, the table need to be sorted by the index key
	 */
	public static void sortTableByIndex(File tableFile, int index) {

		List<Tuple> tupleList = new ArrayList<Tuple>();
		//	System.out.println(tableFile);
		TupleReader tr;
		try {
			tr = new TupleReader(tableFile);
			Tuple tuple = tr.readNext();
			while(tuple!=null) {
				tupleList.add(tuple);
				tuple = tr.readNext();
			}
			tr.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Collections.sort(tupleList, new Comparator<Tuple>(){
			@Override
			public int compare(Tuple t1, Tuple t2) {

				if (t1.getData()[index] > t2.getData()[index]) {
					return 1;
				}
				if (t1.getData()[index] < t2.getData()[index]) {
					return -1;
				}
				return 0;
			}


		});
		//	System.out.println(tupleList);
		TupleWriter tw = new TupleWriter(tableFile.toString());
		for(Tuple t:tupleList) {
			try {
				tw.writeTuple(t);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		tw.close();
	}
	public static Expression createCondition(String tab, String col, 
			int val, boolean isEq, boolean isGE) {
		Table t = new Table(null, tab);
		Column c = new Column(t, col);
		LongValue v = new LongValue(String.valueOf(val));

		if (isEq)
			return new EqualsTo(c, v);
		if (isGE)
			return new GreaterThanEquals(c, v);
		return new MinorThanEquals(c, v);
	}

	public static Expression genAnds(List<Expression> exps) {
		if (exps.isEmpty()) return null;
		Expression ret = exps.get(0);
		for (int i = 1; i < exps.size(); i++) {
			if (exps.get(i) != null) {
				ret = new AndExpression(ret, exps.get(i));
			}
		}

		return ret;
	}




	/**
	 * @param tableName
	 * @param selectExp
	 * verify if the index can be used in selection condition. if it could, split the selection condition into two portion,
	 * one portion for index and the other portion(reminder) for SelectOperator to deal with.
	 */
	public void retrieveIdxAttr(String tableName, Expression selectExp) {
		//select index
		IndexInfo indexInfoObj = SelectOptimizer.whichIndexToUse(tableName, selectExp);
		//		System.out.println("whichIndex:"+indexInfoObj);
		//		String[] indexInfo = App.model.getIndexInfoMap().get(tableName);
		//		System.out.println("indexInfo:"+Arrays.toString(indexInfo));
		if(selectExp==null||indexInfoObj==null) {
			return;	
		}else {
			this.indexUsed = indexInfoObj;
		}
		//split expression	
		List<Expression> expressionList = new ArrayList<Expression>();
		while(selectExp instanceof AndExpression) {
			AndExpression andExp = (AndExpression) selectExp;
			expressionList.add(andExp.getRightExpression());
			selectExp = andExp.getLeftExpression();	
		}
		expressionList.add(selectExp);
		//		System.out.println("expressionList:"+expressionList);
		for (Expression expr : expressionList) {
			//			System.out.println("expression:"+expr);
			Expression left = 
					((BinaryExpression) expr).getLeftExpression();
			Expression right = 
					((BinaryExpression) expr).getRightExpression();

			String str = null;

			if ((left instanceof Column && right instanceof LongValue) || (left instanceof LongValue && right instanceof Column)) {
				if (   expr instanceof EqualsTo || expr instanceof GreaterThan || expr instanceof GreaterThanEquals || expr instanceof MinorThan || expr instanceof MinorThanEquals ) {
					//                  System.out.println("expression:"+expr.toString());
					str = (left instanceof Column) ? left.toString() : 
						right.toString();
					if (str.indexOf('.') != -1) {
						str = str.split("\\.")[1];
					}
					//a table's attribute in expression can be used as an index key search
					//					System.out.println("str:"+str);
					//					System.out.println("Indexstr:"+indexInfoObj.attr);
					if (str.equals(indexInfoObj.attr)) {
						this.indexKeyExpList.add(expr);
						this.canUseIndex = true;
					}else {
						this.otherExp.add(expr);
					}
				}

			}else {
				this.otherExp.add(expr);
			}


		}
		//		System.out.println("otherExp in Tool"+otherExp);
		if(this.canUseIndex) {
			//if the index in index file could be used, find low key and high key for index scan search
			for(Expression exp:this.indexKeyExpList) {
				if(exp instanceof EqualsTo) {
					if(((EqualsTo) exp).getLeftExpression() instanceof LongValue) {
						LongValue lv = (LongValue) ((EqualsTo) exp).getLeftExpression();
						this.lowKey = (int) lv.getValue();
						this.highKey = (int) lv.getValue();
					}else {
						LongValue lv = (LongValue) ((EqualsTo) exp).getRightExpression();
						this.lowKey = (int) lv.getValue();
						this.highKey = (int) lv.getValue();
					}
					break;
				}
				if(exp instanceof GreaterThan) {
					if(((GreaterThan) exp).getLeftExpression() instanceof LongValue) {
						LongValue lv = (LongValue) ((GreaterThan) exp).getLeftExpression();
						if(this.highKey!=null) {
							this.highKey = Math.min(highKey, (int) lv.getValue()-1);
						}else {
							this.highKey = (int) lv.getValue()-1;
						}
					}else {
						LongValue lv = (LongValue) ((GreaterThan) exp).getRightExpression();
						if(this.lowKey!=null) {
							this.lowKey = Math.max(lowKey, (int) lv.getValue()+1);
						}else {

							this.lowKey = (int) lv.getValue()+1;
						}

					}
				}
				if(exp instanceof GreaterThanEquals) {
					if(((GreaterThanEquals) exp).getLeftExpression() instanceof LongValue) {
						LongValue lv = (LongValue) ((GreaterThanEquals) exp).getLeftExpression();
						if(this.highKey!=null) {
							this.highKey = Math.min(highKey, (int) lv.getValue());
						}else {
							this.highKey = (int) lv.getValue();
						}
					}else {
						LongValue lv = (LongValue) ((GreaterThanEquals) exp).getRightExpression();
						if(this.lowKey!=null) {
							this.lowKey = Math.max(lowKey, (int) lv.getValue());
						}else {

							this.lowKey = (int) lv.getValue();
						}

					}
				}
				if(exp instanceof MinorThanEquals) {
					if(((MinorThanEquals) exp).getRightExpression() instanceof LongValue) {
						LongValue lv = (LongValue) ((MinorThanEquals) exp).getRightExpression();
						if(this.highKey!=null) {
							this.highKey = Math.min(highKey, (int) lv.getValue());
						}else {
							this.highKey = (int) lv.getValue();
						}
					}else {
						LongValue lv = (LongValue) ((MinorThanEquals) exp).getLeftExpression();
						if(this.lowKey!=null) {
							this.lowKey = Math.max(lowKey, (int) lv.getValue());
						}else {

							this.lowKey = (int) lv.getValue();
						}

					}
				}
				if(exp instanceof MinorThan) {
					if(((MinorThan) exp).getRightExpression() instanceof LongValue) {
						LongValue lv = (LongValue) ((MinorThan) exp).getRightExpression();
						if(this.highKey!=null) {
							this.highKey = Math.min(highKey, (int) lv.getValue()-1);
						}else {
							this.highKey = (int) lv.getValue()-1;
						}
					}else {
						LongValue lv = (LongValue) ((MinorThan) exp).getLeftExpression();
						if(this.lowKey!=null) {
							this.lowKey = Math.max(lowKey, (int) lv.getValue()+1);
						}else {

							this.lowKey = (int) lv.getValue()+1;
						}

					}
				}


			}
		}
		if(this.otherExp.size()!=0) {
			otherExpCombined = this.otherExp.get(0);
			for (int i = 1; i < this.otherExp.size(); i++) {
				otherExpCombined = new AndExpression(otherExpCombined, this.otherExp.get(i));
			}

		}
		//		System.out.println("otherExpCombined:"+otherExpCombined);

	}
	public IndexInfo getIndexUsed() {
		return this.indexUsed;
	}
	public Integer getLowKey() {
		return this.lowKey;
	}
	public Integer getHighKey() {
		return this.highKey;
	}
	public boolean canUseIndex() {
		return this.canUseIndex;
	}
	public Expression getOtherExp() {
		return this.otherExpCombined;
	}
	private static void updateRange(Integer[] range, int val, 
			boolean isLower, boolean inclusive, boolean oppo) {
		if (oppo) {
			updateRange(range, val, !isLower, inclusive, false);
			return;
		}

		if (!inclusive)
			val = (isLower) ? val + 1 : val - 1;

		if (isLower)
			range[0] = (range[0] == null) ? val : 
				Math.max(range[0], val);
		else
			range[1] = (range[1] == null) ? val :
				Math.min(range[1], val);
	}
	/**
	 * 
	 * @param exp
	 * @param attr
	 * @return int[]rst  rst[0] max, rst[1] min
	 */
	public static Integer[] getSelRange(Expression exp, String[] attr) {

		Expression left = 
				((BinaryExpression) exp).getLeftExpression();
		Expression right = 
				((BinaryExpression) exp).getRightExpression();

		Integer val = null;


		if (left instanceof Column) {
			attr[0] = left.toString();
			val = Integer.parseInt(right.toString());
		}
		else {
			attr[0] = right.toString();
			val = Integer.parseInt(left.toString());
		}

		boolean oppo = !(left instanceof Column);
		boolean inclusive = !(exp instanceof MinorThan) && 
				!(exp instanceof GreaterThan);
		boolean isUpper = (exp instanceof MinorThan ||
				exp instanceof MinorThanEquals || 
				exp instanceof EqualsTo);
		boolean isLower = (exp instanceof GreaterThan ||
				exp instanceof GreaterThanEquals || 
				exp instanceof EqualsTo);

		if (!isLower && !isUpper)
			throw new IllegalArgumentException();

		Integer[] ret = new Integer[2];

		if (isLower)
			updateRange(ret, val, true, inclusive, oppo);
		if (isUpper)
			updateRange(ret, val, false, inclusive, oppo);

		return ret;
	}
	public static List<Expression> decompAnds(Expression exp) {
		List<Expression> ret = new ArrayList<Expression>();
		while (exp instanceof AndExpression) {
			AndExpression and = (AndExpression) exp;
			ret.add(and.getRightExpression());
			exp = and.getLeftExpression();
		}
		ret.add(exp);

		Collections.reverse(ret);
		return ret;
	}

}
