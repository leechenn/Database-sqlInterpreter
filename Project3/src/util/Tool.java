package util;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

public class Tool {
	private List<Expression> indexKeyExpList = new ArrayList<Expression>();
	private List<Expression> otherExp = new ArrayList<Expression>();
	private Expression otherExpCombined;
	private boolean canUseIndex;
	private Integer lowKey;
	private Integer highKey;
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

	//a table's attribute in expression can be used as an index key search
	public void retrieveIdxAttr(String tableName, Expression selectExp) {
		String[] indexInfo = App.model.getIndexInfoMap().get(tableName);
		if(selectExp==null||indexInfo==null) {
			return;	
		}
		//split expression	
		List<Expression> expressionList = new ArrayList<Expression>();
		while(selectExp instanceof AndExpression) {
			AndExpression andExp = (AndExpression) selectExp;
			expressionList.add(andExp.getRightExpression());
			selectExp = andExp.getLeftExpression();	
		}
		expressionList.add(selectExp);

		for (Expression expr : expressionList) {
			System.out.println("expression:"+expr);
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
					if (str.equals(indexInfo[1])) {
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
		if(this.canUseIndex) {
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
					return;
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
}
