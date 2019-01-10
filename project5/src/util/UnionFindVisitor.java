
package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * create union find according to expression
 * @author Chen Li, QinXuan Pian
 *
 */
public class UnionFindVisitor implements ExpressionVisitor{

	//public static void main(String[] args) {
	//	UnionFindVisitor ufVisitor = new UnionFindVisitor();
	//	
	//}

	private UnionFind uf = new UnionFind();
	private List<Expression> residualExp = new ArrayList<Expression>();
	private List<String> tableList = new ArrayList<String>();
	private HashMap<String,List<Expression>> selectCondition = new HashMap<String,List<Expression>>();//push selection
	private List<Expression> residualJoinExp = new ArrayList<Expression>();


	public UnionFindVisitor(List<String> tableList) {
		this.tableList = tableList;
		for(String table:tableList) {
			this.selectCondition.put(table, new ArrayList<Expression>());
		}
	}
	public HashMap<String,List<Expression>> getSelectionMap(){
		return this.selectCondition;
	}
	public UnionFind getUf() {
		return this.uf;
	}
	public List<Expression> getResidualExp(){
		return this.residualExp;
	}
	public List<Expression> getResidualJoinExp(){
		return this.residualJoinExp;
	}
	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AndExpression andExpression) {
		//		  do a recursive method for dealing with expression node
		andExpression.getLeftExpression().accept(this);
		andExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		// TODO Auto-generated method stub
		if(equalsTo.getLeftExpression() instanceof Column && equalsTo.getRightExpression() instanceof Column) {
			Column cL = (Column) equalsTo.getLeftExpression();
			Column cR = (Column) equalsTo.getRightExpression();
			if(!cL.toString().equals(cR.toString())){
				Element elementL = uf.findElement(cL.toString());
				Element elementR = uf.findElement(cR.toString());
				uf.join(elementL, elementR);
				//				System.out.println(uf.getUnionMap());
			}
			if(cL.getTable().getWholeTableName().equals(cR.getTable().getWholeTableName())) {
				this.selectCondition.get(cL.getTable().getWholeTableName()).add(equalsTo);//self compare
			}
		}
		else {
			if(equalsTo.getLeftExpression() instanceof Column) {
				Column cL = (Column) equalsTo.getLeftExpression();
				Element element = uf.findElement(cL.toString());
				//				System.out.println("element in equal:"+element);
				LongValue value = (LongValue) equalsTo.getRightExpression();
				//				System.out.println("value:"+value.getValue());
				element.setEquality((int) value.getValue());
			}else {
				Column cR = (Column) equalsTo.getRightExpression();
				Element element = uf.findElement(cR.toString());
				LongValue value = (LongValue) equalsTo.getLeftExpression();
				element.setEquality((int) value.getValue());
			}
		}

	}

	@Override
	public void visit(GreaterThan greaterThan) {
		if(greaterThan.getLeftExpression() instanceof Column && greaterThan.getRightExpression() instanceof LongValue) {
			Column cL = (Column) greaterThan.getLeftExpression();
			Element element = uf.findElement(cL.toString());
			LongValue value = (LongValue) greaterThan.getRightExpression();
			element.setLowerB((int) value.getValue()+1);			

		}else {
			if(greaterThan.getLeftExpression() instanceof LongValue && greaterThan.getRightExpression() instanceof Column) {
				Column cR = (Column) greaterThan.getRightExpression();
				Element element = uf.findElement(cR.toString());
				LongValue value = (LongValue) greaterThan.getLeftExpression();
				element.setUpperB((int) value.getValue()-1);
			}else {
				this.residualExp.add(greaterThan);
				Column cL = (Column) greaterThan.getLeftExpression();
				Column cR = (Column) greaterThan.getRightExpression();
				String leftTab = cL.getTable().getWholeTableName();
				String rightTab = cR.getTable().getWholeTableName();
				if(leftTab.equals(rightTab)) {
					this.selectCondition.get(cL.getTable().getWholeTableName()).add(greaterThan);//self compare
				}else {
					this.residualJoinExp.add(greaterThan);
				}

			}
		}

	}

	@Override
	public void visit(GreaterThanEquals greaterThanEqual) {
		if(greaterThanEqual.getLeftExpression() instanceof Column && greaterThanEqual.getRightExpression() instanceof LongValue) {
			Column cL = (Column) greaterThanEqual.getLeftExpression();
			Element element = uf.findElement(cL.toString());
			LongValue value = (LongValue) greaterThanEqual.getRightExpression();
			element.setLowerB((int) value.getValue());
		}else {
			if(greaterThanEqual.getLeftExpression() instanceof LongValue && greaterThanEqual.getRightExpression() instanceof Column) {
				Column cR = (Column) greaterThanEqual.getRightExpression();
				Element element = uf.findElement(cR.toString());
				LongValue value = (LongValue) greaterThanEqual.getLeftExpression();
				element.setUpperB((int) value.getValue());
			}else {
				this.residualExp.add(greaterThanEqual);
				Column cL = (Column) greaterThanEqual.getLeftExpression();
				Column cR = (Column) greaterThanEqual.getRightExpression();
				String leftTab = cL.getTable().getWholeTableName();
				String rightTab = cR.getTable().getWholeTableName();
				if(leftTab.equals(rightTab)) {
					this.selectCondition.get(cL.getTable().getWholeTableName()).add(greaterThanEqual);//self compare
				}else {
					this.residualJoinExp.add(greaterThanEqual);
				}
			}
		}
	}

	@Override
	public void visit(MinorThan minorThan) {
		if(minorThan.getLeftExpression() instanceof Column && minorThan.getRightExpression() instanceof LongValue) {
			Column cL = (Column) minorThan.getLeftExpression();
			Element element = uf.findElement(cL.toString());
			LongValue value = (LongValue) minorThan.getRightExpression();
			element.setUpperB((int) value.getValue()-1);
		}else {
			if(minorThan.getLeftExpression() instanceof LongValue && minorThan.getRightExpression() instanceof Column) {
				Column cR = (Column) minorThan.getRightExpression();
				Element element = uf.findElement(cR.toString());
				LongValue value = (LongValue) minorThan.getLeftExpression();
				element.setLowerB((int) value.getValue()+1);
			}else {
				this.residualExp.add(minorThan);
				Column cL = (Column) minorThan.getLeftExpression();
				Column cR = (Column) minorThan.getRightExpression();
				String leftTab = cL.getTable().getWholeTableName();
				String rightTab = cR.getTable().getWholeTableName();
				if(leftTab.equals(rightTab)) {
					this.selectCondition.get(cL.getTable().getWholeTableName()).add(minorThan);//self compare
				}else {
					this.residualJoinExp.add(minorThan);
				}

			}
		}

	}

	@Override
	public void visit(MinorThanEquals minorThanEqual) {
		if(minorThanEqual.getLeftExpression() instanceof Column && minorThanEqual.getRightExpression() instanceof LongValue) {
			Column cL = (Column) minorThanEqual.getLeftExpression();
			Element element = uf.findElement(cL.toString());
			LongValue value = (LongValue) minorThanEqual.getRightExpression();
			element.setUpperB((int) value.getValue());
		}else {
			if(minorThanEqual.getLeftExpression() instanceof LongValue && minorThanEqual.getRightExpression() instanceof Column) {
				Column cR = (Column) minorThanEqual.getRightExpression();
				Element element = uf.findElement(cR.toString());
				LongValue value = (LongValue) minorThanEqual.getLeftExpression();
				element.setLowerB((int) value.getValue());
			}else {
				this.residualExp.add(minorThanEqual);
				Column cL = (Column) minorThanEqual.getLeftExpression();
				Column cR = (Column) minorThanEqual.getRightExpression();
				String leftTab = cL.getTable().getWholeTableName();
				String rightTab = cR.getTable().getWholeTableName();
				if(leftTab.equals(rightTab)) {
					this.selectCondition.get(cL.getTable().getWholeTableName()).add(minorThanEqual);//self compare
				}else {
					this.residualJoinExp.add(minorThanEqual);
				}
			}
		}


	}
	//residual to individual table
	@Override
	public void visit(NotEqualsTo notEqualTo) {
		this.residualExp.add(notEqualTo);
		if(notEqualTo.getLeftExpression() instanceof LongValue) {
			Column cR = (Column) notEqualTo.getRightExpression();
			this.selectCondition.get(cR.getTable().getWholeTableName()).add(notEqualTo);
		}
		if(notEqualTo.getRightExpression() instanceof LongValue) {
			Column cL = (Column) notEqualTo.getLeftExpression();
			this.selectCondition.get(cL.getTable().getWholeTableName()).add(notEqualTo);
		}
		if(notEqualTo.getRightExpression() instanceof Column&& notEqualTo.getLeftExpression() instanceof Column) {
			Column cL = (Column) notEqualTo.getLeftExpression();
			Column cR = (Column) notEqualTo.getRightExpression();
			String leftTab = cL.getTable().getWholeTableName();
			String rightTab = cR.getTable().getWholeTableName();
			if(leftTab.equals(rightTab)) {
				this.selectCondition.get(cL.getTable().getWholeTableName()).add(notEqualTo);//self compare
			}else {
				this.residualJoinExp.add(notEqualTo);
			}

		}
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

	//	@Override
	//	public void visit(SubSelect arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(CaseExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(WhenClause arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(ExistsExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(AllComparisonExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(AnyComparisonExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Concat arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Matches arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(BitwiseAnd arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(BitwiseOr arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(BitwiseXor arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//	
	//	@Override
	//	public void visit(NullValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Function arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(InverseExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(JdbcParameter arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(DoubleValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(DateValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(TimeValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(TimestampValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Parenthesis arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(StringValue arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Addition arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Division arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Multiplication arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Subtraction arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(InExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(IsNullExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(LikeExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(OrExpression arg0) {
	//		throw new UnsupportedException(msg);
	//	}
	//
	//	@Override
	//	public void visit(Between arg0) {
	//		throw new UnsupportedException(msg);
	//	}

}
