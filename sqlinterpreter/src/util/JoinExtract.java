package util;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import handler.App;
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

public class JoinExtract implements ExpressionVisitor {
	    private Map<String, Integer> currentSchema;
	    private Deque<Long> data;
	    private Deque<Boolean> values;
	    private Tuple currentTuple;
//	    private List<Expression> andExpressionList = new ArrayList<Expression>();

	    public JoinExtract() {
//	        currentSchema = schema;
//	        data = new LinkedList<>();
//	        values = new LinkedList<>();
//	        currentTuple = tuple;
	       
	    }
//
//	    /**
//	     * @return result of the expression
//	     */
//	    public boolean getResult() {
//	        if (values.isEmpty()) {
//	            return true;
//	        }
//	        return values.peekFirst();
//	    }

	    @Override
	    public void visit(AndExpression andExpression) {
	        // Todo
	    	System.out.println(andExpression.toString());
//	    	andExpressionList.add(andExpression);
	        andExpression.getLeftExpression().accept(this);
	        andExpression.getRightExpression().accept(this);
	    }

	    @Override
	    public void visit(Column column) {
	     
	        
	    }

	    @Override
	    public void visit(LongValue longValue) {
	       
	    }

	    @Override
	    public void visit(EqualsTo equalsTo) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(equalsTo);
	    	if(equalsTo.getLeftExpression() instanceof LongValue && equalsTo.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(equalsTo);
	    	}
	    	if(equalsTo.getLeftExpression() instanceof Column && equalsTo.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)equalsTo.getLeftExpression();
	    		Column columnRight = (Column)equalsTo.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(equalsTo);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(equalsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(equalsTo);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(equalsTo);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(equalsTo);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(equalsTo);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(equalsTo.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)equalsTo.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(equalsTo);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(equalsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)equalsTo.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(equalsTo);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(equalsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}
	    	
	    }
	   

	    @Override
	    public void visit(NotEqualsTo notEqualsTo) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(notEqualsTo);
	    	if(notEqualsTo.getLeftExpression() instanceof LongValue && notEqualsTo.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(notEqualsTo);
	    	}
	    	if(notEqualsTo.getLeftExpression() instanceof Column && notEqualsTo.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)notEqualsTo.getLeftExpression();
	    		Column columnRight = (Column)notEqualsTo.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(notEqualsTo);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(notEqualsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(notEqualsTo);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(notEqualsTo);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(notEqualsTo);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(notEqualsTo);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(notEqualsTo.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)notEqualsTo.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(notEqualsTo);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(notEqualsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)notEqualsTo.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(notEqualsTo);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(notEqualsTo);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}
	    }

	    @Override
	    public void visit(GreaterThan greaterThan) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(greaterThan);
	    	if(greaterThan.getLeftExpression() instanceof LongValue && greaterThan.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(greaterThan);
	    	}
	    	if(greaterThan.getLeftExpression() instanceof Column && greaterThan.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)greaterThan.getLeftExpression();
	    		Column columnRight = (Column)greaterThan.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThan);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(greaterThan);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(greaterThan);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(greaterThan);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(greaterThan);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(greaterThan.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)greaterThan.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThan);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)greaterThan.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThan);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}
	    }

	    @Override
	    public void visit(GreaterThanEquals greaterThanEquals) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(greaterThanEquals);
	    	if(greaterThanEquals.getLeftExpression() instanceof LongValue && greaterThanEquals.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(greaterThanEquals);
	    	}
	    	if(greaterThanEquals.getLeftExpression() instanceof Column && greaterThanEquals.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)greaterThanEquals.getLeftExpression();
	    		Column columnRight = (Column)greaterThanEquals.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThanEquals);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(greaterThanEquals);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(greaterThanEquals);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(greaterThanEquals);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(greaterThanEquals);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(greaterThanEquals.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)greaterThanEquals.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThanEquals);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)greaterThanEquals.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(greaterThanEquals);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(greaterThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}

	    }

	    @Override
	    public void visit(MinorThan minorThan) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(minorThan);
	    	if(minorThan.getLeftExpression() instanceof LongValue && minorThan.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(minorThan);
	    	}
	    	if(minorThan.getLeftExpression() instanceof Column && minorThan.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)minorThan.getLeftExpression();
	    		Column columnRight = (Column)minorThan.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThan);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(minorThan);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(minorThan);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(minorThan);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(minorThan);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(minorThan.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)minorThan.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThan);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)minorThan.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThan);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThan);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}
	    }

	    @Override
	    public void visit(MinorThanEquals minorThanEquals) {
	    	List<Expression> expList = App.model.getAllExp();
	    	expList.add(minorThanEquals);
	    	if(minorThanEquals.getLeftExpression() instanceof LongValue && minorThanEquals.getRightExpression() instanceof LongValue) {
	    		App.model.getsingleTableExp()[0].add(minorThanEquals);
	    	}
	    	if(minorThanEquals.getLeftExpression() instanceof Column && minorThanEquals.getRightExpression() instanceof Column ) {
	    		Column columnLeft = (Column)minorThanEquals.getLeftExpression();
	    		Column columnRight = (Column)minorThanEquals.getRightExpression();
	    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
	    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
	    		if(tableNameLeft.equals(tableNameRight)) {
	    			int index = App.model.getAllTableList().indexOf(tableNameLeft);
	    			if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThanEquals);
		    		}
	    			else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		else {
	    			int indexLeft = App.model.getAllTableList().indexOf(tableNameLeft);
	    			int indexRight = App.model.getAllTableList().indexOf(tableNameRight);
	    			if(indexLeft>indexRight) {
	    				if(App.model.getJoinedTableExp()[indexLeft-1]==null) {
	    					App.model.getJoinedTableExp()[indexLeft-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexLeft-1].add(minorThanEquals);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexLeft-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(minorThanEquals);
		    				App.model.getJoinedTableExp()[indexLeft-1].add(andExpression);
		    				
		    			}
	    				
	    			}
	    			else {
	    				if(App.model.getJoinedTableExp()[indexRight-1]==null) {
	    					App.model.getJoinedTableExp()[indexRight-1] = new ArrayList<Expression>();
	    					App.model.getJoinedTableExp()[indexRight-1].add(minorThanEquals);
			    		}
	    				
	    				else {
		    				Expression leftExp = App.model.getJoinedTableExp()[indexRight-1].remove(0);
		    				AndExpression andExpression = new AndExpression();
		    				andExpression.setLeftExpression(leftExp);
		    				andExpression.setRightExpression(minorThanEquals);
		    				App.model.getJoinedTableExp()[indexRight-1].add(andExpression);
		    				
		    			}
	    				
	    				
	    			}
	    		}
	    		

	    	}
	    	else {
	    		if(minorThanEquals.getLeftExpression() instanceof Column) {
	    			Column columnLeft = (Column)minorThanEquals.getLeftExpression();
		    		String tableNameLeft = columnLeft.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameLeft);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThanEquals);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}else {
	    			Column columnRight = (Column)minorThanEquals.getRightExpression();
		    		String tableNameRight = columnRight.getTable().getWholeTableName().toString();
		    		int index = App.model.getAllTableList().indexOf(tableNameRight);
		    		if(App.model.getsingleTableExp()[index]==null) {
		    			App.model.getsingleTableExp()[index] = new ArrayList<Expression>();
		    			App.model.getsingleTableExp()[index].add(minorThanEquals);
		    		}
		    		else {
	    				Expression leftExp = App.model.getsingleTableExp()[index].remove(0);
	    				AndExpression andExpression = new AndExpression();
	    				andExpression.setLeftExpression(leftExp);
	    				andExpression.setRightExpression(minorThanEquals);
	    				App.model.getsingleTableExp()[index].add(andExpression);
	    				
	    			}
		    		
	    		}
	    		
	    	}
	    }

		@Override
		public void visit(NullValue nullValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Function function) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(InverseExpression inverseExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(JdbcParameter jdbcParameter) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(DoubleValue doubleValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(DateValue dateValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(TimeValue timeValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(TimestampValue timestampValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Parenthesis parenthesis) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(StringValue stringValue) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Addition addition) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Division division) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Multiplication multiplication) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Subtraction subtraction) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(OrExpression orExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Between between) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(InExpression inExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(IsNullExpression isNullExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(LikeExpression likeExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(SubSelect subSelect) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(CaseExpression caseExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(WhenClause whenClause) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(ExistsExpression existsExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(AllComparisonExpression allComparisonExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(AnyComparisonExpression anyComparisonExpression) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Concat concat) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(Matches matches) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(BitwiseAnd bitwiseAnd) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(BitwiseOr bitwiseOr) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(BitwiseXor bitwiseXor) {
			// TODO Auto-generated method stub
			
		}
}
