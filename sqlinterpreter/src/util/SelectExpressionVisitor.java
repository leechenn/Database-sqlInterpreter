package util;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import entity.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectExpressionVisitor implements ExpressionVisitor {
	    private Map<String, Integer> currentSchema;
	    private Deque<Long> data;
	    private Deque<Boolean> values;
	    private Tuple currentTuple;

	    public SelectExpressionVisitor(Tuple tuple, Map<String, Integer> schema) {
	        currentSchema = schema;
	        data = new LinkedList<>();
	        values = new LinkedList<>();
	        currentTuple = tuple;
	    }

	    /**
	     * @return result of the expression
	     */
	    public boolean getResult() {
	        if (values.isEmpty()) {
	            return true;
	        }
	        return values.peekFirst();
	    }

	    @Override
	    public void visit(AndExpression andExpression) {
	        // Todo
	        andExpression.getLeftExpression().accept(this);
	        andExpression.getRightExpression().accept(this);
	        boolean rightValue = values.removeFirst();
	        boolean leftValue = values.removeFirst();
	        values.addFirst(leftValue && rightValue);
	    }

	    @Override
	    public void visit(Column column) {
	        // Todo
	        String columnName = column.getWholeColumnName();
	        System.out.println("column name is:"+columnName);
	        // int ind = catalog.getIndexOfColumn(columnName);
	        int ind = currentSchema.get(columnName);
	        data.push((long)currentTuple.getData()[ind]);
	        System.out.println(data);
	    }

	    @Override
	    public void visit(LongValue longValue) {
	        // Todo
	        data.push(longValue.getValue());
	    }

	    @Override
	    public void visit(EqualsTo equalsTo) {
	        // Todo
	        equalsTo.getLeftExpression().accept(this);
	        equalsTo.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue == rightValue);
	    }

	    @Override
	    public void visit(NotEqualsTo notEqualsTo) {
	        // Todo
	        notEqualsTo.getLeftExpression().accept(this);
	        notEqualsTo.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue != rightValue);
	    }

	    @Override
	    public void visit(GreaterThan greaterThan) {
	        // Todo
	        greaterThan.getLeftExpression().accept(this);
	        greaterThan.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue > rightValue);
	    }

	    @Override
	    public void visit(GreaterThanEquals greaterThanEquals) {
	        // todo
	        greaterThanEquals.getLeftExpression().accept(this);
	        greaterThanEquals.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue >= rightValue);

	    }

	    @Override
	    public void visit(MinorThan minorThan) {
	        // Todo
	        minorThan.getLeftExpression().accept(this);
	        minorThan.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue < rightValue);
	    }

	    @Override
	    public void visit(MinorThanEquals minorThanEquals) {
	        // Todo
	        minorThanEquals.getLeftExpression().accept(this);
	        minorThanEquals.getRightExpression().accept(this);
	        long rightValue = data.removeFirst();
	        long leftValue = data.removeFirst();
	        values.addFirst(leftValue <= rightValue);
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
