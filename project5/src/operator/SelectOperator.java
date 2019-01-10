package operator;

import java.io.PrintStream;
import java.util.Map;
import entity.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.SelectExpressionVisitor;

/**
 * SelectOperator class for selecting tuple according to getwhere() expression 
 * @author Chen Li, QinXuan Pian
 */
public class SelectOperator extends Operator {
	
private Operator operator;
private Expression expression;
private Map<String, Integer> curSchema;

public SelectOperator(Operator operator, PlainSelect plainSelect, Expression expression) {
	
	this.operator = operator;
	this.expression = expression;
	curSchema = operator.getSchema();//get current schema 

}

@Override
public Tuple getNextTuple() {
	if(expression == null) {
		return this.operator.getNextTuple();// if expression is null, each tuple will be selected 
	}
//	if expression is not null, the tuple will be selected according to select expression
	else {
		
		 Tuple next = this.operator.getNextTuple();
		 while (next != null) {
//			 SelectExpressionVisitor will be accepted here for selectexpression
             SelectExpressionVisitor sv = new SelectExpressionVisitor(next, operator.getSchema());
             expression.accept(sv);
             if (sv.getResult()) {
                 break;
             }
             next = this.operator.getNextTuple();
         }
		 return next;
	}
}
@Override
public void reset() {
	this.operator.reset();
}
public Map<String,Integer> getSchema() {
	return this.curSchema;
}

@Override
public String printPhysicalTree() {
	// TODO Auto-generated method stub
	return String.format("Select[%s]", 
			((expression == null) ? "null" : expression.toString()));

}
@Override
public void printTree(PrintStream ps, int lv) {
	printIndent(ps, lv);
	ps.println(printPhysicalTree());
	operator.printTree(ps, lv + 1);
}
public String getTableName() {
	return this.operator.getTableName();
}

}
