package operator;

import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
//import util.JoinExpressionVisitor;
import util.SelectExpressionVisitor;

public class SelectOperator extends Operator {
private ScanOperator scanOperator;
private Operator operator;
private Expression expression;
private Map<String, Integer> curSchema;

public SelectOperator(Operator operator, PlainSelect plainSelect,Expression expression) {
	this.operator = operator;
	this.expression = expression;
	curSchema = operator.getSchema();
//	JoinExpressionVisitor joinExpress = new JoinExpressionVisitor(operator.getSchema());
//    expression.accept(joinExpress);
//    expression = joinExpress.getExpression();
//	if(this.operator instanceof JoinOperator ) {
//		System.out.println("join schema:-----"+operator.getSchema());
//		joinExpress = new JoinExpressionVisitor(operator.getSchema());
//        expression.accept(joinExpress);
//        expression = joinExpress.getExpression();
//        System.out.println("join expression------------"+expression.toString());
//	}
}
@Override
public Tuple getNextTuple() {
	if(expression == null) {
		return this.operator.getNextTuple();
	}
	else {
		 Tuple next = this.operator.getNextTuple();
		 while (next != null) {
			 System.out.println("schema:----:"+operator.getSchema());
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
}
