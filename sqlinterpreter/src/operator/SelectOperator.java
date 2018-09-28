package operator;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.SelectExpressionVisitor;

public class SelectOperator extends Operator {
private ScanOperator scanOperator;
private Expression expression;

public SelectOperator(ScanOperator operator, PlainSelect plainSelect) {
	scanOperator = operator;
	this.expression = plainSelect.getWhere();
}
@Override
public Tuple getNextTuple() {
	if(expression == null) {
		return scanOperator.getNextTuple();
	}
	else {
		 Tuple next = scanOperator.getNextTuple();
		 while (next != null) {
             SelectExpressionVisitor sv = new SelectExpressionVisitor(next, App.model.getCurSchema());
             expression.accept(sv);
             if (sv.getResult()) {
                 break;
             }
             next = scanOperator.getNextTuple();
         }
		 return next;
	}
}
@Override
public void reset() {
	scanOperator.reset();
}
}
