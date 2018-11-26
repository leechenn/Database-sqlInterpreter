package operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import entity.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.SelectExpressionVisitor;

/**
 * JoinOperator class for operating joined tables
 * @author Chen Li, QinXuan Pian
 */
public class TupleNestedLoopJoinOperator extends Operator {

	private Operator leftOp;
	private Operator rightOp;
	private PlainSelect plainSelect;
	private Map<String, Integer> schema;
	private Tuple outerTuple;
	private Tuple innerTuple;
	private Expression joinedCondition;
	public int countNum = 0;


	/**
	 * JoinOperator constructor, joinedCondition is the condition expression for two tables
	 * @param opLeft
	 * @param opRight
	 * @param plainSelect
	 * @param joinedCondition
	 */
	public TupleNestedLoopJoinOperator(Operator opLeft, Operator opRight, PlainSelect plainSelect,Expression joinedCondition){

		this.leftOp = opLeft;
		this.rightOp = opRight;
		this.plainSelect = plainSelect;
		this.schema = new HashMap<>();
		schema.putAll(opLeft.getSchema());
		this.joinedCondition = joinedCondition;
		//       update schema
		for (Map.Entry<String, Integer> entry : opRight.getSchema().entrySet()) {
			schema.put(entry.getKey(), entry.getValue() + opLeft.getSchema().size());
		}

		outerTuple = null;
		innerTuple = null;

	}

	/**
	 * get the next tuple of the operator.
	 */
	@Override
	public Tuple getNextTuple(){

		Tuple tuple = this.nextPair();
		//        if expression is null, it is cross product
		if(this.joinedCondition==null) {

			return tuple;
		}
		//if expression, accpect selectExpressionVisitor to deal to expression
		while (tuple != null) {

			SelectExpressionVisitor sv = new SelectExpressionVisitor(tuple, this.getSchema());
			joinedCondition.accept(sv);
			if (sv.getResult()) {
				//            	System.out.println(tuple.toString());
				return tuple;

			}
			tuple = this.nextPair();
		}
		return tuple;
	}
	public Tuple nextPair() {
		if(outerTuple == null && innerTuple == null){
			outerTuple = leftOp.getNextTuple();
			innerTuple = rightOp.getNextTuple();
		}
		else{
			innerTuple = rightOp.getNextTuple();
			if(innerTuple == null){
				rightOp.reset();
				outerTuple = leftOp.getNextTuple();
				innerTuple = rightOp.getNextTuple();
			}
		}
		if(innerTuple == null || outerTuple == null){
			return null;
		}
		long[] newTupleData = new long[outerTuple.getData().length + innerTuple.getData().length];

		for(int i = 0; i < outerTuple.getData().length; i++){
			newTupleData[i] = outerTuple.getData()[i];
		}

		for(int i = 0; i < innerTuple.getData().length; i++){
			newTupleData[i + outerTuple.getData().length] = innerTuple.getData()[i];
		}
		String dataString = Arrays.toString(newTupleData).replaceAll(" ", "");
		int len = dataString.length();
		Tuple tuple = new Tuple(dataString.substring(1, len-1));
		return tuple;
	}

	public Map<String,Integer> getSchema(){
		return this.schema;
	};

	/**
	 * reset the operator.
	 */
	@Override
	public void reset(){

		leftOp.reset();
		rightOp.reset();
	}
}
