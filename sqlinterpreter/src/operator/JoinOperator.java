package operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import entity.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.SelectExpressionVisitor;

public class JoinOperator extends Operator {
	private Operator leftOp;
	private Operator rightOp;
	private PlainSelect plainSelect;
	private Map<String, Integer> schema;
	private Tuple outerTuple;
	private Tuple innerTuple;
	private Expression joinedCondition;
	public JoinOperator(Operator opLeft, Operator opRight, PlainSelect plainSelect,Expression joinedCondition){
        this.leftOp = opLeft;
        this.rightOp = opRight;
        this.plainSelect = plainSelect;
        this.schema = new HashMap<>();
        schema.putAll(opLeft.getSchema());
        this.joinedCondition = joinedCondition;
       
        
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
        // update outer tuple and inner tuple
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

        // Concentrate Tuple
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
        if(this.joinedCondition==null) {
        	return tuple;
        }
        while (tuple != null) {
			System.out.println("schema:----:"+this.getSchema());
            SelectExpressionVisitor sv = new SelectExpressionVisitor(tuple, this.getSchema());
            joinedCondition.accept(sv);
            if (sv.getResult()) {
                break;
            }
            tuple = this.getNextTuple();
        }
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
