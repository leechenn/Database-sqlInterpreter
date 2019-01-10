package operator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.expression.Expression;
import util.SelectExpressionVisitor;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class BlockNestedJoinOperator extends Operator {
	private int blocksize;
	private List<Tuple> block;
	private Tuple outTuple;
	private Tuple innerTuple;
	private int id = 0;
	private Operator left;
	private Operator right;
	private Expression expr;
	private Map<String, Integer> schema;
	public BlockNestedJoinOperator(Operator left, Operator right, Expression expr) {

		this.left = left;
		this.right = right;
		this.expr = expr;
		this.schema = new HashMap<>();
		schema.putAll(left.getSchema());
		int tuplesize = left.getSchema().size()*4;
		for (Map.Entry<String, Integer> entry : right.getSchema().entrySet()) {
			schema.put(entry.getKey(), entry.getValue() + left.getSchema().size());
		}
		blocksize = App.model.joinBuffer*(4096/tuplesize);
		block = new ArrayList();
		readBlock();
		right.reset();

	}
	public Tuple nextPair() {
		if(outTuple == null && innerTuple == null){
			outTuple = getTuple();
			innerTuple = right.getNextTuple();
		}
		else{
			outTuple = getTuple();
			if(outTuple == null){

				innerTuple = right.getNextTuple();
				if(innerTuple == null) {
					readBlock();
					outTuple = getTuple();
					right.reset();
					innerTuple = right.getNextTuple();

				}
				else {
					id = 0;
					outTuple = getTuple();
				}

			}
		}

		if(outTuple == null){
			return null;
		}
		long[] newTupleData = new long[outTuple.getData().length + innerTuple.getData().length];

		for(int i = 0; i < outTuple.getData().length; i++){
			newTupleData[i] = outTuple.getData()[i];
		}

		for(int i = 0; i < innerTuple.getData().length; i++){
			newTupleData[i + outTuple.getData().length] = innerTuple.getData()[i];
		}
		String dataString = Arrays.toString(newTupleData).replaceAll(" ", "");
		int len = dataString.length();
		Tuple tuple = new Tuple(dataString.substring(1, len-1));
		return tuple;
	}
	public Tuple getTuple() {
		if(id<block.size()) {
			Tuple tuple =  block.get(id);
			id++;
			return tuple;
		}
		else {
			return null;
		}
	}
	public void readBlock() {
		block.clear();
		int count = 0;
		Tuple tp = null;
		while(count < blocksize && (tp = left.getNextTuple()) != null) {

			block.add(tp);
			count++;
		}
		id = 0;

	}

	@Override
	public Tuple getNextTuple() {

		Tuple tuple = this.nextPair();
		//      if expression is null, it is cross product
		if(this.expr==null) {
			return tuple;
		}
		//if expression, accpect selectExpressionVisitor to deal to expression
		while (tuple != null) {
			SelectExpressionVisitor sv = new SelectExpressionVisitor(tuple, this.getSchema());
			expr.accept(sv);
			if (sv.getResult()) {
				return tuple;
			}
			tuple = this.nextPair();
		}
		return tuple;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}
	public Map<String,Integer> getSchema(){
		return this.schema;
	}
	@Override
	public String printPhysicalTree() {
		String expression = (expr != null) ? expr.toString() : "null";
        return String.format("BNLJ[" + expression + "]");
	};
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printPhysicalTree());
		left.printTree(ps, lv + 1);
		right.printTree(ps, lv + 1);
	}

}
