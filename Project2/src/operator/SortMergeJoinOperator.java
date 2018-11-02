package operator;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * @author QinXuan Pian, Chen Li
 *
 */
public class SortMergeJoinOperator extends Operator{

	private Operator leftChild, rightChild;
	private int innerInd; // current pointer to the tuple of inner
	private int currentParInd; // pointer to the current inner partition
	private Tuple tOuter, tInner;
	private Tuple parInner;
	private List<String> leftOrderList;
	private List<String> rightOrderList;
	private Map<String, Integer> leftSchema;
	private Map<String, Integer> rightSchema;
	private Map<String, Integer> schema;

	/**
	 * @param opLeft
	 * @param opRight
	 * @param plainSelect
	 * @param joinedCondition
	 * @param leftOrderList
	 * @param rightOrderList
	 */
	public SortMergeJoinOperator(Operator opLeft, Operator opRight, PlainSelect plainSelect,Expression joinedCondition, List<String> leftOrderList,List<String> rightOrderList) {

	    leftChild = opLeft;
		rightChild = opRight;
		innerInd = 0;
		this.leftOrderList = leftOrderList;
		this.rightOrderList = rightOrderList;
		this.leftSchema = opLeft.getSchema();
		this.rightSchema = opRight.getSchema();
		this.schema = new HashMap<>();
		schema.putAll(opLeft.getSchema());
		//update schema
		for (Map.Entry<String, Integer> entry : opRight.getSchema().entrySet()) {
			schema.put(entry.getKey(), entry.getValue() + opLeft.getSchema().size());
		}

		//initiate tuples
		tOuter = leftChild.getNextTuple();
		tInner = rightChild.getNextTuple();
		parInner = tInner; // current inner partition
	}

	
	public Tuple getNextTuple() {

		Tuple result = null;
		if (tOuter != null && tInner != null) {
			while (compareTo(tOuter, tInner) < 0) {
				tOuter = leftChild.getNextTuple();
				if(tOuter==null) {
					return null;
				}
			
			}
			while (compareTo(tOuter, tInner) > 0) {
				tInner = rightChild.getNextTuple();
				if(tInner==null) {
					return null;
				}
				innerInd++;
				currentParInd = innerInd;
			
			}
			if(compareTo(tOuter, tInner) != 0) {
				return getNextTuple();
				
			}
			
			long[] newTupleData = new long[tOuter.getData().length + tInner.getData().length];

			for(int i = 0; i < tOuter.getData().length; i++){
				newTupleData[i] = tOuter.getData()[i];
			}

			for(int i = 0; i < tInner.getData().length; i++){
				newTupleData[i + tOuter.getData().length] = tInner.getData()[i];
			}
			String dataString = Arrays.toString(newTupleData).replaceAll(" ", "");
			int len = dataString.length();
			result = new Tuple(dataString.substring(1, len-1));
			innerInd++;
			nextPair();
			if (result != null) return result;
		}
		return null;
	}


	public void nextPair() {
		tInner = rightChild.getNextTuple();
		if(tInner!=null&&compareTo(tOuter, tInner) == 0) {
			innerInd++;
			return;
		}else {
			if(rightChild instanceof InMemorySortOperator) {
				((InMemorySortOperator) rightChild).reset(currentParInd);
				innerInd = currentParInd;
			}
			else {
				((ExternalSortOperator) rightChild).reset(currentParInd);
				innerInd = currentParInd;
			}
			tInner = rightChild.getNextTuple();
			tOuter = leftChild.getNextTuple();
		}
	}



	/**
	 * compare two tuples according to columns in orderList
	 * @param a 
	 * @param b
	 */
	public int compareTo(Tuple a, Tuple b) {
		for (int i = 0; i < this.leftOrderList.size(); i++) {
			int cmp = Long.compare(a.getData()[leftSchema.get(leftOrderList.get(i))], b.getData()[rightSchema.get(rightOrderList.get(i))]);
			if (cmp != 0) return cmp;
		}

		return 0;
	}


	public Map<String,Integer> getSchema(){
		return this.schema;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	};

}

