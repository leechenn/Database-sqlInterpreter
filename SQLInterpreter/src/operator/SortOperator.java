package operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * @author Chen Li, Qinxuan Pian
 * SortOperator for sorting tuple according to expression after order by
 */
public class SortOperator extends Operator {
	
	private List<Tuple> tupleList;
	private PlainSelect plainSelect;
	private int currentIndex;
	private Map<String, Integer> schema;

	/**
	 * SortOperator constructor
	 * @param operator
	 * @param plainSelect
	 */
	public SortOperator(Operator operator, PlainSelect plainSelect) {
		
		tupleList = new ArrayList<>();
		this.plainSelect = plainSelect;
		this.schema = operator.getSchema();
		Tuple tuple = operator.getNextTuple();
		
//		add all tuple to list
		while(tuple != null) {
			tupleList.add(tuple);
			tuple = operator.getNextTuple();
		}
		//override compare method for sorting tuple
        Collections.sort(tupleList, new Comparator<Tuple>(){
        	List<OrderByElement> order = plainSelect.getOrderByElements();
			@Override
			public int compare(Tuple t1, Tuple t2) {
				if (order != null) {
					for (int i = 0; i < order.size(); i++) {
						String column = order.get(i).toString();
						int index = schema.get(column);
						if (t1.getData()[index] > t2.getData()[index]) {
							return 1;
						}
						if (t1.getData()[index] < t2.getData()[index]) {
							return -1;
						}
					}
				}
//				if child operator is not project operator, all columns should be in tuple
				int len = operator.getSchema().size();
//				if child operator is project operator, the number of columns could be different
				if(operator instanceof ProjectOperator) {
					len = ((ProjectOperator)operator).getTupleLen();
				}
				
				for (int i = 0; i < len ; i++){
					
					if (t1.getData()[i] > t2.getData()[i]) {
						return 1;
					}
					if (t1.getData()[i] < t2.getData()[i]) {
						return -1;
					}
				}
				return 0;
			}
        	
        	});
		
	}

	
	@Override
	public Tuple getNextTuple() {
		
		Tuple tuple = null;
		if (currentIndex < tupleList.size()) {
			tuple = tupleList.get(currentIndex);
		}
		currentIndex++;
		return tuple;

	}

	
	@Override
	public void reset() {
		currentIndex = 0;
	}

	
	@Override
	public Map<String, Integer> getSchema(){
		return this.schema;
	}

	
	public List<Tuple> getTupleList() {
		return tupleList;
	}

	
	

}
