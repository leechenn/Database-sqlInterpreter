package operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import entity.Tuple;

public class DuplicateEliminationOperator extends Operator {

	// stores tuples
    private List<Tuple> tupleList;
    private int currentIndex;
    private Map<String, Integer> schema;

    
    public DuplicateEliminationOperator(Operator operator) {
        currentIndex = 0;
        this.schema = operator.getSchema();
        this.tupleList = new ArrayList<>();
        if (operator instanceof SortOperator) {
            List<Tuple> sortedList = ((SortOperator) operator).getTupleList();
            if (sortedList.size() > 1) {
                tupleList.add(sortedList.get(0));
                for (int i = 1; i < sortedList.size(); i++) {
                    if (!sortedList.get(i).equals(sortedList.get(i - 1))) {
                        tupleList.add(sortedList.get(i));
                    }
                }
            }
        }
    }

   
    @Override
    public Tuple getNextTuple() {
        // TODO Auto-generated method stub
        Tuple tuple = null;
        if (currentIndex < tupleList.size()) {
            tuple = tupleList.get(currentIndex);
        }
        currentIndex++;
        return tuple;
    }

    
    @Override
    public void reset() {
        // TODO Auto-generated method stub
        currentIndex = 0;
    }

    /**
     * get the schema
     */
    @Override
    public Map<String, Integer> getSchema(){
        return this.schema;
    }

}
