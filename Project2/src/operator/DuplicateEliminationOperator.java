package operator;

import java.util.ArrayList;
import java.util.List;

import entity.Tuple;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class DuplicateEliminationOperator extends Operator {


    private List<Tuple> tupleList;
    private int currentIndex;
   
   
   
    public DuplicateEliminationOperator(Operator operator) {
        currentIndex = 0;
        this.tupleList = new ArrayList<>();
        if(operator instanceof InMemorySortOperator) {
            List<Tuple> sortedList = ((InMemorySortOperator)operator).getTupleList();
            if(sortedList.size()>0) {
            tupleList.add(sortedList.get(0));
            }
            if (sortedList.size() > 1) {
                for (int i = 1; i < sortedList.size(); i++) {
//                	check if two tuples are equal
                    if (!sortedList.get(i).equals(sortedList.get(i - 1))) {
                        tupleList.add(sortedList.get(i));
                    }
                }
            }
        }else {

        	Tuple firstTuple = ((ExternalSortOperator)operator).getNextTuple();
        	if(firstTuple!=null) {
        		 tupleList.add(firstTuple);
        	}
        	Tuple lastTuple = firstTuple;
        	Tuple currentTuple = ((ExternalSortOperator)operator).getNextTuple();
        	while(currentTuple!=null) {
        		 if (!currentTuple.equals(lastTuple)) {
                     tupleList.add(currentTuple);
                 }
        		 lastTuple = currentTuple;
        		 currentTuple = ((ExternalSortOperator)operator).getNextTuple();
        	}
        	
        }
        
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
   

}
