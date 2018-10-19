package operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import entity.Tuple;

/**
 * @author Chen Li, Qinxuan Pian
 *
 */
public class DuplicateEliminationOperator extends Operator {


    private List<Tuple> tupleList;
    private int currentIndex;
   
   
   
    public DuplicateEliminationOperator(SortOperator operator) {
        currentIndex = 0;
        this.tupleList = new ArrayList<>();
            List<Tuple> sortedList = operator.getTupleList();
            if (sortedList.size() > 1) {
                tupleList.add(sortedList.get(0));
                for (int i = 1; i < sortedList.size(); i++) {
//                	check if two tuples are equal
                    if (!sortedList.get(i).equals(sortedList.get(i - 1))) {
                        tupleList.add(sortedList.get(i));
                    }
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
