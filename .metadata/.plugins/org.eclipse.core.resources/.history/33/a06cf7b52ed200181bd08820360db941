package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import entity.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;


/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class ProjectOperator extends Operator {

	private Operator childOp;
	private List<String> selectItemsList = new ArrayList<String>();
	private List<Integer> tupleIndexList = new ArrayList<Integer>();
	private Map<String, Integer> schema;
	private int tupleLen;

	/**
	 * ProjectOperator construct
	 *
	 */
	public ProjectOperator (Operator operator, PlainSelect plainSelect) {

		childOp = operator;//childOp could be ScanOperator, SelectOperator and JoinOperator 
		List<SelectItem> itemList = plainSelect.getSelectItems();
		this.schema =  operator.getSchema();
		if(itemList.get(0).toString()!="*") {
			tupleLen = itemList.size();//will be used for DuplicateEliminationOperator
			for(SelectItem selectItem:itemList) {
				selectItemsList.add(selectItem.toString());
				int index = schema.get(selectItem.toString());
				tupleIndexList.add(index); //find columns which should be selected
			}
		}
		else {
			tupleLen = schema.size();
		}
	}

	public Map<String, Integer> getSchema() {
		return this.schema;
	}

	public int getTupleLen() {
		return this.tupleLen;
	}

	@Override
	public Tuple getNextTuple() {
		Tuple next = childOp.getNextTuple();
		if(next!=null && tupleIndexList.size()!=0) {
			long[] data = new long[tupleIndexList.size()];
			for(int i = 0;i<tupleIndexList.size();i++) {
				data[i]=next.getData()[tupleIndexList.get(i)];
			}
			String dataString = Arrays.toString(data).replaceAll(" ", "");
			int len = dataString.length();
			next = new Tuple(dataString.substring(1, len-1));//create a projected tuple according to select items
		}
		return next;
	}
	@Override
	public void reset() {
		childOp.reset();
	}

}
