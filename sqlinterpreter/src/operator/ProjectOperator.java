package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends Operator {
Operator childOp;
List<String> selectItemsList = new ArrayList<String>();
List<Integer> tupleIndexList = new ArrayList<Integer>();
public ProjectOperator (Operator operator, PlainSelect plainSelect) {
	childOp = operator;//childOp could be ScanOperator or SelectOperator
	List<SelectItem> itemList = plainSelect.getSelectItems();
	Map<String, Integer> schema = App.model.getCurSchema();
	if(itemList.get(0).toString()!="*") {
	for(SelectItem selectItem:itemList) {
		selectItemsList.add(selectItem.toString());
		int index = schema.get(selectItem.toString());
		tupleIndexList.add(index);
	}
	}
}
@Override
public Tuple getNextTuple() {
	Tuple next = childOp.getNextTuple();
	if(next!=null&&tupleIndexList.size()!=0) {
		long[] data = new long[tupleIndexList.size()];
		for(int i = 0;i<tupleIndexList.size();i++) {
			data[i]=next.getData()[tupleIndexList.get(i)];
		}
		String dataString = Arrays.toString(data).replaceAll(" ", "");
		int len = dataString.length();
		next = new Tuple(dataString.substring(1, len-1));
	}
	return next;
}
@Override
public void reset() {
	childOp.reset();
}
public static void main(String[] args) {
	int[] data = {1,2,3};
	System.out.println(Arrays.toString(data));
	String str = Arrays.toString(data);
	int len = str.length();
	System.out.println(str.substring(1, len-1));
	str = str.substring(1, len-1).replaceAll(" ", "");
	String[] strArray = str.split(",");
	System.out.println(strArray[1]);
	
}
}
