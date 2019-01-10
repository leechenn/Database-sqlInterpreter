package bPlusTree;

import java.util.List;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class LeafNode extends Node {
	
private List<DataEntry> dataEntries;
private int orderNum;
private int minKey;

/**
 * @param order
 * @param dataEntries
 */
public LeafNode(int order,List<DataEntry> dataEntries){
	
	this.minKey = dataEntries.get(0).getKey();//record the minimum key for each leaf Node
	this.orderNum = order;
	this.dataEntries = dataEntries;
}
public int getMinKey() {
	return this.minKey;
}
public List<DataEntry> getDataEntry(){
	return this.dataEntries;
}

@Override
public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("LeafNode[\n");
	for (DataEntry data : dataEntries) {
		sb.append(data.toString() + "\n");
	}
	sb.append("]\n");
	return sb.toString();
}

}
