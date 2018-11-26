package bPlusTree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class IndexNode extends Node {

private List<Node> children;
private List<Integer> indexKeyList;
private List<Integer> addressList;
private int leafMostKey;
private int orderNum;
private int address;

/**
 * @param order
 * @param keys
 * @param children
 * @param address
 */
public IndexNode(int order, List<Integer> keys,List<Node> children,List<Integer> address ) {
	this.orderNum = order;
	this.children = children;
	this.indexKeyList = keys;
	this.addressList = address;
	this.leafMostKey = children.get(0).getMinKey();// this leafMostKey will be useful for building key inside the upper layer Node
	
	
}
public int getMinKey() {
	return leafMostKey;
}
public void setAddress(int address) {
	this.address = address;
}
public int getAddress() {
	return this.address;
}
public List<Integer> getKeysList(){
	return this.indexKeyList;
}
public List<Integer> getChildrenAddress(){
	return this.addressList;
}
@Override
public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("IndexNode with keys [");
	for (Integer key : indexKeyList) {
		sb.append(key + ", ");
	}
	sb.setLength(sb.length() - 2);
	sb.append("] and child addresses [");
	for (Integer addr : addressList) {
		sb.append(addr + ", ");
	}
	sb.setLength(sb.length() - 2);
	sb.append("]\n");
	sb.append("nodeAdress:"+this.address);

	return sb.toString();
}


}
