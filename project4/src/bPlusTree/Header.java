package bPlusTree;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class Header {
	
private int rootAddress;
private int leafNum;
private int orderNum;

public int getRootAddress() {
	return rootAddress;
}
public void setRootAddress(int rootAddress) {
	this.rootAddress = rootAddress;
}
public int getLeafNum() {
	return leafNum;
}
public void setLeafNum(int leafNum) {
	this.leafNum = leafNum;
}
public int getOrderNum() {
	return orderNum;
}
public void setOrderNum(int orderNum) {
	this.orderNum = orderNum;
}
public Header(int rootAddress, int leafNum, int orderNum) {
	super();
	this.rootAddress = rootAddress;
	this.leafNum = leafNum;
	this.orderNum = orderNum;
}


}
