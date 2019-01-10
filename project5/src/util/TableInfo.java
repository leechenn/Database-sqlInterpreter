package util;

import java.util.HashMap;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class TableInfo {
	private String tableName;
	private int tupleNum = 0;
	private String[] attrsArray;
	private HashMap<String, int[]> attrInfoMap = new HashMap<String,int[]>();//attribute map, key is attrName, value is low value and high value pair;
	//init table info
	public TableInfo(String tableName, String[]attrsArray) {
		this.attrsArray = attrsArray;
		this.tableName = tableName;
		for(String attrName: attrsArray) {
			int[] valuePair = {Integer.MAX_VALUE,Integer.MIN_VALUE};
			attrInfoMap.put(attrName, valuePair);
		}
	}
	public int getTupleNum() {
		return this.tupleNum;
	}
	public String getTableName() {
		return this.tableName;
	}
	public String[] getAttrsName() {
		return this.attrsArray;
	}
	public HashMap<String,int[]> getInfoMap(){
		return this.attrInfoMap;
	}
	public void addTuple() {
		this.tupleNum++;
	}
	@Override
	public String toString() {
		String info = "";
		String tableName = this.tableName;
		int tupleNum = this.tupleNum;
		info = info + tableName + " "+ tupleNum + " ";
		for(String attrName:attrInfoMap.keySet()) {
			//		System.out.println("attrName:"+attrName);
			int[] valuePair = attrInfoMap.get(attrName);
			info = info + attrName + " " + valuePair[0] + " " + valuePair[1]+ " ";

		}
		return info;
	}
}
