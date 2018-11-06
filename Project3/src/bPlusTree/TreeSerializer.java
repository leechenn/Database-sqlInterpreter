package bPlusTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.tree.TreeNode;

import entity.Tuple;
import handler.App;
import util.Catalog;
import util.TupleReader;

/**
 * @author chen
 *
 */
public class TreeSerializer {
	private int orderNum;
	private String tableName;
	private String indexKey;
	private Header header;
	private List<Node> leafNodeList = new ArrayList<Node>();
	private List<IndexNode> indexNodeList = new ArrayList<IndexNode>();
	private List<DataEntry> dataEntryList;
	private Map<Integer,DataEntry> leafNodeMap = new HashMap<Integer,DataEntry>();
	private int keyInt;
	private int dataEntryNum = 0;
	private int indexNodeStartAddress;
	private String indexFileName;
public TreeSerializer(int orderNum,String tableName,String indexKey,int keyInt) {
	this.orderNum = orderNum;
	this.tableName = tableName;
	this.indexKey = indexKey;
	this.indexFileName = Catalog.indexDir+"/"+this.tableName+"."+this.indexKey;
	this.keyInt = keyInt;
}
public void constructBPlustTree() {
	//scan table construct dataEntry and sort
	TupleReader tr = new TupleReader(this.tableName);
	Tuple tp = tr.readNext();
	while(tp!=null) {
		if(!leafNodeMap.containsKey(tp.getData()[keyInt])) {
		List<Rid> ridList = new ArrayList<Rid>();
		Rid rid = new Rid(tr.getPageIndex(),tr.getTupleIndex());
		ridList.add(rid);
		DataEntry dataEntry = new DataEntry(tp.getData()[keyInt],ridList);
		leafNodeMap.put(tp.getData()[keyInt], dataEntry);
		}
		else {
			Rid rid = new Rid(tr.getPageIndex(),tr.getTupleIndex());
			leafNodeMap.get(tp.getData()[keyInt]).addRid(rid);
		}
		tp = tr.readNext();
	}
	tr.close();
	dataEntryList = new ArrayList<DataEntry>();
	
	for(Map.Entry<Integer,DataEntry> entry:leafNodeMap.entrySet()) {
		DataEntry dataEntry = entry.getValue();
		List<Rid> ridList = dataEntry.getRidList();
		Collections.sort(ridList, new Rid.ridCompare());
		dataEntryList.add(dataEntry);
		dataEntryNum++;
	}
	Collections.sort(dataEntryList, new DataEntry.dataEntryCompare());
	
	//construct leaf nodes
	
	int startIndex = 0;
	int remainEntries = dataEntryNum;
	while(remainEntries>=3*orderNum) {
		
		List<DataEntry> oneLeafNodeList = new ArrayList<DataEntry>();
	for(int i = 0;i<orderNum*2;i++) {
		oneLeafNodeList.add(dataEntryList.get(i+startIndex));
		
	}
	LeafNode leafNode = new LeafNode(orderNum,oneLeafNodeList);
	leafNodeList.add(leafNode);
	startIndex+=(2*orderNum);
	remainEntries = remainEntries-(orderNum*2);

	}
	if(remainEntries<=2*orderNum) {
		List<DataEntry> oneLeafNodeList = new ArrayList<DataEntry>();
		for(int i = 0;i<(dataEntryNum-startIndex);i++) {
			oneLeafNodeList.add(dataEntryList.get(i+startIndex));
			
		}
		LeafNode leafNode = new LeafNode(orderNum,oneLeafNodeList);
		leafNodeList.add(leafNode);
	}else {
		List<DataEntry> oneLeafNodeList = new ArrayList<DataEntry>();
		for(int i = 0;i<remainEntries/2;i++) {
			oneLeafNodeList.add(dataEntryList.get(i+startIndex));
			
		}
		LeafNode leafNode = new LeafNode(orderNum,oneLeafNodeList);
		leafNodeList.add(leafNode);
		startIndex+=(remainEntries/2);
		oneLeafNodeList = new ArrayList<DataEntry>();
		for(int i = startIndex;i<dataEntryNum;i++) {
			oneLeafNodeList.add(dataEntryList.get(i));
			
		}
		leafNode = new LeafNode(orderNum,oneLeafNodeList);
		leafNodeList.add(leafNode);
	}
	this.indexNodeStartAddress = leafNodeList.size()+1;
	
	//construct first index node layer
	
	createFirstIndexLayer(this.leafNodeList);
	
	//construct rest index node
	
	createrestIndexLayer(this.indexNodeList);
	
	//construct header
	
	int rootAddress = this.indexNodeList.get(this.indexNodeList.size()-1).getAddress();
	this.header = new Header(rootAddress, this.leafNodeList.size(), this.orderNum);
	
//	for(IndexNode indexNode:this.indexNodeList) {
//		System.out.println(indexNode);
//	}
	
	//write to index file and then close
	
	WriteBPlusTree wbt = new WriteBPlusTree(this.header,this.indexNodeList,this.leafNodeList,this.indexFileName);
	wbt.writeToFile();
	wbt.close();
	


}

/**
 * construct first index node layer
 * @param leafNodeList
 * @return
 */
public List<IndexNode> createFirstIndexLayer(List<Node> leafNodeList){
	List<IndexNode> layer = new ArrayList<IndexNode>();
	//basecase only one leafnode
	if(leafNodeList.size()==1) {
		List<Integer> addressList = new ArrayList<Integer>();
		addressList.add(1);
		IndexNode indexNode = new IndexNode(this.orderNum, null,leafNodeList,addressList);
		indexNode.setAddress(indexNodeStartAddress);
	}else {
		int startIndex = 0;
		int remainchildren = leafNodeList.size();
		while(remainchildren>=(3*orderNum+2)) {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<2*orderNum+1;i++) {
				addressList.add(startIndex+i+1);
				childrenList.add(leafNodeList.get(startIndex+i));
				
			}
			for(int i = 1;i<2*orderNum+1;i++) {
				keysList.add(leafNodeList.get(startIndex+i).getMinKey());
			}
			startIndex+=(2*orderNum+1);
			remainchildren = remainchildren - orderNum*2 - 1;
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			this.indexNodeList.add(indexNode);
			
			
		}
		
		
		if(remainchildren<=2*orderNum+1) {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<(leafNodeList.size()-startIndex);i++) {
				addressList.add(startIndex+i+1);
				childrenList.add(leafNodeList.get(startIndex+i));		
			}
			for(int i = 1;i<(leafNodeList.size()-startIndex);i++) {
				keysList.add(leafNodeList.get(startIndex+i).getMinKey());
			}
			
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			this.indexNodeList.add(indexNode);
		}else {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<remainchildren/2;i++) {
				addressList.add(startIndex+i+1);
				childrenList.add(leafNodeList.get(startIndex+i));
				
			}
			for(int i = 1;i<remainchildren/2;i++) {
				keysList.add(leafNodeList.get(startIndex+i).getMinKey());
			}
			startIndex+=remainchildren/2;
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			this.indexNodeList.add(indexNode);
			addressList = new ArrayList<Integer>();
			keysList = new ArrayList<Integer>();
			childrenList = new ArrayList<Node>();
			for(int i = 0;i<remainchildren/2;i++) {
				addressList.add(startIndex+1+i);
				childrenList.add(leafNodeList.get(startIndex+i));
				
			}
			for(int i = 1;i<remainchildren/2;i++) {
				keysList.add(leafNodeList.get(startIndex+i).getMinKey());
			}
			indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			this.indexNodeList.add(indexNode);
		}
	}
	return this.indexNodeList;
	
}

/**
 * @param indexNodeLayer
 * @return
 */
public void createrestIndexLayer(List<IndexNode> indexNodeLayer){
	List<IndexNode> layer = new ArrayList<IndexNode>();
	//root indexNode
	if(indexNodeLayer.size()==1) {
		return;
	}else {
		int startIndex = 0;
		int remainchildren = indexNodeLayer.size();
		while(remainchildren>=(3*orderNum+2)) {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<2*orderNum+1;i++) {
				addressList.add(indexNodeLayer.get(startIndex+i).getAddress());
				childrenList.add(indexNodeLayer.get(startIndex+i));
				
			}
			for(int i = 1;i<2*orderNum+1;i++) {
				keysList.add(indexNodeLayer.get(startIndex+i).getMinKey());
			}
			startIndex+=(2*orderNum+1);
			remainchildren = remainchildren - orderNum*2 - 1;
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			layer.add(indexNode);
			
			
		}
		
		
		if(remainchildren<=2*orderNum+1) {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<(indexNodeLayer.size()-startIndex);i++) {
				addressList.add(indexNodeLayer.get(startIndex+i).getAddress());
				childrenList.add(indexNodeLayer.get(startIndex+i));		
			}
			for(int i = 1;i<(indexNodeLayer.size()-startIndex);i++) {
				keysList.add(indexNodeLayer.get(startIndex+i).getMinKey());
			}
			
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			layer.add(indexNode);
		}else {
			List<Integer> addressList = new ArrayList<Integer>();
			List<Integer> keysList = new ArrayList<Integer>();
			List<Node> childrenList = new ArrayList<Node>();
			for(int i = 0;i<remainchildren/2;i++) {
				addressList.add(indexNodeLayer.get(startIndex+i).getAddress());
				childrenList.add(indexNodeLayer.get(startIndex+i));
				
			}
			for(int i = 1;i<remainchildren/2;i++) {
				keysList.add(indexNodeLayer.get(startIndex+i).getMinKey());
			}
			startIndex+=remainchildren/2;
			IndexNode indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			layer.add(indexNode);
			addressList = new ArrayList<Integer>();
			keysList = new ArrayList<Integer>();
			childrenList = new ArrayList<Node>();
			for(int i = 0;i<remainchildren/2;i++) {
				addressList.add(indexNodeLayer.get(startIndex+i).getAddress());
				childrenList.add(indexNodeLayer.get(startIndex+i));
				
			}
			for(int i = 1;i<remainchildren/2;i++) {
				keysList.add(indexNodeLayer.get(startIndex+i).getMinKey());
			}
			indexNode = new IndexNode(this.orderNum, keysList,childrenList,addressList);
			indexNode.setAddress(this.indexNodeStartAddress++);
			layer.add(indexNode);
		}
	}
	this.indexNodeList.addAll(layer);
	createrestIndexLayer(layer);
	
}




}
