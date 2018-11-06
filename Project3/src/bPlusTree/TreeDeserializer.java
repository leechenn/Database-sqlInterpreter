package bPlusTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TreeDeserializer {
	private File indexFile;
	private FileChannel fc;
	private ByteBuffer buffer;
	private int leafNum;
	private int orderNum;
	private Integer lowKey;
	private Integer highKey;
	private LeafNode curLeaf;
	private int rootAddress;
	private int curAddress;
	private int dataEntryIndex;
	private int curRidIndex;
	
/**
 * lowKey and highKey will be included in search range
 * @param indexFile
 * @param lowKey
 * @param highKey
 */
public TreeDeserializer(File indexFile,Integer lowKey,Integer highKey) {
	this.lowKey = lowKey;
	this.highKey = highKey;
	this.indexFile = indexFile;
	try {
		fc = new FileInputStream(indexFile).getChannel();
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	buffer = ByteBuffer.allocate(4096);
	this.readHead();
}
//public static void main(String[] args) {
//	List<Integer> list = new ArrayList<Integer>();
//	list.add(4);
//	list.add(7);
//	list.add(9);
//	list.add(10);
//	System.out.println(Collections.binarySearch(list, 8));
//}
public void readHead() {
	readPage(0);
	this.rootAddress = buffer.getInt(0);
	this.leafNum = buffer.getInt(4);
	this.orderNum = buffer.get(8);
	clearBuffer();
	
}
public void readPage(int pageId) {
	clearBuffer();
	try {
		fc.position((long)pageId*4096);
		fc.read(buffer);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	buffer.flip();
	
	
}
public void clearBuffer() {
	buffer.clear();
	buffer.put(new byte[4096]);
	buffer.clear();
}
/**
 * move point to start leafNode
 */
public void findStartLeaf() {
	if(lowKey == null||this.leafNum==1) {
		this.curLeaf = this.leafNodeD(1);
		this.curAddress = 1;
	}else {
	
		this.curAddress = this.rootAddress;
		while(curAddress>this.leafNum) {
		List<Integer>[] keyAddressPair = this.getKeyAddressPair(this.curAddress);
		List<Integer> keyList = keyAddressPair[0];
		List<Integer> addressList = keyAddressPair[1];
		//if equal, find right child
		int nextPageIndex = Collections.binarySearch(keyList, lowKey);
		nextPageIndex = nextPageIndex >= 0 ? nextPageIndex+1 : -(nextPageIndex + 1);
		curAddress = addressList.get(nextPageIndex);
		
		}
//		System.out.println(this.curAddress);
		this.curLeaf = this.leafNodeD(curAddress);
		this.dataEntryIndex = 0;
		while(curLeaf !=null&&curLeaf.getDataEntry().get(dataEntryIndex).getKey()<this.lowKey) {
			dataEntryIndex++;
			if(dataEntryIndex>=curLeaf.getDataEntry().size()) {
				this.curAddress++;
				if(curAddress>0&&curAddress<=this.leafNum) {
				this.curLeaf = this.leafNodeD(curAddress);
				dataEntryIndex = 0;
				}else {
					this.curLeaf = null;
				}
				
			}
		}
	}
}

/**
 * get next rid
 * @return
 */
public Rid getNextRid() {
	if(this.curLeaf==null) {
		return null;
	}
	if(this.curRidIndex == this.curLeaf.getDataEntry().get(this.dataEntryIndex).getRidList().size()) {
		this.dataEntryIndex++;
		this.curRidIndex = 0;
	}
	if(this.dataEntryIndex == this.curLeaf.getDataEntry().size()) {
		this.curAddress++;
		if(curAddress>0&&curAddress<=this.leafNum) {
		this.curLeaf = this.leafNodeD(curAddress);
		dataEntryIndex = 0;
		}else {
			this.curLeaf = null;
		}
		if(this.curLeaf == null) {
			return null;
		}
	}
	if(highKey!=null&&this.curLeaf.getDataEntry().get(this.dataEntryIndex).getKey()>this.highKey) {
		return null;
	}
	
	return this.curLeaf.getDataEntry().get(this.dataEntryIndex).getRidList().get(this.curRidIndex++);
	
	
}
/**
 * get keys and addresses pair from a indexNode
 * @return
 */
public List<Integer>[] getKeyAddressPair(int address) {
	this.readPage(address);
	//skip flag
	buffer.position(4);
	int keyNum = buffer.getInt();
	List<Integer> keysList = new ArrayList<Integer>();
	for(int i = 0; i<keyNum; i++) {
		keysList.add(buffer.getInt());
	}
	List<Integer> addressList = new ArrayList<Integer>();
	for(int i = 0; i<keyNum+1;i++) {
		addressList.add(buffer.getInt());
	}
	List<Integer>[] keyAddressPair = new ArrayList[2];
	keyAddressPair[0] =  keysList;
	keyAddressPair[1] = addressList;
	return keyAddressPair;
}
public LeafNode leafNodeD(int address) {
	this.readPage(address);
	buffer.position(4);
	int entryNum = buffer.getInt();
	List<DataEntry> dataEntries = new ArrayList<DataEntry>();
	for(int i = 0;i<entryNum;i++) {
		int key = buffer.getInt();
		int ridNum = buffer.getInt();
		List<Rid> ridList = new ArrayList<Rid>();
		for(int j = 0;j<ridNum;j++) {
			int pageId = buffer.getInt();
			int tupleId = buffer.getInt();
			ridList.add(new Rid(pageId,tupleId));
		}
		dataEntries.add(new DataEntry(key,ridList));
	}
	return new LeafNode(this.orderNum,dataEntries);
}

}
