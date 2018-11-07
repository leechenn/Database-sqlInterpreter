package bPlusTree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import handler.App;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class WriteBPlusTree {
	
	private File indexFile;
	private int pageSize = 4096;
	private int leafFlag = 0;
	private int indexFlag = 1;
	private int headerFlag = 0;
	private Header header;
	private FileChannel fc;
	private ByteBuffer buffer;
	private int pageNumber;
	private List<IndexNode> indexNodeList;
	private List<Node> leafNodeList;
	
	public WriteBPlusTree(Header header, List<IndexNode> indexNodeList, List<Node> leafNodeList, String indexFileName) {
		this.indexNodeList = indexNodeList;
		this.leafNodeList = leafNodeList;
		this.header = header;
		this.indexFile = new File(indexFileName);
		try {
			fc = new FileOutputStream(this.indexFile).getChannel();
			buffer = buffer.allocate(pageSize);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * write the entire index tree to file
	 */
	public void writeToFile() {
		//write header
		long headerPosition = 0;
		try {
			fc.position(headerPosition);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		clearBuffer();
		buffer.putInt(header.getRootAddress());
		buffer.putInt(header.getLeafNum());
		buffer.putInt(header.getOrderNum());
		while(buffer.hasRemaining()) {
			buffer.putInt(0);
		}
		buffer.flip();
		try {
			fc.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//write leafNode
		
		this.pageNumber = 1;
		
		for(Node leafNode:leafNodeList) {
			long position = this.pageSize*pageNumber;
			try {
				fc.position(position);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			clearBuffer();
			LeafNode lNode = (LeafNode)leafNode;
			int entryNum = lNode.getDataEntry().size();
			buffer.putInt(this.leafFlag);
			buffer.putInt(entryNum);
			for(DataEntry dataEntry:lNode.getDataEntry()) {
				buffer.putInt(dataEntry.getKey());
				buffer.putInt(dataEntry.getRidList().size());
				for(Rid rid:dataEntry.getRidList()) {
					buffer.putInt(rid.getPageId());
					buffer.putInt(rid.getTupleId());
				}
			}
			while(buffer.hasRemaining()) {
				buffer.putInt(0);
			}
			buffer.flip();
			try {
				fc.write(buffer);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			pageNumber++;
		}
		//write indexNode
		
		for(IndexNode indexNode:indexNodeList) {
			long position = this.pageSize*pageNumber;
			try {
				fc.position(position);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			clearBuffer();
			int keyNum = indexNode.getKeysList().size();
			buffer.putInt(this.indexFlag);
			buffer.putInt(keyNum);
			for(Integer key:indexNode.getKeysList()) {
				buffer.putInt(key);
			}
			for(Integer address:indexNode.getChildrenAddress()) {
				buffer.putInt(address);
			}
			while(buffer.hasRemaining()) {
				buffer.putInt(0);
			}
			buffer.flip();
			try {
				fc.write(buffer);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			pageNumber++;
		}
		
	}
	public void clearBuffer() {
		buffer.clear();
		buffer.put(new byte[this.pageSize]);
		buffer.clear();
	}
	public void close() {
		try {
			fc.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
