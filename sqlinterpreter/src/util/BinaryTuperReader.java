package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import entity.Tuple;

public class BinaryTuperReader implements TuperReader{
	
	private ByteBuffer page;
	private FileInputStream fin;
	private FileChannel fc;
	private int columnNum;
	private ByteBuffer zeros;
	
	private int count = 0; // num of tuples per page
	
	private String filepath;
	
	
	public BinaryTuperReader(String fp, HashMap<String, Integer> currentSchema) {
		
		page = ByteBuffer.allocate(4096); //one page = 4096 bytes
		columnNum = currentSchema.size();
		filepath = fp;
		
		try {
			fin = new FileInputStream(filepath);
			fc = fin.getChannel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		zeros = ByteBuffer.allocate(4096);
		while (zeros.hasRemaining()) {
			zeros.putInt(0);
			zeros.flip(); // ready for read
		}
	}
	
	public void fillPage() {
		
		//reset the page
		page.clear();
		//zero out page, reset position to 0
		page.put(zeros);
		page.flip();
		zeros.rewind();
		
		//metadata: tuple size, number of tuple
		page.putInt(0, columnNum);
		page.putInt(4, count);
		
		while (page.remaining() > 4*columnNum) { //can fit the last tuple
			try {
				if (fc.read(page) == -1) {
					close();
					break; // nothing left to read into the buffer
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		assert ((page.position() - 8)%columnNum == 0);
		count = (page.position() - 8)/columnNum;
		
		page.putInt(4, count);
		
		// ready to be read
		page.flip();
		page.getInt();
		page.getInt();
	}

	@Override
	public Tuple readNext() {
		Tuple next = null;
		if (count == 0) {
			fillPage();
			// if count is still 0 => nothing left to read
			if (count == 0)
				return next;
			else
				return readNext();
		}
		else {
			int[] result = new int[columnNum];
			for (int i = 0; i < columnNum; i++) {
				result[i] = page.getInt();
			}
			count --;
			next = new Tuple(result);
			return next;
		}
	}

	@Override
	public void close() {
		try {
			fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void reset() {
		try {
			fin.close();
			fin = new FileInputStream(filepath);
			fc = fin.getChannel();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
