package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import entity.Tuple;

public class BinaryTupleReader implements TupleReader{
	
	private ByteBuffer page;
	private FileInputStream fin;
	private int columnNum;
	
	private int count = 0; // num of tuples per page
	
	private String filepath;
	public FileChannel fc;
	private ByteBuffer zeros;
	
	public BinaryTupleReader(String fp) {
		
		page = ByteBuffer.allocate(4096); //one page = 4096 bytes
		filepath = fp;
		System.out.println("initiation");
		
		try {
			fin = new FileInputStream(filepath);
			fc = fin.getChannel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// a bytebuffer of 0s
		zeros = ByteBuffer.allocate(4096);
		while (zeros.hasRemaining()) {
			zeros.putInt(0);
		}
		zeros.flip(); // ready for read
		
	}
	
	public void fillPage() {
		System.out.println("fillpage");

		assert (count == 0);
		
		//reset and zero out the buffer
		page.clear();
		page.put(zeros);
		zeros.rewind();
		page.flip();

		try {
			int temp = fc.read(page);
			System.out.println("bytes read: " + temp);
			if (temp == -1) {// nothing left to read into the buffer
				close();
			}
			else {
				page.flip();
				columnNum = page.getInt();
				count = page.getInt();
				System.out.println("count is "+count);
				System.out.println("tuple size is "+columnNum);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Tuple readNext() {
		System.out.println("readnext");
		Tuple next = null;
		if (count == 0) {
			fillPage();
			// if count is still 0 => nothing left to read
			if (count == 0) {
				System.out.println("end of file");
				return next;
				}
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
			for (int i = 0; i < result.length; i ++)
				System.out.println(next.getData()[i]);
			return next;
		}
	}

	@Override
	public void close() {
		System.out.println("Closing channel");
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
	
	public static void main(String[] args) throws IOException {
		String fp = "samples/input/db/data/Boats";
		
		BinaryTupleReader x = new BinaryTupleReader(fp);
		
		while (x.fc.isOpen()) {
			x.readNext();
		}
	}
}
