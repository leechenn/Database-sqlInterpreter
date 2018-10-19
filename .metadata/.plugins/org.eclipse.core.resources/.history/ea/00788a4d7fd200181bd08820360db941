package util;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import entity.Tuple;

public class BinaryTupleWriter implements TupleWriter{
	
	private String filepath;
	private FileOutputStream fou;
	private FileChannel fc;
	
	private ByteBuffer page;
	private int columnNum = 0;
	private int count = 0; // num of tuples per page
	private ByteBuffer zeros;
	
	public BinaryTupleWriter(String fp) {
		filepath = fp;
		
		page = ByteBuffer.allocate(4096);
		
		page.putInt(columnNum);
		page.putInt(count);
		
		try {
			fou = new FileOutputStream(filepath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		fc = fou.getChannel();
		
		zeros = ByteBuffer.allocate(4096);
		
		while (zeros.hasRemaining()) {
			zeros.putInt(0);
		}
		zeros.flip();
	}

	@Override
	public void fillBuffer(Tuple input) {
		int[] temp = input.getData();
		columnNum = temp.length;

		if (page.remaining()/4 < columnNum) {
			while (page.hasRemaining()) {
				page.putInt(0);
			}
			writeFile();
			}
		
		System.out.println(page.toString());
		for (int i = 0; i < columnNum; i++) {
			page.putInt(temp[i]);
		}
		count ++;
	}
	
	public void finishWriting() { // write the the last page to file if theres one
		System.out.println("finish writing");
		System.out.println("the final page has" + count + "tuples");
		if (count > 0) {
			page.putInt(0, columnNum);
			page.putInt(4, count);
			page.flip();
			try {
				fc.write(page);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			fou.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void writeFile() {
		System.out.println("writing to file");
		page.putInt(0, columnNum);
		page.putInt(4, count);
		System.out.println("count is now:"+count);
		assert(page.position()==4096);
		
		page.flip();
		try {
			fc.write(page);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// reset page and zero out
		page.clear();
		page.put(zeros);
		zeros.rewind();
		page.flip();
		count = 0;
		page.putInt(columnNum);
		page.putInt(count);
		
	}
	
	public static void main(String[] args) throws IOException {
		String fp = "samples/input/db/data/test";
		
		String input = "samples/input/db/data/Boats";
		
		BinaryTupleWriter x = new BinaryTupleWriter(fp);
		
		BinaryTupleReader in = new BinaryTupleReader(input);
		
//		int[] a = {1,2,3};
//		Tuple temp = new Tuple(a);
		
		Tuple temp;
		
		int c = 0;
		while ((temp = in.readNext())!=null) {
			System.out.println(c);
			c++;
			x.fillBuffer(temp);
		}
		x.finishWriting();
			
		BinaryTupleReader y = new BinaryTupleReader(fp);
		while (y.fc.isOpen()) {
			y.readNext();
		}
	}

}
