package util;

import java.util.Random;

import entity.Tuple;

public class randomNum {
	
	private BinaryTupleWriter btw;
	
	private BinaryTupleReader btr;
	
//	private String inputfilepath;
	
	private String outputfilepath;
	
	public randomNum(String ifp, String ofp) {
//		inputfilepath = ifp;
		outputfilepath = ofp;
		btw = new BinaryTupleWriter(outputfilepath);
		btr = new BinaryTupleReader(outputfilepath);
	}
	
	public void randomTuple (int count, int columnNum, int min, int max) {
		if (count < 0 || columnNum < 0 || min > max) {
			System.out.println("invalid input");
		}
		else {
			for (int i = 0; i < count; i ++) {
				int[] temp = new int[columnNum];
				for (int j = 0; j < columnNum; j ++) {
					temp[j] = (int) generateRandomIntIntRange(min, max);
				}
				btw.fillBuffer(new Tuple(temp));
			}
		}
		btw.finishWriting();
	}
	
	public static int generateRandomIntIntRange(int min, int max) {
	    Random r = new Random();
	    return r.nextInt((max - min) + 1) + min;
	}
	
	public Tuple readTuple() {
		return btr.readNext();
	}

}
