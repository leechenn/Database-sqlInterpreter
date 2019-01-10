package operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import entity.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.TupleWriter;
import util.TupleReader;
/**
 * @author Chen Li, QinXuan Pian
 * External SortOperator for sorting tuple according to expression after order by or according to orderList in join node
 */
public class ExternalSortOperator extends Operator{

	private String subdir;
	private File dir;
	private int bufferNum;
	private int pageCapacity;
	private HashMap<Integer, TupleReader> readerMap = new HashMap<Integer, TupleReader>();
	private TupleReader resultReader;
	private TupleWriter btw;
	private int count;// number of runs in the result
	private int passNum = 0;
	private PlainSelect ps;
	private Map<String, Integer> schema;
	private ArrayList<Integer> compareInd = new ArrayList<Integer>();
	private Tuple[] tupleQueue;
	private Operator op;
	private List<String> orderList;
	private List<OrderByElement> order;
	private boolean orderByOrderElement;

	int counter = 0;

	public ExternalSortOperator (Operator operator, PlainSelect p, String tempDir, int b) {

		ps = p;
		op=operator;
		schema = operator.getSchema();
		order = ps.getOrderByElements();
		orderByOrderElement = true;
		subdir = tempDir+File.separator+this.toString(); // a unique name as a subdirectory
		dir = new File(subdir);
		dir.mkdir();
		bufferNum = b;
		int tupleWidth = operator.getSchema().size();
		pageCapacity = (int)4096/(4*tupleWidth); // # of tuples per page
		count = 0; //initiate # of result runs to 0
		int tuplenumber = 0;
		// pass 0
		ArrayList<Tuple> temp = new ArrayList<Tuple>(); // serve as a buffer to hold tuples
		for (int i = 0; i < pageCapacity*bufferNum; i ++) { // # of tuple per page * page number
			Tuple t = operator.getNextTuple();
			if (t==null) { //reach eof
				if (temp.isEmpty()) // temp is also empty
					break;
				btw = new TupleWriter(subdir+File.separator+passNum+count);
				sort(temp);
				while (!temp.isEmpty()) {
					tuplenumber ++;
					try {
						btw.writeTuple(temp.remove(0));
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				btw.close();
				count ++;
				break;
			}
			else {// not eof
				temp.add(t);


				if (i == pageCapacity*bufferNum-1) { // but the last tuple in the current buffer

					btw = new TupleWriter(subdir+File.separator+passNum+count);

					sort(temp);
					//					System.out.println("after sort "+ temp.size());
					while (!temp.isEmpty()) {
						tuplenumber ++;
						//						System.out.println("temp's head is " + temp.get(0).toString());
						try {
							btw.writeTuple(temp.remove(0));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}// temp is empty by now
					btw.close();
					count ++;
					i = 0; // reset i
				}
			}
		}

		int tupleinfile = 0;
		while (count > 1) { // more than 1 runs in the result. still needs to merge
			//			System.out.println("count is now " + count + " this is pass " + (passNum + 1));

			int newCount = (int) Math.ceil((double)count/(bufferNum - 1)); // number of runs in result for this pass
			//			System.out.println("newcount is " + newCount);
			for(int j = 0; j < count; j ++) {
				try {
					readerMap.put(j, new TupleReader(new File(subdir+File.separator+Integer.toString(passNum)+j)));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (int k = 0; k < newCount; k ++) {
				//create a file and a file writer for each result run
				File scratch = new File(subdir+File.separator+Integer.toString(passNum+1)+k);
				try {
					scratch.createNewFile();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				btw = new TupleWriter(subdir+File.separator+Integer.toString(passNum+1)+k);

				//read result from previous pass
				tupleQueue = new Tuple[bufferNum - 1]; //comparing tuples in this queue
				for (int l = k*(bufferNum-1); l < (k+1)*(bufferNum-1); l++) { //initiate the readNext cycle
					
					if (readerMap.get(l) == null) // no such reader
						tupleQueue[l%(bufferNum -1)] = null;
					else
						tupleQueue[l%(bufferNum -1)] = readerMap.get(l).readNext();// read a tuple from each reader
				}
				
				while (!allNull(tupleQueue)) { //complete the readNext cycle
					
					Tuple result;
					int ind = findMin(tupleQueue);
					if (ind == -1) // all null
						break;
					result = tupleQueue[ind];
					tuplenumber ++;
					try {
						btw.writeTuple(result);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}// findMin will keep reading tuples from readers
					tupleQueue[ind] = readerMap.get(k*(bufferNum-1) + ind).readNext();
				}
				btw.close();
			}
			count = newCount; //update count for the next pass
			passNum ++;

		}

		File target = new File(subdir+File.separator+passNum+"0");
		if(target.exists()) {
		try {
			resultReader = new TupleReader(target);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	}
	public ExternalSortOperator (Operator operator, PlainSelect p, String tempDir, int b,List<String> orderList) {

		ps = p;
		this.orderList = orderList;
		op=operator;
		schema = operator.getSchema();
		List<OrderByElement> order = ps.getOrderByElements();
		subdir = tempDir+File.separator+this.toString(); // a unique name as a subdirectory
		dir = new File(subdir);
		dir.mkdir();
		bufferNum = b;
		int tupleWidth = operator.getSchema().size();
		pageCapacity = (int)4096/(4*tupleWidth); // # of tuples per page
		count = 0; //initiate # of result runs to 0
		int tuplenumber = 0;
		
		// pass 0
		ArrayList<Tuple> temp = new ArrayList<Tuple>(); // serve as a buffer to hold tuples
		for (int i = 0; i < pageCapacity*bufferNum; i ++) { // # of tuple per page * page number
			Tuple t = operator.getNextTuple();
			if (t==null) { //reach eof
				if (temp.isEmpty()) // temp is also empty
					break;
				btw = new TupleWriter(subdir+File.separator+passNum+count);
				sort(temp);
				while (!temp.isEmpty()) {
					tuplenumber ++;
					try {
						btw.writeTuple(temp.remove(0));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				btw.close();
				count ++;
				break;
			}
			else {// not eof
				temp.add(t);
				if (i == pageCapacity*bufferNum-1) { // but the last tuple in the current buffer
					btw = new TupleWriter(subdir+File.separator+passNum+count);
					sort(temp);
					while (!temp.isEmpty()) {
						tuplenumber ++;
						try {
							btw.writeTuple(temp.remove(0));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}// temp is empty by now
					btw.close();
					count ++;
					i = 0; // reset i
				}
			}
		}

		int tupleinfile = 0;
		// more than 1 runs in the result. still needs to merge
		while (count > 1) { 
			int newCount = (int) Math.ceil((double)count/(bufferNum - 1)); // number of runs in result for this pass
			for(int j = 0; j < count; j ++) {
				try {
					readerMap.put(j, new TupleReader(new File(subdir+File.separator+Integer.toString(passNum)+j)));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (int k = 0; k < newCount; k ++) {
				//create a file and a file writer for each result run
				File scratch = new File(subdir+File.separator+Integer.toString(passNum+1)+k);
				try {
					scratch.createNewFile();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				btw = new TupleWriter(subdir+File.separator+Integer.toString(passNum+1)+k);
				//read result from previous pass
				tupleQueue = new Tuple[bufferNum - 1]; //comparing tuples in this queue
				for (int l = k*(bufferNum-1); l < (k+1)*(bufferNum-1); l++) { //initiate the readNext cycle
					if (readerMap.get(l) == null) // no such reader
						tupleQueue[l%(bufferNum -1)] = null;
					else
						tupleQueue[l%(bufferNum -1)] = readerMap.get(l).readNext();// read a tuple from each reader
				}
				while (!allNull(tupleQueue)) { //complete the readNext cycle
					for (int i = 0; i < tupleQueue.length; i ++) {
						
					}
					Tuple result;
					int ind = findMin(tupleQueue);
					if (ind == -1) // all null
						break;
					result = tupleQueue[ind];
					tuplenumber ++;
					try {
						btw.writeTuple(result);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}// findMin will keep reading tuples from readers
					tupleQueue[ind] = readerMap.get(k*(bufferNum-1) + ind).readNext();
				}
				btw.close();
			}
			count = newCount; //update count for the next pass
			passNum ++;

		}

		File target = new File(subdir+File.separator+passNum+"0");
		if(target.exists()) {
		try {
			resultReader = new TupleReader(target);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	}

	@Override
	public Tuple getNextTuple() {
		if(resultReader==null) {
			return null;
		}
		Tuple result = resultReader.readNext();
		return result;
	}

	// sort for pass 0
	public void sort(ArrayList<Tuple> in) {
		if(orderByOrderElement) {
			Collections.sort(in, new compareTupleForOrderByElement());
		}else {
		Collections.sort(in, new compareTuple());
		}
	}

	public boolean allNull(Tuple[] x) { // all the runs reach eof now
		for (int i = 0; i < x.length; i ++) {
			if (x[i]!=null)
				return false;
		}
		return true;
	}


	public int findMin(Tuple[] x) { // pop the smallest tuple and invoke readNext once popped
		Comparator<Tuple> ct = null;
		if(orderByOrderElement) {
			ct = new compareTupleForOrderByElement();
		}else {
			ct = new compareTuple();
		}
		int target = -1;
		Tuple temp = null;
		for (int i = 0; i < x.length; i++) {
			if (x[i] != null) { // initiate the first non-null tuple as min now
				temp = x[i]; //current min tuple
				target = i; // min tuple index
				break;
			}
		}
		if (target == -1)
			return target; //all null

		// compare it to the rest of the array
		for (int i = target+1; i < x.length; i++) {
			if (ct.compare(temp, x[i]) > -1) { // current min is larger or equal to the target
				temp = x[i]; // current min
				target = i;
			}
		}
		return target;
	}


	public static boolean deleteDirectory(File dir) {
		File[] allContents = dir.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return dir.delete();
	}

	class compareTuple implements Comparator<Tuple>{

		//		List<OrderByElement> order = ps.getOrderByElements();
		@Override
		public int compare(Tuple t1, Tuple t2) {
			if (t2 == null)
				return -1;
			if (orderList != null) {
				for (int i = 0; i < orderList.size(); i++) {
					String column = orderList.get(i);
					int index = schema.get(column);
					//					System.out.println(t1.toString());
					//					System.out.println(t2.toString());

					if (t1.getData()[index] > t2.getData()[index]) {
						return 1;
					}
					if (t1.getData()[index] < t2.getData()[index]) {
						return -1;
					}
				}
			}
			//			if child operator is not project operator, all columns should be in tuple
			int len = op.getSchema().size();
			
			for (int i = 0; i < len ; i++){

				if (t1.getData()[i] > t2.getData()[i]) {
					return 1;
				}
				if (t1.getData()[i] < t2.getData()[i]) {
					return -1;
				}
			}
			return 0;
		}

	}
	class compareTupleForOrderByElement implements Comparator<Tuple>{

		//		List<OrderByElement> order = ps.getOrderByElements();
		@Override
		public int compare(Tuple t1, Tuple t2) {
			if (t2 == null)
				return -1;
			if (order != null) {
				
				for (int i = 0; i < order.size(); i++) {
					
					String column = order.get(i).toString();
					int index = schema.get(column);
					if (t1.getData()[index] > t2.getData()[index]) {
						return 1;
					}
					if (t1.getData()[index] < t2.getData()[index]) {
						return -1;
					}
				}
			}
			//			if child operator is not project operator, all columns should be in tuple
			int len = op.getSchema().size();
			
			for (int i = 0; i < len ; i++){

				if (t1.getData()[i] > t2.getData()[i]) {
					return 1;
				}
				if (t1.getData()[i] < t2.getData()[i]) {
					return -1;
				}
			}
			return 0;
		}

	}
	public TupleReader getResultReader() {
		return this.resultReader;
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}
	public void reset(int index) {
		resultReader.reset(index);
	}
	public Map<String,Integer> getSchema(){
		return this.schema;
	};
	public String printPhysicalTree() {
		if(order!=null) {
			return String.format("ExternalSort%s", order.toString());
		}else if(orderList!=null) {
			return String.format("ExternalSort%s", orderList.toString());
		}else {
			return String.format("ExternalSort[]");
		}
	}
	@Override
	public void printTree(PrintStream ps, int lv) {
		printIndent(ps, lv);
		ps.println(printPhysicalTree());
		op.printTree(ps, lv + 1);
	}

}
