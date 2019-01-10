package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import handler.App;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class Stats {

	private String dbDir;
	private String dataDir;
	private String schemaPath;
	private BufferedReader br;
	private BufferedWriter bw;
	private TupleReader tr;
	private HashMap<String, TableInfo> statsMap = new HashMap<String,TableInfo>();
	//private Map<String,Map<String,Integer>> schemas;

	public Stats() {
		dbDir = App.model.getInputPath()+"/db";
		dataDir = App.model.getInputPath()+"/db/data";
		schemaPath = App.model.getInputPath()+"/db/schema.txt";
		//	schemas = App.model.gets
		File statFile = new File(this.dbDir+"/stat.txt");
		try {
			bw = new BufferedWriter(new FileWriter(statFile));
			br = new BufferedReader(new FileReader(schemaPath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * write the table stats to disk
	 */
	public void storeTableInfo() {

		try{
			String line = br.readLine();
			while(line!=null) {
				//		System.out.println(line);
				String[] attrsName = this.retrieveAttrsName(line.split("\\s+"));
				String tableName = line.split("\\s+")[0];

				//print test
				//		System.out.println(tableName);
				//		System.out.println(Arrays.toString(attrsName));
				//		
				TableInfo ti = new TableInfo(tableName,attrsName);

				//		System.out.println(ti);

				File table = new File(App.model.getDataStoredPath(tableName));
				tr = new TupleReader(table);
				long[] tp = tr.nextTuple();
				while(tp!=null) {
					for(int i = 0; i<attrsName.length; i++) {
						ti.getInfoMap().get(attrsName[i])[0]=(int) Math.min(ti.getInfoMap().get(attrsName[i])[0], tp[i]);
						ti.getInfoMap().get(attrsName[i])[1]=(int) Math.max(ti.getInfoMap().get(attrsName[i])[1], tp[i]);
					}
					ti.addTuple();
					//			System.out.println(ti);
					tp = tr.nextTuple();
				}

				bw.write(tableName + " ");
				bw.write(ti.getTupleNum() + " ");
				for(String attrName:ti.getInfoMap().keySet()) {
					bw.write(attrName + ",");
					int[] valuePair = ti.getInfoMap().get(attrName);
					bw.write(valuePair[0]+",");
					bw.write(valuePair[1]+"  ");	
				}
				this.statsMap.put(tableName, ti);
				bw.newLine();
				line = br.readLine();
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				bw.close();
				br.close();
				tr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public  HashMap<String, TableInfo> getStatsMap() {
		return this.statsMap;
	}
	public String[] retrieveAttrsName(String[] names) {
		int len = names.length;
		String[] attrsName = new String[len-1];
		for(int i = 0;i<len-1;i++) {
			attrsName[i] = names[i+1];
		}
		return attrsName;
	}
}


