package util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.Expression;

/**
 * Catalog class for keeping tracking information of tables, the class will be created as a singleton
 * @author Chen Li, QinXuan Pian
 *
 */
public class Catalog {

	private static Catalog instance = null;
	private String inputPath;
	private String outputPath;
	private String dataSchemaFile;
	private String dataDir;
	private String sqlPath;
	private Map<String,String> aliaMap = new HashMap<String,String>();
	private Map<String,String> dataStore = new HashMap<String,String>();//store information of data stored path
	private Map<String,Integer> currentSchema = new HashMap<String,Integer>();//schema of current tuple
	private Map<String,Map<String,Integer>> schemas = new HashMap<String,Map<String,Integer>>();
	private List<String> allTableList;//all table names in SQL
	private List<Expression>[] singTableExp;//selection list for single table
	private List<Expression>[] JoinedTableCondition;//join condition for each join
	private List<Expression> allExp;//all expression in get where expression
	private List<String> joinedTableList;//list of joined tables
	public int joinConfig;
	public int joinBuffer;
	public int sortConfig;
	public int sortBuffer;
	public String temDir;
	
	/**
	 * Catalog constructor
	 * @param inputPath
	 * @param outputPath
	 */
	private Catalog(String inputPath,String outputPath,String temDir) {
		setInputPath(inputPath);
		setOutputPath(outputPath);
		this.temDir = temDir;
		this.sqlPath = this.getInputPath()+"/queries.sql";
		this.dataSchemaFile = this.getInputPath()+"/db/schema.txt";
		this.dataDir = this.getInputPath()+"/db/data";
		BufferedReader br = null;
		try {
			File schemaFile = new File(this.dataSchemaFile);
			Reader schemaReader;
			schemaReader = new FileReader(schemaFile);
			br = new BufferedReader(schemaReader);
			String s = null;
			while((s = br.readLine())!=null) {
				String[] table = s.split("\\s+");
				dataStore.put(table[0],dataDir+"/"+table[0]);
				Map<String,Integer> schema = new HashMap<String,Integer>();
				for(int i = 1;i<table.length;i++) {
					String tableName = table[0];
					schema.put(tableName+"."+table[i], i-1);
				}
				schemas.put(table[0], schema);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String configFile = this.getInputPath() + "/"+ "plan_builder_config.txt";
		File conf = new File(configFile);
		Scanner in  = null;
		try {
			in = new Scanner(conf);
			String[] joinMethod = in.nextLine().split(" ");
			joinBuffer = joinMethod.length == 2 ? joinBuffer = Integer.valueOf(joinMethod[1]) : 0;
			joinConfig = Integer.valueOf(joinMethod[0]);
			String[] sortMethod = in.nextLine().split(" ");
			if (sortMethod.length == 2) {
				sortConfig = 1;
				sortBuffer = Integer.valueOf(sortMethod[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if( in != null ) in.close();
		}

	}

	//    singleton instance
	public static synchronized Catalog getInstance(String inputPath,String outputPath,String temDir) {
		if(instance == null) {
			instance = new Catalog(inputPath,outputPath,temDir);
		}
		return instance;
	}
	public void iniAllTableList() {
		allTableList = new ArrayList<String>();
	}
	public void iniJoinedTableList() {
		joinedTableList = new ArrayList<String>();
	}
	public List<String> getJoinedTableList(){
		return this.joinedTableList;
	}
	public List<Expression>[] getsingleTableExp() {
		return this.singTableExp;
	}
	public List<Expression>[] getJoinedTableExp() {
		return this.JoinedTableCondition;
	}
	public void iniSingleTableExp(int size) {
		this.singTableExp = new ArrayList[size];
	}
	public void iniJoinedTableExp(int size) {
		this.JoinedTableCondition = new ArrayList[size];
	}
	public void iniAllExp() {
		this.allExp = new ArrayList<Expression>();
	}
	public List<Expression> getAllExp() {
		return this.allExp;
	}
	public List<String> getAllTableList() {
		return this.allTableList;
	}

	public void setAliaMap(String[] aliaStr) {
		//		key is alias name
		this.aliaMap.put(aliaStr[2], aliaStr[0]);
	}

	public void setCurSchema(String alias) {
		Map<String, Integer> tempSchema = schemas.get(aliaMap.get(alias));
		currentSchema = new HashMap<>();
		for (Map.Entry<String, Integer> entry : tempSchema.entrySet()) {
			String newKey = alias + "." + entry.getKey().split("\\.")[1];
			currentSchema.put(newKey, entry.getValue());
		}
	}
	//if there is no alias,key is tableName.column,value is index
	public void iniCurSchema(String tableName) {
		this.currentSchema = schemas.get(tableName);
	}

	public Map<String, Integer> getCurSchema(){
		return this.currentSchema;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getDataStoredPath(String table) {
		return dataStore.get(table);
	}

	public Map<String, Integer> getTableSchema(String table) {
		return schemas.get(table);
	} 
	public String getSqlPath() {
		return this.sqlPath;
	}

}
