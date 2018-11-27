package util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import bPlusTree.TreeSerializer;
import handler.App;
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
	private String indexFile;
	private Map<String,String> aliaMap = new HashMap<String,String>();
	private static Map<String,String> dataStore = new HashMap<String,String>();//store information of data stored path
	private Map<String,Integer> currentSchema = new HashMap<String,Integer>();//schema of current tuple
	private Map<String,Map<String,Integer>> schemas = new HashMap<String,Map<String,Integer>>();
	private List<String> allTableList;//all table names in SQL
	private List<Expression>[] singTableExp;//selection list for single table
	private List<Expression>[] JoinedTableCondition;//join condition for each join
	private List<Expression> allExp;//all expression in get where expression
	private List<String> joinedTableList;//list of joined tables
	private Map<String, String[]> indexInfoMap = new HashMap<String,String[]>();//index info map
	public Map<String,TableInfo> tableInfoMap = new HashMap<String,TableInfo>();
	public Map<String, List<IndexInfo>> tableInfo = new HashMap<String, List<IndexInfo>>();	// The index info for the tables.
	public Stats stats;
	public int joinConfig;
	public int joinBuffer;
	public int sortConfig;
	public int sortBuffer;
	public String temDir;
	public static String indexDir;
	public static boolean buildIndex = false;
	public static boolean evaluateSql = false;
	public static boolean useIndex = false;
	
	/**
	 * Catalog constructor
	 * @param inputPath
	 * @param outputPath
	 */
	private Catalog(String inputPath,String outputPath,String temDir, Integer buildIndexFlag, Integer evaluateSqlFlag) {
		setInputPath(inputPath);
		setOutputPath(outputPath);
		if(buildIndexFlag == 1) {
			Catalog.buildIndex = true;
		}
		if(evaluateSqlFlag == 1) {
			Catalog.evaluateSql = true;
		}
		this.temDir = temDir;
		this.sqlPath = this.getInputPath()+"/queries.sql";
		this.dataSchemaFile = this.getInputPath()+"/db/schema.txt";
		this.dataDir = this.getInputPath()+"/db/data";
		this.indexDir = this.getInputPath()+"/db/indexes";
		this.indexFile = this.getInputPath()+"/db/index_info.txt";
		
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
		
		try {
			File indexFile = new File(this.indexFile);
			Reader indexFileReader;
			indexFileReader = new FileReader(indexFile);
			br = new BufferedReader(indexFileReader);
			String s = null;
			while((s = br.readLine())!=null) {
				String[] indexInfo = s.split("\\s+");
				this.indexInfoMap.put(indexInfo[0], indexInfo);
				//if we need to build index, we sort table first
				if(buildIndex&&Integer.valueOf(indexInfo[2])==1) {
					
					int index = schemas.get(indexInfo[0]).get(indexInfo[0]+"."+indexInfo[1]);
					//sort table for clustered index              
					Tool.sortTableByIndex(new File(this.getDataStoredPath(indexInfo[0])), index);
								
				}
				if(buildIndex) {
				TreeSerializer treeserializer = new TreeSerializer(Integer.valueOf(indexInfo[3]),indexInfo[0],indexInfo[1],this.getTableSchema(indexInfo[0]).get(indexInfo[0]+"."+indexInfo[1]));
				treeserializer.constructBPlustTree();
				}
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
			if(Integer.valueOf(in.nextLine()) == 1) {
				Catalog.useIndex = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if( in != null ) in.close();
		}

	}

	//    singleton instance
	public static synchronized Catalog getInstance(String inputPath,String outputPath,String temDir,Integer buildIndexFlag, Integer evaluateSqlFlag) {
		if(instance == null) {
			instance = new Catalog(inputPath,outputPath,temDir,buildIndexFlag,evaluateSqlFlag);
		}
		return instance;
	}
	public void setStats(Stats stats) {
		this.stats = stats;
	}
	public Stats getStats() {
		return this.stats;
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
	public String searchTableName(String alias) {
		if(this.aliaMap.get(alias)!=null) {
			return this.aliaMap.get(alias);
		}else {
			return alias;
		}
	}
	public String getIndexFile() {
		return this.indexFile;
	}
	public String getIndexDir() {
		return this.indexDir;
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

	public static String getDataStoredPath(String table) {
		return dataStore.get(table);
	}

	public Map<String, Integer> getTableSchema(String table) {
		return schemas.get(table);
	}
	public String getSqlPath() {
		return this.sqlPath;
	}
	public Map<String, String[]> getIndexInfoMap(){
		return this.indexInfoMap;
	}
	
	public void resetIdxInfo() {
		
		try (BufferedReader br = new BufferedReader(
				new FileReader(indexFile))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] str = line.split(" ");
				String relt = str[0];
				String attr = str[1];
				boolean clust = (str[2].equals("1"));
				int ord = Integer.parseInt(str[3]);
				addIndexInfo(relt, attr, clust, ord);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	public IndexInfo getIndexInfo(String table, String attr) {
		List<IndexInfo> indexInfos = getIndexInfoList(table);
		if (indexInfos == null) return null;
		
		// find the index info with the attribute name
		for (IndexInfo info : indexInfos) {
			if (info.attr.equals(attr)) return info;
		}
		
		// Does not found
		return null;
	}
	
	
	public List<IndexInfo> getIndexInfoList(String table) {
		// transfer to the origin table name
		String originTable = App.model.searchTableName(table);
		if (!tableInfo.containsKey(originTable)) {
			return null;
		}
		return tableInfo.get(originTable);
	}
	public void addIndexInfo(String relation, String attr, boolean clust, int ord) {
		// create the table item if does not exist.
		if (!tableInfo.containsKey(relation)) {
			tableInfo.put(relation, new LinkedList<IndexInfo>());
		}
		
		List<IndexInfo> indicesOfTheTable = tableInfo.get(relation);
		
		IndexInfo info = new IndexInfo(relation, attr, clust, ord);
		
		// we should add clustered at the head of the list.
		if (clust) {
			indicesOfTheTable.add(0, info);
		} else {
			indicesOfTheTable.add(info);
		}
	}

}
