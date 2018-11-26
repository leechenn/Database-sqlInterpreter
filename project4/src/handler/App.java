package handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import util.Catalog;
import util.Stats;

/**
 * @author Chen Li, QinXuan Pian
 * Upper Class for input output path
 */
public class App {
	
public static Catalog model;
	
	public static void main(String[] args) {
        if (args != null && args.length == 1) {
        	try {
        	File InterpreterConfig = new File(args[0]);
        	Reader reader = new FileReader(InterpreterConfig);
        	BufferedReader br =new BufferedReader(reader);
        	String inputPath = br.readLine();
        	String outputPath = br.readLine();
        	String tempDir = br.readLine();
        	Integer buildIndexFlag = Integer.valueOf(br.readLine());
        	Integer evaluateSqlFlag = Integer.valueOf(br.readLine());  
        	br.close();      	
        	model = Catalog.getInstance(inputPath,outputPath,tempDir,buildIndexFlag,evaluateSqlFlag);//create a singleton for Catalog and store it as static variable in App named model
        	Stats stats = new Stats();
        	stats.storeTableInfo();
        	//if evaluate sql
        	if(Catalog.evaluateSql) {
            Handler.init();
            Handler.parseSql();
        	}
           
        	}catch(IOException e) {
        		e.printStackTrace();
        	}
        	
        }
       
	}
}
