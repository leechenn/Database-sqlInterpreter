package handler;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class LogicalPlanBuilder {
	
	public static void init() {
	//	get output path from Catalog and build a directory
		String outputPath = App.model.getOutputPath();
		new File(outputPath).mkdirs();
	}
	
	public static void parseSql() {
		
	}
}
