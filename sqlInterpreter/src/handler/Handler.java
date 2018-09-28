package handler;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator.Operator;
import operator.ScanOperator;

public class Handler {
public static void init() {
	String outputPath = App.model.getOutputPath();
	System.out.println(outputPath);
	new File(outputPath).mkdirs();
}
public static void parseSql() {
	try {
        // try
        String sqlFile = App.model.getSqlPath();
        CCJSqlParser parser = new CCJSqlParser(new FileReader(sqlFile));
        Statement statement;
        int sqlCount = 1;
        while ((statement = parser.Statement()) != null) {
            System.out.println("Read statement: " + statement);
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            System.out.println("Select body is " + select.getSelectBody());
            try {
            Operator operator = new ScanOperator(plainSelect);
//            System.out.println("already catch");
            operator.dump(sqlCount);
            }
            catch(NullPointerException e) {
            	System.out.println("Table not found");
            }
            sqlCount = sqlCount + 1;
        }
    } catch (Exception e) {
        System.err.println("Exception occurred during parsing");
        e.printStackTrace();
    }
}
}
